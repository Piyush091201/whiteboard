// Command server is the entrypoint for the collaborative whiteboard backend.
//
// At this scaffold stage it wires up only the cross-cutting concerns that every
// later phase depends on: structured logging, configuration from the
// environment, an HTTP server with a health check, and context-based graceful
// shutdown on SIGINT/SIGTERM. The real-time hub, WebSocket endpoint, Redis
// fan-out, and persistence are added in later phases.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Piyush091201/whiteboard/internal/broker"
	"github.com/Piyush091201/whiteboard/internal/hub"
	"github.com/Piyush091201/whiteboard/internal/metrics"
	"github.com/Piyush091201/whiteboard/internal/store"
	"github.com/Piyush091201/whiteboard/internal/ws"
)

// config holds runtime configuration sourced from the environment.
//
// For a C# developer: this is the hand-rolled equivalent of binding
// IConfiguration to an options class. Go's standard library has no built-in
// configuration framework, so reading os.Getenv with explicit defaults is the
// idiomatic baseline; we only reach for a library if this grows unwieldy.
type config struct {
	addr            string        // host:port the HTTP server listens on
	redisAddr       string        // Redis host:port; empty => in-memory single-instance broker
	databaseURL     string        // Postgres DSN; empty => persistence disabled
	shutdownTimeout time.Duration // how long to wait for in-flight requests to drain
}

func loadConfig() config {
	return config{
		addr:            getenv("WB_ADDR", ":8080"),
		redisAddr:       getenv("WB_REDIS_ADDR", ""),
		databaseURL:     getenv("WB_DATABASE_URL", ""),
		shutdownTimeout: 15 * time.Second,
	}
}

func getenv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return fallback
}

// newBroker builds the message/state broker from configuration. With a Redis
// address it verifies connectivity up front so a misconfiguration fails fast at
// startup rather than on the first message.
func newBroker(ctx context.Context, logger *slog.Logger, cfg config) (broker.Broker, error) {
	if cfg.redisAddr == "" {
		logger.Info("using in-memory broker (single instance)")
		return broker.NewMemory(), nil
	}

	rdb := redis.NewClient(&redis.Options{Addr: cfg.redisAddr})
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("connect to redis at %s: %w", cfg.redisAddr, err)
	}
	logger.Info("using redis broker", "addr", cfg.redisAddr)
	return broker.NewRedis(rdb), nil
}

// newStore builds the durable store from configuration. With no database URL it
// returns a nil store, which disables persistence.
func newStore(ctx context.Context, logger *slog.Logger, cfg config) (store.Store, error) {
	if cfg.databaseURL == "" {
		logger.Info("persistence disabled (no WB_DATABASE_URL)")
		return nil, nil
	}
	pg, err := store.NewPostgres(ctx, cfg.databaseURL)
	if err != nil {
		return nil, fmt.Errorf("connect to postgres: %w", err)
	}
	logger.Info("using postgres persistence")
	return pg, nil
}

func main() {
	// slog is Go's standard structured logger (stdlib since 1.21). A JSON
	// handler gives us machine-parseable logs out of the box — the analog of
	// configuring Serilog/Microsoft.Extensions.Logging with a JSON sink.
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	if err := run(logger); err != nil {
		logger.Error("server exited with error", "err", err)
		os.Exit(1)
	}
	logger.Info("server stopped cleanly")
}

// run holds the real program logic so that errors can be returned and handled
// in one place rather than calling os.Exit/log.Fatal from deep in the stack.
// This "return errors up to main" shape is idiomatic Go and keeps the code
// testable.
//
// For a C# developer: the signal.NotifyContext below is the direct analog of
// wiring IHostApplicationLifetime.ApplicationStopping to a CancellationToken.
// The ctx is then threaded explicitly into Shutdown rather than being ambient.
func run(logger *slog.Logger) error {
	cfg := loadConfig()

	// ctx is cancelled when the process receives SIGINT (Ctrl+C) or SIGTERM
	// (what `docker stop` / Kubernetes send). Everything that needs to drain on
	// shutdown hangs off this context.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Choose the broker: Redis for multi-instance fan-out, or an in-process
	// broker for single-instance operation. Both satisfy the same interface, so
	// the hub is unaware of which one it is using.
	b, err := newBroker(ctx, logger, cfg)
	if err != nil {
		return err
	}
	defer func() { _ = b.Close() }()

	st, err := newStore(ctx, logger, cfg)
	if err != nil {
		return err
	}
	if st != nil {
		defer func() { _ = st.Close() }()
	}

	m := metrics.NewPrometheus()

	h := hub.New(logger, b, hub.WithStore(st), hub.WithMetrics(m))
	defer h.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	// WebSocket endpoint: clients connect to /ws/<board-id> to join a board.
	mux.Handle("GET /ws/{board}", ws.Handler(h))
	// Prometheus metrics: active connections, throughput, backpressure.
	mux.Handle("GET /metrics", m.Handler())

	srv := &http.Server{
		Addr:              cfg.addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	// serveErr carries a fatal ListenAndServe error out of its goroutine.
	// Buffered so the goroutine never blocks if we've already moved on to
	// shutdown.
	serveErr := make(chan error, 1)
	go func() {
		logger.Info("http server listening", "addr", cfg.addr)
		// ListenAndServe always returns a non-nil error; ErrServerClosed is the
		// expected one after Shutdown and is not a failure.
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
			return
		}
		serveErr <- nil
	}()

	// Block until either the server dies on its own or we're asked to shut down.
	select {
	case err := <-serveErr:
		return err
	case <-ctx.Done():
		logger.Info("shutdown signal received, draining", "timeout", cfg.shutdownTimeout)
	}

	// Give in-flight work a bounded window to finish.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.shutdownTimeout)
	defer cancel()

	// Stop accepting new connections first. Shutdown does not close hijacked
	// WebSocket connections, so it returns quickly while sessions are still live.
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("http server shutdown error", "err", err)
	}

	// Drain the live WebSocket sessions: close every board, flush a final
	// snapshot, and wait for connections to finish within the timeout.
	if err := h.Shutdown(shutdownCtx); err != nil {
		logger.Error("hub drain did not finish in time", "err", err)
		return err
	}
	logger.Info("drain complete")
	return nil
}
