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
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Piyush091201/whiteboard/internal/hub"
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
	shutdownTimeout time.Duration // how long to wait for in-flight requests to drain
}

func loadConfig() config {
	return config{
		addr:            getenv("WB_ADDR", ":8080"),
		shutdownTimeout: 15 * time.Second,
	}
}

func getenv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return fallback
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

	h := hub.New(logger)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	// WebSocket endpoint: clients connect to /ws/<board-id> to join a board.
	mux.Handle("GET /ws/{board}", ws.Handler(h))
	// Later phases register GET /metrics here.

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

	// Give in-flight work a bounded window to finish. In later phases this is
	// where we also signal the hub to close connections gracefully.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.shutdownTimeout)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		return err
	}
	return nil
}
