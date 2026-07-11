// Command loadtest drives a running whiteboard instance with many concurrent
// WebSocket clients and reports how many connections it sustains, the message
// throughput, and end-to-end latency.
//
// Each client connects to a board and (optionally) sends shape updates at a
// fixed rate, embedding a timestamp in each op. Because ops are fanned out to
// every client on the board — including the sender — a client measures latency
// by timing how long its own ops take to loop back. All clients continuously
// drain their inbound stream so the server never has to kick them for being slow.
//
// Example:
//
//	loadtest -url ws://localhost:8080 -clients 2000 -boards 40 -rate 2 -duration 30s
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"github.com/Piyush091201/whiteboard/internal/protocol"
)

type config struct {
	url      string
	clients  int
	boards   int
	rate     float64 // shape updates per second per client (0 = hold connection only)
	duration time.Duration
	dialConc int // max concurrent dials during ramp-up
}

// stats holds process-wide counters plus per-client latency samples.
type stats struct {
	connected  atomic.Int64
	dialErrors atomic.Int64
	sent       atomic.Int64
	received   atomic.Int64

	mu        sync.Mutex
	latencies []time.Duration // round-trip of a client's own ops
}

func (s *stats) addLatencies(ls []time.Duration) {
	s.mu.Lock()
	s.latencies = append(s.latencies, ls...)
	s.mu.Unlock()
}

func main() {
	cfg := config{}
	flag.StringVar(&cfg.url, "url", "ws://localhost:8080", "base WebSocket URL of the server")
	flag.IntVar(&cfg.clients, "clients", 1000, "number of concurrent clients")
	flag.IntVar(&cfg.boards, "boards", 20, "number of boards clients are spread across")
	flag.Float64Var(&cfg.rate, "rate", 1, "shape updates per second per client (0 = connection only)")
	flag.DurationVar(&cfg.duration, "duration", 20*time.Second, "steady-state measurement window")
	flag.IntVar(&cfg.dialConc, "dial-concurrency", 200, "max simultaneous dials during ramp-up")
	flag.Parse()

	if err := run(cfg); err != nil {
		log.Fatal(err)
	}
}

func run(cfg config) error {
	st := &stats{}

	// Ramp-up: dial all clients (bounded concurrency) before the measurement
	// window, so the reported throughput reflects steady state, not connecting.
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	dialSem := make(chan struct{}, cfg.dialConc)
	ready := make(chan struct{}, cfg.clients)

	fmt.Fprintf(os.Stderr, "connecting %d clients across %d boards...\n", cfg.clients, cfg.boards)
	start := time.Now()
	for i := 0; i < cfg.clients; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			dialSem <- struct{}{}
			c, err := dial(runCtx, cfg, idx)
			<-dialSem
			if err != nil {
				st.dialErrors.Add(1)
				ready <- struct{}{}
				return
			}
			st.connected.Add(1)
			ready <- struct{}{}
			c.run(runCtx, st)
		}(i)
	}

	// Wait until every client has either connected or failed to connect.
	for i := 0; i < cfg.clients; i++ {
		<-ready
	}
	connectDur := time.Since(start)
	connected := st.connected.Load()
	fmt.Fprintf(os.Stderr, "connected %d/%d in %s (%d dial errors)\n",
		connected, cfg.clients, connectDur.Round(time.Millisecond), st.dialErrors.Load())

	// Measurement window: reset traffic counters so they cover steady state.
	st.sent.Store(0)
	st.received.Store(0)
	windowStart := time.Now()
	time.Sleep(cfg.duration)
	window := time.Since(windowStart)

	sent := st.sent.Load()
	received := st.received.Load()

	cancel()
	wg.Wait()

	report(cfg, connected, connectDur, window, sent, received, st.latencies)
	return nil
}

// client is one connected load-test participant.
type client struct {
	conn    *websocket.Conn
	shapeID string
	rate    float64
}

func dial(ctx context.Context, cfg config, idx int) (*client, error) {
	board := fmt.Sprintf("board%d", idx%cfg.boards)
	url := fmt.Sprintf("%s/ws/%s?name=c%d", cfg.url, board, idx)
	conn, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		return nil, err
	}
	conn.SetReadLimit(1 << 20)
	return &client{conn: conn, shapeID: fmt.Sprintf("c%d", idx), rate: cfg.rate}, nil
}

// run drains inbound messages and, if a rate is set, sends shape updates until
// ctx is cancelled. It records the round-trip latency of its own ops.
func (c *client) run(ctx context.Context, st *stats) {
	defer func() { _ = c.conn.CloseNow() }()

	var latencies []time.Duration
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		for {
			_, data, err := c.conn.Read(ctx)
			if err != nil {
				return
			}
			st.received.Add(1)
			if ts, ok := c.ownOpTimestamp(data); ok {
				latencies = append(latencies, time.Since(time.Unix(0, ts)))
			}
		}
	}()

	if c.rate > 0 {
		interval := time.Duration(float64(time.Second) / c.rate)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
	send:
		for {
			select {
			case <-ctx.Done():
				break send
			case <-ticker.C:
				if err := c.sendUpdate(ctx); err != nil {
					break send
				}
				st.sent.Add(1)
			}
		}
	}

	<-readerDone
	st.addLatencies(latencies)
}

func (c *client) sendUpdate(ctx context.Context) error {
	shape, _ := json.Marshal(map[string]any{
		"ts": time.Now().UnixNano(),
		"x":  rand.Intn(1000),
		"y":  rand.Intn(1000),
	})
	msg, err := protocol.Marshal(protocol.TypeShapeUpdate, 0, protocol.ShapeOp{
		ID:    c.shapeID,
		Shape: shape,
	})
	if err != nil {
		return err
	}
	return c.conn.Write(ctx, websocket.MessageText, msg)
}

// ownOpTimestamp returns the embedded timestamp if data is a shape update for
// this client's own shape (i.e. one of its ops looping back).
func (c *client) ownOpTimestamp(data []byte) (int64, bool) {
	var env protocol.Envelope
	if err := json.Unmarshal(data, &env); err != nil || env.Type != protocol.TypeShapeUpdate {
		return 0, false
	}
	var op protocol.ShapeOp
	if err := env.DecodePayload(&op); err != nil || op.ID != c.shapeID {
		return 0, false
	}
	var body struct {
		TS int64 `json:"ts"`
	}
	if err := json.Unmarshal(op.Shape, &body); err != nil {
		return 0, false
	}
	return body.TS, true
}

func report(cfg config, connected int64, connectDur, window time.Duration, sent, received int64, latencies []time.Duration) {
	secs := window.Seconds()
	fmt.Println("========== load test results ==========")
	fmt.Printf("target url:        %s\n", cfg.url)
	fmt.Printf("clients connected: %d / %d\n", connected, cfg.clients)
	fmt.Printf("boards:            %d (~%d clients/board)\n", cfg.boards, connected/int64(max(cfg.boards, 1)))
	fmt.Printf("connect time:      %s\n", connectDur.Round(time.Millisecond))
	fmt.Printf("window:            %s\n", window.Round(time.Millisecond))
	fmt.Printf("messages sent:     %d (%.0f/s)\n", sent, float64(sent)/secs)
	fmt.Printf("messages received: %d (%.0f/s)\n", received, float64(received)/secs)
	if len(latencies) > 0 {
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
		fmt.Printf("round-trip latency: p50=%s p95=%s p99=%s max=%s (n=%d)\n",
			pct(latencies, 0.50).Round(time.Microsecond),
			pct(latencies, 0.95).Round(time.Microsecond),
			pct(latencies, 0.99).Round(time.Microsecond),
			latencies[len(latencies)-1].Round(time.Microsecond),
			len(latencies),
		)
	}
	fmt.Println("=======================================")
}

func pct(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := int(p * float64(len(sorted)))
	if i >= len(sorted) {
		i = len(sorted) - 1
	}
	return sorted[i]
}
