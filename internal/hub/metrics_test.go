package hub

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Piyush091201/whiteboard/internal/broker"
	"github.com/Piyush091201/whiteboard/internal/protocol"
)

// countingMetrics is a concurrency-safe Metrics used to assert the hub
// instruments the right events.
type countingMetrics struct {
	conns     atomic.Int64
	boards    atomic.Int64
	received  atomic.Int64
	delivered atomic.Int64
	kicked    atomic.Int64
}

func (m *countingMetrics) ConnOpened()             { m.conns.Add(1) }
func (m *countingMetrics) ConnClosed()             { m.conns.Add(-1) }
func (m *countingMetrics) BoardOpened()            { m.boards.Add(1) }
func (m *countingMetrics) BoardClosed()            { m.boards.Add(-1) }
func (m *countingMetrics) MessageReceived()        { m.received.Add(1) }
func (m *countingMetrics) MessagesDelivered(n int) { m.delivered.Add(int64(n)) }
func (m *countingMetrics) ClientKicked()           { m.kicked.Add(1) }

func waitForInt64(t *testing.T, name string, get func() int64, want int64) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		if got := get(); got == want {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("%s = %d, want %d", name, get(), want)
		case <-time.After(2 * time.Millisecond):
		}
	}
}

// TestMetricsInstrumentation verifies the hub drives the Metrics hooks:
// connection and board gauges track lifecycle, and traffic counters advance.
func TestMetricsInstrumentation(t *testing.T) {
	m := &countingMetrics{}
	h := New(testLogger(), broker.NewMemory(), WithMetrics(m))

	ctx, cancel := context.WithCancel(context.Background())

	a := newFakeConn(8)
	go h.Serve(ctx, "board1", ClientInfo{}, a)
	waitForClients(t, h, "board1", 1)

	waitForInt64(t, "active connections", m.conns.Load, 1)
	waitForInt64(t, "active boards", m.boards.Load, 1)

	a.readCh <- shapeCreate(t, "s1", `{"kind":"rect"}`)
	readUntilType(t, a.writeCh, protocol.TypeShapeCreate, time.Second)

	waitForInt64(t, "messages received", m.received.Load, 1)
	if m.delivered.Load() == 0 {
		t.Fatal("expected some delivered messages (snapshot, presence, op loop-back)")
	}

	cancel()
	waitForBoardGone(t, h, "board1")
	waitForInt64(t, "active connections", m.conns.Load, 0)
	waitForInt64(t, "active boards", m.boards.Load, 0)
}
