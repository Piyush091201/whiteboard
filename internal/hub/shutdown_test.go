package hub

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Piyush091201/whiteboard/internal/broker"
	"github.com/Piyush091201/whiteboard/internal/protocol"
	"github.com/Piyush091201/whiteboard/internal/store"
)

func assertClosed(t *testing.T, f *fakeConn) {
	t.Helper()
	select {
	case <-f.closed:
	case <-time.After(time.Second):
		t.Fatal("connection was not closed by the drain")
	}
}

func waitWG(t *testing.T, wg *sync.WaitGroup, d time.Duration) {
	t.Helper()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatal("timed out waiting for Serve calls to return")
	}
}

// TestShutdownDrainsSessions verifies that Shutdown closes every connection,
// tears down boards, and returns only once all Serve calls have finished.
func TestShutdownDrainsSessions(t *testing.T) {
	h := New(testLogger(), broker.NewMemory())
	ctx := context.Background()

	a := newFakeConn(8)
	b := newFakeConn(8)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); h.Serve(ctx, "board1", ClientInfo{}, a) }()
	go func() { defer wg.Done(); h.Serve(ctx, "board1", ClientInfo{}, b) }()
	waitForClients(t, h, "board1", 2)

	if err := h.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	assertClosed(t, a)
	assertClosed(t, b)
	waitForBoardGone(t, h, "board1")
	waitWG(t, &wg, 2*time.Second) // Serve calls returned
}

// TestServeRejectedAfterShutdown verifies that a connection arriving after
// Shutdown is rejected promptly (not left hanging) and closed.
func TestServeRejectedAfterShutdown(t *testing.T) {
	h := New(testLogger(), broker.NewMemory())
	if err := h.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	c := newFakeConn(8)
	done := make(chan struct{})
	go func() { h.Serve(context.Background(), "board2", ClientInfo{}, c); close(done) }()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Serve did not return after shutdown")
	}
	assertClosed(t, c)
	waitForBoardGone(t, h, "board2") // no board was created
}

// TestShutdownPersistsFinalSnapshot verifies that draining a board on shutdown
// writes a final durable snapshot.
func TestShutdownPersistsFinalSnapshot(t *testing.T) {
	st := store.NewMemory()
	h := New(testLogger(), broker.NewMemory(), WithStore(st))
	defer h.Close()

	ctx := context.Background()
	a := newFakeConn(8)
	go h.Serve(ctx, "board1", ClientInfo{}, a)
	waitForClients(t, h, "board1", 1)

	a.readCh <- shapeCreate(t, "s1", `{"kind":"rect"}`)
	readUntilType(t, a.writeCh, protocol.TypeShapeCreate, time.Second)

	if err := h.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	// Shutdown waits for board.done, which is closed after persistOnClose, so the
	// snapshot is already durable — no polling needed.
	snap, ok, err := st.LoadSnapshot(ctx, "board1")
	if err != nil || !ok {
		t.Fatalf("load = (ok=%v, err=%v), want persisted", ok, err)
	}
	if len(snap.Shapes) != 1 || snap.Shapes[0].ID != "s1" {
		t.Fatalf("persisted snapshot = %+v, want shape s1", snap)
	}
}
