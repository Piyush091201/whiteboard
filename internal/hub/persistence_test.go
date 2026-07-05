package hub

import (
	"context"
	"testing"
	"time"

	"github.com/Piyush091201/whiteboard/internal/broker"
	"github.com/Piyush091201/whiteboard/internal/protocol"
	"github.com/Piyush091201/whiteboard/internal/store"
)

// waitForSnapshot blocks until the store has a snapshot for the board, or fails.
func waitForSnapshot(t *testing.T, s store.Store, boardID string) protocol.Snapshot {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		if snap, ok, err := s.LoadSnapshot(context.Background(), boardID); err != nil {
			t.Fatalf("load snapshot: %v", err)
		} else if ok {
			return snap
		}
		select {
		case <-deadline:
			t.Fatalf("board %q was never persisted", boardID)
		case <-time.After(2 * time.Millisecond):
		}
	}
}

// TestPersistAndHydrate proves durability across a "Redis restart": a shape
// drawn on one broker is persisted to the store when the board closes, and a
// fresh broker (empty, as after a restart) is rehydrated from the store so a new
// client sees the shape.
func TestPersistAndHydrate(t *testing.T) {
	st := store.NewMemory()

	// --- instance 1: draw a shape, then disconnect so the board persists ---
	b1 := broker.NewMemory()
	h1 := New(testLogger(), b1, st)
	defer h1.Close()

	ctx, cancel := context.WithCancel(context.Background())

	a := newFakeConn(8)
	go h1.Serve(ctx, "board1", ClientInfo{}, a)
	waitForClients(t, h1, "board1", 1)

	a.readCh <- shapeCreate(t, "s1", `{"kind":"rect"}`)
	readUntilType(t, a.writeCh, protocol.TypeShapeCreate, time.Second) // op applied

	_ = a.Close() // disconnect -> board closes -> persists on close
	waitForBoardGone(t, h1, "board1")

	snap := waitForSnapshot(t, st, "board1")
	if snap.Seq != 1 || len(snap.Shapes) != 1 || snap.Shapes[0].ID != "s1" {
		t.Fatalf("persisted snapshot = %+v, want seq 1 with shape s1", snap)
	}

	// --- instance 2: a fresh broker (simulating a restart) rehydrates ---
	b2 := broker.NewMemory()
	h2 := New(testLogger(), b2, st)
	defer h2.Close()

	c := newFakeConn(8)
	go h2.Serve(ctx, "board1", ClientInfo{}, c)
	waitForClients(t, h2, "board1", 1)

	env := readUntilType(t, c.writeCh, protocol.TypeSnapshot, time.Second)
	var got protocol.Snapshot
	decodePayload(t, env, &got)
	if got.Seq != 1 || len(got.Shapes) != 1 || got.Shapes[0].ID != "s1" {
		t.Fatalf("hydrated snapshot = %+v, want the persisted shape s1", got)
	}

	cancel()
	waitForBoardGone(t, h2, "board1")
}
