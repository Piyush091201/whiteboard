package hub

import (
	"context"
	"testing"
	"time"

	"github.com/Piyush091201/whiteboard/internal/broker"
	"github.com/Piyush091201/whiteboard/internal/protocol"
)

// TestCrossInstanceShapeSync is the horizontal-scalability proof: two separate
// hubs (standing in for two server instances) share one broker. A shape op made
// by a client on hub 1 reaches a client on hub 2, and both instances then serve
// an identical snapshot.
func TestCrossInstanceShapeSync(t *testing.T) {
	bk := broker.NewMemory() // one shared bus/state for both instances
	h1 := New(testLogger(), bk, nil)
	h2 := New(testLogger(), bk, nil)

	ctx, cancel := context.WithCancel(context.Background())

	a := newFakeConn(8) // connected to instance 1
	go h1.Serve(ctx, "shared", ClientInfo{}, a)
	waitForClients(t, h1, "shared", 1)

	b := newFakeConn(8) // connected to instance 2
	go h2.Serve(ctx, "shared", ClientInfo{}, b)
	waitForClients(t, h2, "shared", 1)

	// A client on instance 1 draws a shape.
	a.readCh <- shapeCreate(t, "s1", `{"kind":"rect"}`)

	// The client on instance 2 receives it, sequenced by the shared broker.
	env := readUntilType(t, b.writeCh, protocol.TypeShapeCreate, 2*time.Second)
	if env.Seq != 1 {
		t.Fatalf("cross-instance op seq = %d, want 1", env.Seq)
	}
	var op protocol.ShapeOp
	decodePayload(t, env, &op)
	if op.ID != "s1" {
		t.Fatalf("cross-instance op id = %q, want s1", op.ID)
	}

	// Both instances serve the same snapshot (state is shared, not per-instance).
	snap, err := bk.Snapshot(ctx, "shared")
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if len(snap.Shapes) != 1 || snap.Shapes[0].ID != "s1" {
		t.Fatalf("shared snapshot = %+v, want one shape s1", snap.Shapes)
	}

	cancel()
	waitForBoardGone(t, h1, "shared")
	waitForBoardGone(t, h2, "shared")
}

// TestCrossInstancePresenceAndCursors proves presence and cursors are global:
// a client on instance 2 sees the roster and cursor of a client on instance 1,
// and departures cross instances too.
func TestCrossInstancePresenceAndCursors(t *testing.T) {
	bk := broker.NewMemory()
	h1 := New(testLogger(), bk, nil)
	h2 := New(testLogger(), bk, nil)

	ctx, cancel := context.WithCancel(context.Background())

	a := newFakeConn(8) // instance 1
	go h1.Serve(ctx, "shared", ClientInfo{Name: "Ada"}, a)
	waitForClients(t, h1, "shared", 1)

	// Read Ada's own presence.state first, so her entry is in the shared roster
	// before Bob joins and reads it.
	var aState protocol.PresenceState
	decodePayload(t, readUntilType(t, a.writeCh, protocol.TypePresenceState, time.Second), &aState)
	adaID := aState.Self.ClientID

	b := newFakeConn(8) // instance 2
	go h2.Serve(ctx, "shared", ClientInfo{Name: "Bob"}, b)
	waitForClients(t, h2, "shared", 1)

	// Bob (instance 2) sees Ada (instance 1) in his roster.
	var bState protocol.PresenceState
	decodePayload(t, readUntilType(t, b.writeCh, protocol.TypePresenceState, time.Second), &bState)
	if len(bState.Others) != 1 || bState.Others[0].ClientID != adaID || bState.Others[0].Name != "Ada" {
		t.Fatalf("bob roster = %+v, want [Ada] across instances", bState.Others)
	}
	bobID := bState.Self.ClientID

	// Ada (instance 1) is told Bob joined on instance 2.
	var join protocol.Presence
	decodePayload(t, readUntilType(t, a.writeCh, protocol.TypePresenceJoin, time.Second), &join)
	if join.ClientID != bobID {
		t.Fatalf("ada saw join %q, want bob %q", join.ClientID, bobID)
	}

	// Ada's cursor (instance 1) reaches Bob (instance 2), stamped with Ada's id.
	a.readCh <- cursorMsg(t, 5, 6)
	var cur protocol.Cursor
	decodePayload(t, readUntilType(t, b.writeCh, protocol.TypeCursor, time.Second), &cur)
	if cur.ClientID != adaID || cur.X != 5 || cur.Y != 6 {
		t.Fatalf("bob cursor = %+v, want Ada's at (5,6)", cur)
	}

	// Bob disconnects on instance 2; Ada on instance 1 is notified.
	_ = b.Close()
	var leave protocol.Presence
	decodePayload(t, readUntilType(t, a.writeCh, protocol.TypePresenceLeave, time.Second), &leave)
	if leave.ClientID != bobID {
		t.Fatalf("ada saw leave %q, want bob %q", leave.ClientID, bobID)
	}

	cancel()
	waitForBoardGone(t, h1, "shared")
	waitForBoardGone(t, h2, "shared")
}
