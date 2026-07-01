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
	h1 := New(testLogger(), bk)
	h2 := New(testLogger(), bk)

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
