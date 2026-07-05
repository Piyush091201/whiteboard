package hub

import (
	"context"
	"testing"
	"time"

	"github.com/Piyush091201/whiteboard/internal/broker"
	"github.com/Piyush091201/whiteboard/internal/protocol"
)

// TestPresenceJoinAndState verifies that a joining client learns who is already
// present (presence.state) and that existing clients are told about the
// newcomer (presence.join).
func TestPresenceJoinAndState(t *testing.T) {
	h := New(testLogger(), broker.NewMemory())
	ctx, cancel := context.WithCancel(context.Background())

	a := newFakeConn(8)
	go h.Serve(ctx, "room", ClientInfo{Name: "Ada", Color: "#fff"}, a)
	waitForClients(t, h, "room", 1)

	// Ada joins alone: her presence.state names her and lists no others.
	var aState protocol.PresenceState
	decodePayload(t, readUntilType(t, a.writeCh, protocol.TypePresenceState, time.Second), &aState)
	if aState.Self.Name != "Ada" || aState.Self.Color != "#fff" {
		t.Fatalf("a self = %+v, want name Ada color #fff", aState.Self)
	}
	if len(aState.Others) != 0 {
		t.Fatalf("a others = %+v, want none", aState.Others)
	}
	adaID := aState.Self.ClientID

	b := newFakeConn(8)
	go h.Serve(ctx, "room", ClientInfo{Name: "Bob"}, b)
	waitForClients(t, h, "room", 2)

	// Bob's presence.state lists Ada as already present.
	var bState protocol.PresenceState
	decodePayload(t, readUntilType(t, b.writeCh, protocol.TypePresenceState, time.Second), &bState)
	if bState.Self.Name != "Bob" {
		t.Fatalf("b self = %+v, want name Bob", bState.Self)
	}
	if len(bState.Others) != 1 || bState.Others[0].ClientID != adaID || bState.Others[0].Name != "Ada" {
		t.Fatalf("b others = %+v, want [Ada]", bState.Others)
	}

	// Ada is told Bob joined.
	var join protocol.Presence
	decodePayload(t, readUntilType(t, a.writeCh, protocol.TypePresenceJoin, time.Second), &join)
	if join.ClientID != bState.Self.ClientID || join.Name != "Bob" {
		t.Fatalf("a got join %+v, want Bob", join)
	}

	cancel()
	waitForBoardGone(t, h, "room")
}

// TestCursorRelayExcludesOrigin verifies that a cursor move is relayed to other
// clients, stamped with the origin's id, and not echoed back to the sender.
func TestCursorRelayExcludesOrigin(t *testing.T) {
	h := New(testLogger(), broker.NewMemory())
	ctx, cancel := context.WithCancel(context.Background())

	a := newFakeConn(8)
	b := newFakeConn(8)
	go h.Serve(ctx, "room", ClientInfo{}, a)
	go h.Serve(ctx, "room", ClientInfo{}, b)
	waitForClients(t, h, "room", 2)

	var aState protocol.PresenceState
	decodePayload(t, readUntilType(t, a.writeCh, protocol.TypePresenceState, time.Second), &aState)
	aID := aState.Self.ClientID

	a.readCh <- cursorMsg(t, 10, 20)

	var cur protocol.Cursor
	decodePayload(t, readUntilType(t, b.writeCh, protocol.TypeCursor, time.Second), &cur)
	if cur.X != 10 || cur.Y != 20 {
		t.Fatalf("b cursor = (%v,%v), want (10,20)", cur.X, cur.Y)
	}
	if cur.ClientID != aID {
		t.Fatalf("cursor clientId = %q, want origin %q", cur.ClientID, aID)
	}

	// The origin must not receive its own cursor.
	expectNoMessageOfType(t, a.writeCh, protocol.TypeCursor, 100*time.Millisecond)

	cancel()
	waitForBoardGone(t, h, "room")
}

// TestPresenceLeaveOnDisconnect verifies that remaining clients are notified
// when a participant disconnects.
func TestPresenceLeaveOnDisconnect(t *testing.T) {
	h := New(testLogger(), broker.NewMemory())
	ctx, cancel := context.WithCancel(context.Background())

	a := newFakeConn(8)
	b := newFakeConn(8)
	go h.Serve(ctx, "room", ClientInfo{}, a)
	go h.Serve(ctx, "room", ClientInfo{}, b)
	waitForClients(t, h, "room", 2)

	var bState protocol.PresenceState
	decodePayload(t, readUntilType(t, b.writeCh, protocol.TypePresenceState, time.Second), &bState)
	bID := bState.Self.ClientID

	_ = b.Close() // b disconnects

	var leave protocol.Presence
	decodePayload(t, readUntilType(t, a.writeCh, protocol.TypePresenceLeave, time.Second), &leave)
	if leave.ClientID != bID {
		t.Fatalf("leave clientId = %q, want %q", leave.ClientID, bID)
	}

	waitForClients(t, h, "room", 1)

	cancel()
	waitForBoardGone(t, h, "room")
}

// TestCursorDroppedNotKicked verifies the ephemeral backpressure path: a slow
// client flooded with cursor updates has them dropped rather than being
// disconnected (unlike reliable traffic, which would kick it).
func TestCursorDroppedNotKicked(t *testing.T) {
	h := New(testLogger(), broker.NewMemory())
	ctx, cancel := context.WithCancel(context.Background())

	fast := newFakeConn(8) // origin of the cursor flood; drained below
	slow := newFakeConn(0) // unbuffered writes, never drained -> always blocked
	go h.Serve(ctx, "b", ClientInfo{}, fast)
	go h.Serve(ctx, "b", ClientInfo{}, slow)
	waitForClients(t, h, "b", 2)

	go func() {
		for {
			select {
			case <-fast.writeCh:
			case <-ctx.Done():
				return
			}
		}
	}()

	cur := cursorMsg(t, 1, 1)
	go func() {
		for i := 0; i < sendBuffer*4; i++ {
			select {
			case fast.readCh <- cur:
			case <-ctx.Done():
				return
			}
		}
	}()

	// The slow client must survive the cursor flood (drops, not a kick).
	select {
	case <-slow.closed:
		t.Fatal("slow client was kicked by a cursor flood; cursors must be dropped")
	case <-time.After(500 * time.Millisecond):
	}
	waitForClients(t, h, "b", 2)

	cancel()
	waitForBoardGone(t, h, "b")
}
