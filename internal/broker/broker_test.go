package broker_test

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/Piyush091201/whiteboard/internal/broker"
)

// withBrokers runs fn against every Broker implementation, so the contract is
// verified identically for the in-memory and Redis-backed brokers.
func withBrokers(t *testing.T, fn func(t *testing.T, b broker.Broker)) {
	t.Helper()

	t.Run("memory", func(t *testing.T) {
		b := broker.NewMemory()
		t.Cleanup(func() { _ = b.Close() })
		fn(t, b)
	})

	t.Run("redis", func(t *testing.T) {
		mr := miniredis.RunT(t) // auto-closed at test end
		rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		b := broker.NewRedis(rdb)
		t.Cleanup(func() { _ = b.Close() })
		fn(t, b)
	})
}

func TestApplyShapeSequencesAndSnapshot(t *testing.T) {
	withBrokers(t, func(t *testing.T, b broker.Broker) {
		ctx := context.Background()

		seq1, err := b.ApplyShape(ctx, "room", "s1", []byte(`"v1"`), false)
		if err != nil {
			t.Fatalf("apply create: %v", err)
		}
		if seq1 != 1 {
			t.Fatalf("first seq = %d, want 1", seq1)
		}

		seq2, err := b.ApplyShape(ctx, "room", "s1", []byte(`"v2"`), false)
		if err != nil {
			t.Fatalf("apply update: %v", err)
		}
		if seq2 != 2 {
			t.Fatalf("second seq = %d, want 2", seq2)
		}

		snap, err := b.Snapshot(ctx, "room")
		if err != nil {
			t.Fatalf("snapshot: %v", err)
		}
		if snap.Seq != 2 {
			t.Errorf("snapshot seq = %d, want 2", snap.Seq)
		}
		if len(snap.Shapes) != 1 || string(snap.Shapes[0].Shape) != `"v2"` {
			t.Fatalf("snapshot = %+v, want single shape with latest value", snap.Shapes)
		}

		if _, err := b.ApplyShape(ctx, "room", "s1", nil, true); err != nil {
			t.Fatalf("apply delete: %v", err)
		}
		snap, err = b.Snapshot(ctx, "room")
		if err != nil {
			t.Fatalf("snapshot after delete: %v", err)
		}
		if len(snap.Shapes) != 0 {
			t.Errorf("after delete want 0 shapes, got %d", len(snap.Shapes))
		}
	})
}

func TestSnapshotOrderedBySeq(t *testing.T) {
	withBrokers(t, func(t *testing.T, b broker.Broker) {
		ctx := context.Background()
		mustApply(t, b, "room", "a", `1`) // seq 1
		mustApply(t, b, "room", "b", `2`) // seq 2
		mustApply(t, b, "room", "a", `3`) // seq 3 -> a becomes newest

		snap, err := b.Snapshot(ctx, "room")
		if err != nil {
			t.Fatalf("snapshot: %v", err)
		}
		if len(snap.Shapes) != 2 {
			t.Fatalf("want 2 shapes, got %d", len(snap.Shapes))
		}
		if snap.Shapes[0].ID != "b" || snap.Shapes[1].ID != "a" {
			t.Errorf("order = [%s %s], want [b a]", snap.Shapes[0].ID, snap.Shapes[1].ID)
		}
	})
}

// TestPubSubDeliversToAllSubscribers proves the fan-out contract: a message
// published once reaches every subscriber — the mechanism that lets two
// instances share a board.
func TestPubSubDeliversToAllSubscribers(t *testing.T) {
	withBrokers(t, func(t *testing.T, b broker.Broker) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		sub1, err := b.Subscribe(ctx, "room")
		if err != nil {
			t.Fatalf("subscribe 1: %v", err)
		}
		sub2, err := b.Subscribe(ctx, "room")
		if err != nil {
			t.Fatalf("subscribe 2: %v", err)
		}

		if err := b.Publish(ctx, "room", []byte("hello")); err != nil {
			t.Fatalf("publish: %v", err)
		}

		for i, sub := range []<-chan []byte{sub1, sub2} {
			select {
			case got := <-sub:
				if string(got) != "hello" {
					t.Fatalf("sub %d got %q, want hello", i, got)
				}
			case <-time.After(2 * time.Second):
				t.Fatalf("sub %d did not receive the message", i)
			}
		}
	})
}

// TestPresenceRoster verifies the global roster: participants can be added,
// listed (ordered), and removed — the state a joiner reads to see who is present
// across all instances.
func TestPresenceRoster(t *testing.T) {
	withBrokers(t, func(t *testing.T, b broker.Broker) {
		ctx := context.Background()

		if err := b.SetPresence(ctx, "room", "u1", []byte(`{"clientId":"u1","name":"Ada"}`)); err != nil {
			t.Fatalf("set u1: %v", err)
		}
		if err := b.SetPresence(ctx, "room", "u2", []byte(`{"clientId":"u2","name":"Bob"}`)); err != nil {
			t.Fatalf("set u2: %v", err)
		}

		roster, err := b.Presence(ctx, "room")
		if err != nil {
			t.Fatalf("presence: %v", err)
		}
		if len(roster) != 2 || roster[0].ClientID != "u1" || roster[1].ClientID != "u2" {
			t.Fatalf("roster = %+v, want [u1 u2]", roster)
		}
		if roster[0].Name != "Ada" {
			t.Errorf("u1 name = %q, want Ada", roster[0].Name)
		}

		if err := b.RemovePresence(ctx, "room", "u1"); err != nil {
			t.Fatalf("remove u1: %v", err)
		}
		roster, err = b.Presence(ctx, "room")
		if err != nil {
			t.Fatalf("presence after remove: %v", err)
		}
		if len(roster) != 1 || roster[0].ClientID != "u2" {
			t.Fatalf("roster after remove = %+v, want [u2]", roster)
		}
	})
}

func mustApply(t *testing.T, b broker.Broker, board, id, shape string) {
	t.Helper()
	if _, err := b.ApplyShape(context.Background(), board, id, []byte(shape), false); err != nil {
		t.Fatalf("apply %s: %v", id, err)
	}
}
