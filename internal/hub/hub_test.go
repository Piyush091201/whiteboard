package hub

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/Piyush091201/whiteboard/internal/protocol"
)

// TestMain runs every test under goleak, which fails the suite if any goroutine
// (a board run loop, a read pump, a write pump) is still alive at the end. This
// is the automated proof that the hub never leaks goroutines.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// fakeConn is an in-memory hub.Conn for tests. Inbound messages are injected on
// readCh; outbound messages appear on writeCh. A writeCh with no buffer and no
// reader simulates a slow client (Write blocks), which is how we exercise
// backpressure.
type fakeConn struct {
	readCh  chan []byte
	writeCh chan []byte
	closed  chan struct{}
	once    sync.Once
}

func newFakeConn(writeBuf int) *fakeConn {
	return &fakeConn{
		readCh:  make(chan []byte, 8),
		writeCh: make(chan []byte, writeBuf),
		closed:  make(chan struct{}),
	}
}

func (f *fakeConn) Read(ctx context.Context) ([]byte, error) {
	select {
	case data := <-f.readCh:
		return data, nil
	case <-f.closed:
		return nil, io.EOF
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (f *fakeConn) Write(ctx context.Context, data []byte) error {
	cp := make([]byte, len(data))
	copy(cp, data)
	select {
	case f.writeCh <- cp:
		return nil
	case <-f.closed:
		return io.EOF
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (f *fakeConn) Ping(ctx context.Context) error {
	select {
	case <-f.closed:
		return io.EOF
	default:
		return nil
	}
}

func (f *fakeConn) Close() error {
	f.once.Do(func() { close(f.closed) })
	return nil
}

// --- helpers ---------------------------------------------------------------

// shapeCreate builds the wire bytes for a shape.create from a client. The
// inbound seq is zero; the board assigns the authoritative one.
func shapeCreate(t *testing.T, id, shape string) []byte {
	t.Helper()
	data, err := protocol.Marshal(protocol.TypeShapeCreate, 0, protocol.ShapeOp{
		ID:    id,
		Shape: json.RawMessage(shape),
	})
	if err != nil {
		t.Fatalf("build shape.create: %v", err)
	}
	return data
}

// readEnvelope reads and decodes one outbound envelope, failing on timeout.
func readEnvelope(t *testing.T, ch <-chan []byte, timeout time.Duration) protocol.Envelope {
	t.Helper()
	select {
	case raw := <-ch:
		var env protocol.Envelope
		if err := json.Unmarshal(raw, &env); err != nil {
			t.Fatalf("decode outbound: %v", err)
		}
		return env
	case <-time.After(timeout):
		t.Fatal("timed out waiting for an outbound message")
		return protocol.Envelope{}
	}
}

// waitForClients blocks until the board reports exactly n clients, or fails.
func waitForClients(t *testing.T, h *Hub, board string, n int) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		if got, ok := h.boardClientCount(board); ok && got == n {
			return
		} else if !ok && n == 0 {
			return
		}
		select {
		case <-deadline:
			got, _ := h.boardClientCount(board)
			t.Fatalf("board %q: wanted %d clients, have %d", board, n, got)
		case <-time.After(2 * time.Millisecond):
		}
	}
}

// waitForBoardGone blocks until the board has been removed from the registry.
func waitForBoardGone(t *testing.T, h *Hub, board string) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		h.mu.Lock()
		_, exists := h.boards[board]
		h.mu.Unlock()
		if !exists {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("board %q was not torn down", board)
		case <-time.After(2 * time.Millisecond):
		}
	}
}

// --- tests -----------------------------------------------------------------

// TestFanoutSequencesAndDelivers verifies that a shape op from one client is
// assigned an authoritative sequence number and delivered to every client,
// including the origin.
func TestFanoutSequencesAndDelivers(t *testing.T) {
	h := New(testLogger())
	ctx, cancel := context.WithCancel(context.Background())

	a := newFakeConn(8)
	b := newFakeConn(8)
	go h.Serve(ctx, "board1", a)
	go h.Serve(ctx, "board1", b)
	waitForClients(t, h, "board1", 2)

	// Each client first receives its (empty) snapshot on join.
	if env := readEnvelope(t, a.writeCh, time.Second); env.Type != protocol.TypeSnapshot {
		t.Fatalf("a first message = %q, want snapshot", env.Type)
	}
	if env := readEnvelope(t, b.writeCh, time.Second); env.Type != protocol.TypeSnapshot {
		t.Fatalf("b first message = %q, want snapshot", env.Type)
	}

	a.readCh <- shapeCreate(t, "s1", `{"kind":"rect"}`)

	// b receives the sequenced op.
	env := readEnvelope(t, b.writeCh, time.Second)
	if env.Type != protocol.TypeShapeCreate || env.Seq != 1 {
		t.Fatalf("b got type=%q seq=%d, want shape.create seq=1", env.Type, env.Seq)
	}
	var op protocol.ShapeOp
	if err := env.DecodePayload(&op); err != nil || op.ID != "s1" {
		t.Fatalf("b op = %+v err=%v, want id s1", op, err)
	}

	// The origin also receives the authoritative, sequenced op.
	if envA := readEnvelope(t, a.writeCh, time.Second); envA.Type != protocol.TypeShapeCreate || envA.Seq != 1 {
		t.Fatalf("origin got type=%q seq=%d, want shape.create seq=1", envA.Type, envA.Seq)
	}

	cancel()
	waitForBoardGone(t, h, "board1")
}

// TestSnapshotOnJoin verifies that a client joining a board with existing state
// receives that state as a snapshot.
func TestSnapshotOnJoin(t *testing.T) {
	h := New(testLogger())
	ctx, cancel := context.WithCancel(context.Background())

	a := newFakeConn(8)
	go h.Serve(ctx, "room", a)
	waitForClients(t, h, "room", 1)
	readEnvelope(t, a.writeCh, time.Second) // drain a's empty snapshot

	a.readCh <- shapeCreate(t, "s1", `{"kind":"rect"}`)
	readEnvelope(t, a.writeCh, time.Second) // drain a's echo of its own op

	// A second client joins and should be handed the current state.
	b := newFakeConn(8)
	go h.Serve(ctx, "room", b)
	waitForClients(t, h, "room", 2)

	env := readEnvelope(t, b.writeCh, time.Second)
	if env.Type != protocol.TypeSnapshot {
		t.Fatalf("b first message = %q, want snapshot", env.Type)
	}
	var snap protocol.Snapshot
	if err := env.DecodePayload(&snap); err != nil {
		t.Fatalf("decode snapshot: %v", err)
	}
	if snap.Seq != 1 {
		t.Errorf("snapshot seq = %d, want 1", snap.Seq)
	}
	if len(snap.Shapes) != 1 || snap.Shapes[0].ID != "s1" {
		t.Fatalf("snapshot shapes = %+v, want one shape s1", snap.Shapes)
	}

	cancel()
	waitForBoardGone(t, h, "room")
}

// TestLifecycleCleanup verifies that clients register, then that the board is
// fully torn down once they all disconnect. Combined with goleak in TestMain,
// this proves the connection lifecycle leaves nothing behind.
func TestLifecycleCleanup(t *testing.T) {
	h := New(testLogger())
	ctx, cancel := context.WithCancel(context.Background())

	const n = 5
	conns := make([]*fakeConn, n)
	for i := range conns {
		conns[i] = newFakeConn(8)
		go h.Serve(ctx, "room", conns[i])
	}
	waitForClients(t, h, "room", n)

	for _, c := range conns {
		_ = c.Close()
	}
	waitForBoardGone(t, h, "room")

	cancel()
}

// TestBackpressureKicksSlowClient verifies that one slow consumer cannot stall
// the board: it is kicked once its buffer overflows, while a healthy client on
// the same board keeps working.
func TestBackpressureKicksSlowClient(t *testing.T) {
	h := New(testLogger())
	ctx, cancel := context.WithCancel(context.Background())

	fast := newFakeConn(8) // its outbound is drained below, so it never backs up
	slow := newFakeConn(0) // unbuffered writes, never drained -> always blocked
	go h.Serve(ctx, "b", fast)
	go h.Serve(ctx, "b", slow)
	waitForClients(t, h, "b", 2)

	// Continuously drain the fast client's outbound (it receives every echo).
	go func() {
		for {
			select {
			case <-fast.writeCh:
			case <-ctx.Done():
				return
			}
		}
	}()

	op := shapeCreate(t, "s1", `{"kind":"rect"}`)
	go func() {
		for i := 0; i < sendBuffer*4; i++ {
			select {
			case fast.readCh <- op:
			case <-ctx.Done():
				return
			}
		}
	}()

	select {
	case <-slow.closed:
		// kicked: the board closed its connection
	case <-time.After(2 * time.Second):
		t.Fatal("slow client was not kicked")
	}

	waitForClients(t, h, "b", 1)

	cancel()
	waitForBoardGone(t, h, "b")
}
