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

	"github.com/Piyush091201/whiteboard/internal/broker"
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

func cursorMsg(t *testing.T, x, y float64) []byte {
	t.Helper()
	data, err := protocol.Marshal(protocol.TypeCursor, 0, protocol.Cursor{X: x, Y: y})
	if err != nil {
		t.Fatalf("build cursor: %v", err)
	}
	return data
}

// decodePayload decodes an envelope payload into v, failing the test on error.
func decodePayload(t *testing.T, env protocol.Envelope, v any) {
	t.Helper()
	if err := env.DecodePayload(v); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
}

// readUntilType reads and decodes outbound messages, skipping types other than
// want (e.g. the snapshot and presence frames a client receives on join), and
// returns the first envelope of type want. Fails on timeout.
func readUntilType(t *testing.T, ch <-chan []byte, want protocol.Type, timeout time.Duration) protocol.Envelope {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case raw := <-ch:
			var env protocol.Envelope
			if err := json.Unmarshal(raw, &env); err != nil {
				t.Fatalf("decode outbound: %v", err)
			}
			if env.Type == want {
				return env
			}
		case <-deadline:
			t.Fatalf("timed out waiting for message type %q", want)
			return protocol.Envelope{}
		}
	}
}

// expectNoMessageOfType reads for dur and fails if a message of type notWant
// arrives. Messages of other types are ignored.
func expectNoMessageOfType(t *testing.T, ch <-chan []byte, notWant protocol.Type, dur time.Duration) {
	t.Helper()
	deadline := time.After(dur)
	for {
		select {
		case raw := <-ch:
			var env protocol.Envelope
			if err := json.Unmarshal(raw, &env); err != nil {
				t.Fatalf("decode outbound: %v", err)
			}
			if env.Type == notWant {
				t.Fatalf("unexpectedly received a message of type %q", notWant)
			}
		case <-deadline:
			return
		}
	}
}

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
	h := New(testLogger(), broker.NewMemory())
	ctx, cancel := context.WithCancel(context.Background())

	a := newFakeConn(8)
	b := newFakeConn(8)
	go h.Serve(ctx, "board1", ClientInfo{Name: "A"}, a)
	go h.Serve(ctx, "board1", ClientInfo{Name: "B"}, b)
	waitForClients(t, h, "board1", 2)

	a.readCh <- shapeCreate(t, "s1", `{"kind":"rect"}`)

	// b receives the sequenced op.
	env := readUntilType(t, b.writeCh, protocol.TypeShapeCreate, time.Second)
	if env.Seq != 1 {
		t.Fatalf("b got seq=%d, want 1", env.Seq)
	}
	var op protocol.ShapeOp
	if err := env.DecodePayload(&op); err != nil || op.ID != "s1" {
		t.Fatalf("b op = %+v err=%v, want id s1", op, err)
	}

	// The origin also receives the authoritative, sequenced op.
	if envA := readUntilType(t, a.writeCh, protocol.TypeShapeCreate, time.Second); envA.Seq != 1 {
		t.Fatalf("origin got seq=%d, want 1", envA.Seq)
	}

	cancel()
	waitForBoardGone(t, h, "board1")
}

// TestSnapshotOnJoin verifies that a client joining a board with existing state
// receives that state as a snapshot.
func TestSnapshotOnJoin(t *testing.T) {
	h := New(testLogger(), broker.NewMemory())
	ctx, cancel := context.WithCancel(context.Background())

	a := newFakeConn(8)
	go h.Serve(ctx, "room", ClientInfo{}, a)
	waitForClients(t, h, "room", 1)

	a.readCh <- shapeCreate(t, "s1", `{"kind":"rect"}`)
	readUntilType(t, a.writeCh, protocol.TypeShapeCreate, time.Second) // a's echo

	b := newFakeConn(8)
	go h.Serve(ctx, "room", ClientInfo{}, b)
	waitForClients(t, h, "room", 2)

	env := readUntilType(t, b.writeCh, protocol.TypeSnapshot, time.Second)
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
	h := New(testLogger(), broker.NewMemory())
	ctx, cancel := context.WithCancel(context.Background())

	const n = 5
	conns := make([]*fakeConn, n)
	for i := range conns {
		conns[i] = newFakeConn(8)
		go h.Serve(ctx, "room", ClientInfo{}, conns[i])
	}
	waitForClients(t, h, "room", n)

	for _, c := range conns {
		_ = c.Close()
	}
	waitForBoardGone(t, h, "room")

	cancel()
}

// TestBackpressureKicksSlowClient verifies that one slow consumer cannot stall
// the board on reliable traffic: it is kicked once its buffer overflows, while
// a healthy client on the same board keeps working.
func TestBackpressureKicksSlowClient(t *testing.T) {
	h := New(testLogger(), broker.NewMemory())
	ctx, cancel := context.WithCancel(context.Background())

	fast := newFakeConn(8) // its outbound is drained below, so it never backs up
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
