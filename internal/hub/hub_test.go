package hub

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"
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

// TestFanout verifies that a message from one client reaches the others and is
// NOT echoed back to its sender.
func TestFanout(t *testing.T) {
	h := New(testLogger())
	ctx, cancel := context.WithCancel(context.Background())

	a := newFakeConn(8)
	b := newFakeConn(8)
	go h.Serve(ctx, "board1", a)
	go h.Serve(ctx, "board1", b)
	waitForClients(t, h, "board1", 2)

	a.readCh <- []byte("hello")

	select {
	case got := <-b.writeCh:
		if string(got) != "hello" {
			t.Fatalf("b received %q, want %q", got, "hello")
		}
	case <-time.After(time.Second):
		t.Fatal("b did not receive the broadcast")
	}

	// The origin must not receive its own message.
	select {
	case got := <-a.writeCh:
		t.Fatalf("origin received its own message: %q", got)
	case <-time.After(50 * time.Millisecond):
	}

	cancel()
	waitForBoardGone(t, h, "board1")
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

	// Disconnect every client by closing its connection (read pump returns EOF).
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

	sender := newFakeConn(8) // origin; never receives, so never blocks
	slow := newFakeConn(0)   // unbuffered writes, nobody reads -> always blocked
	go h.Serve(ctx, "b", sender)
	go h.Serve(ctx, "b", slow)
	waitForClients(t, h, "b", 2)

	// Flood enough messages to overflow the slow client's send buffer.
	go func() {
		for i := 0; i < sendBuffer*4; i++ {
			select {
			case sender.readCh <- []byte("x"):
			case <-ctx.Done():
				return
			}
		}
	}()

	select {
	case <-slow.closed:
		// kicked: its connection was closed by the board
	case <-time.After(2 * time.Second):
		t.Fatal("slow client was not kicked")
	}

	// The healthy sender stays; the board converges to a single client.
	waitForClients(t, h, "b", 1)

	cancel()
	waitForBoardGone(t, h, "b")
}
