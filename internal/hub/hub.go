// Package hub is the real-time core of the whiteboard: a hub-and-spoke design
// where each board is an actor goroutine that owns its set of connected clients
// and fans out messages over channels.
//
// Concurrency model:
//
//   - Hub holds the registry of boards behind a mutex. This is the only lock in
//     the package, and it guards board lifecycle (create/lookup/teardown) plus a
//     keep-open count — never the message hot path.
//   - Each Board runs a single goroutine (see board.go) that owns its client set
//     with no lock.
//   - Each connection is a Client (see client.go) with one read goroutine and one
//     write goroutine, guaranteed to exit on disconnect — no goroutine leaks.
package hub

import (
	"log/slog"
	"sync"
)

// boardEntry tracks a live board plus the number of clients keeping it open.
// count includes clients that have been admitted by acquire but not yet
// registered in the board's run loop, which is what makes teardown race-free:
// the count is bumped under the mutex before a join can begin, so a board can
// never be torn down while a join for it is in flight.
type boardEntry struct {
	board *Board
	count int
}

// Hub is the registry of active boards. It is safe for concurrent use.
type Hub struct {
	log *slog.Logger

	mu     sync.Mutex
	boards map[string]*boardEntry
}

// New constructs a Hub. A nil logger falls back to slog.Default().
func New(log *slog.Logger) *Hub {
	if log == nil {
		log = slog.Default()
	}
	return &Hub{log: log, boards: make(map[string]*boardEntry)}
}

// acquire returns the board for id, creating and starting it if necessary, and
// increments its keep-open count. Every acquire must be paired with exactly one
// release.
func (h *Hub) acquire(id string) *Board {
	h.mu.Lock()
	defer h.mu.Unlock()

	e, ok := h.boards[id]
	if !ok {
		e = &boardEntry{board: newBoard(id, h.log)}
		h.boards[id] = e
		go e.board.run()
	}
	e.count++
	return e.board
}

// release decrements the keep-open count for the client's board. When the last
// client leaves, the board is removed from the registry and told to stop;
// otherwise the client is unregistered from fan-out.
//
// The select on b.done in the non-last path closes a real race: a concurrent
// release that takes the count to zero may close b.quit (stopping the board)
// before this send lands. Without the b.done arm, that send would block
// forever; with it, the board's own shutdown drains this client instead.
func (h *Hub) release(c *Client) {
	b := c.board

	h.mu.Lock()
	e, ok := h.boards[b.id]
	if !ok || e.board != b {
		h.mu.Unlock()
		return
	}
	e.count--
	last := e.count == 0
	if last {
		delete(h.boards, b.id)
	}
	h.mu.Unlock()

	if last {
		close(b.quit) // board shuts down and drains all remaining clients
		return
	}
	select {
	case b.unregister <- c:
	case <-b.done:
	}
}

// boardClientCount reports how many clients a board's run loop currently holds.
// It asks the board itself, so it never races on the client set. Returns
// (0, false) if the board does not exist or is shutting down.
func (h *Hub) boardClientCount(id string) (int, bool) {
	h.mu.Lock()
	e, ok := h.boards[id]
	h.mu.Unlock()
	if !ok {
		return 0, false
	}
	ch := make(chan int)
	select {
	case e.board.inspect <- ch:
		return <-ch, true
	case <-e.board.done:
		return 0, false
	}
}
