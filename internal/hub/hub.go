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
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/Piyush091201/whiteboard/internal/broker"
	"github.com/Piyush091201/whiteboard/internal/store"
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
	log     *slog.Logger
	broker  broker.Broker
	store   store.Store // set via WithStore; nil disables persistence
	persist *persistence
	metrics Metrics

	mu       sync.Mutex
	boards   map[string]*boardEntry
	closing  bool           // set by Shutdown; blocks new sessions
	sessions sync.WaitGroup // tracks in-flight Serve calls

	stop     chan struct{}  // closed to stop the coordinator
	stopOnce sync.Once      // guards close(stop)
	wg       sync.WaitGroup // tracks the coordinator goroutine
}

// Option configures a Hub. Options keep the constructor small as optional
// dependencies (persistence, metrics) are added.
type Option func(*Hub)

// WithStore enables durable persistence backed by s. A nil store is a no-op.
func WithStore(s store.Store) Option {
	return func(h *Hub) { h.store = s }
}

// WithMetrics records hub activity through m. A nil Metrics is a no-op.
func WithMetrics(m Metrics) Option {
	return func(h *Hub) {
		if m != nil {
			h.metrics = m
		}
	}
}

// New constructs a Hub backed by the given broker (shared state and message
// bus). A nil logger falls back to slog.Default(). When WithStore supplies a
// store, a coordinator goroutine periodically snapshots active boards; call
// Close to stop it.
func New(log *slog.Logger, b broker.Broker, opts ...Option) *Hub {
	if log == nil {
		log = slog.Default()
	}
	h := &Hub{
		log:     log,
		broker:  b,
		metrics: nopMetrics{},
		boards:  make(map[string]*boardEntry),
	}
	for _, opt := range opts {
		opt(h)
	}
	if h.store != nil {
		h.persist = &persistence{broker: b, store: h.store, log: log}
		h.stop = make(chan struct{})
		h.wg.Add(1)
		go h.snapshotLoop(snapshotInterval)
	}
	return h
}

// Close stops the persistence coordinator. It is safe to call when persistence
// is disabled and safe to call more than once (Shutdown also calls it).
func (h *Hub) Close() {
	h.stopCoordinator()
}

func (h *Hub) stopCoordinator() {
	if h.stop != nil {
		h.stopOnce.Do(func() { close(h.stop) })
		h.wg.Wait()
	}
}

// Shutdown gracefully drains the hub: it stops accepting new sessions, closes
// every board (which drains its clients, closes their connections, and writes a
// final durable snapshot), and waits for all in-flight connections to finish or
// ctx to expire. It also stops the persistence coordinator. After Shutdown the
// hub rejects new Serve calls.
func (h *Hub) Shutdown(ctx context.Context) error {
	h.mu.Lock()
	if h.closing {
		h.mu.Unlock()
		return nil
	}
	h.closing = true
	boards := make([]*Board, 0, len(h.boards))
	for _, e := range h.boards {
		boards = append(boards, e.board)
	}
	// Clear the registry so a draining client's release becomes a no-op and can
	// never double-close a board.
	h.boards = make(map[string]*boardEntry)
	h.mu.Unlock()

	h.log.Info("draining hub", "boards", len(boards))
	for _, b := range boards {
		b.closeQuit()
	}

	// Wait for each board to finish draining and persisting.
	for _, b := range boards {
		select {
		case <-b.done:
		case <-ctx.Done():
			h.stopCoordinator()
			return ctx.Err()
		}
	}

	// Wait for every Serve call to complete its own cleanup.
	sessionsDone := make(chan struct{})
	go func() {
		h.sessions.Wait()
		close(sessionsDone)
	}()
	select {
	case <-sessionsDone:
	case <-ctx.Done():
		h.stopCoordinator()
		return ctx.Err()
	}

	h.stopCoordinator()
	return nil
}

// snapshotLoop periodically persists every active board until Close is called.
func (h *Hub) snapshotLoop(interval time.Duration) {
	defer h.wg.Done()
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			h.snapshotAllBoards()
		case <-h.stop:
			return
		}
	}
}

func (h *Hub) snapshotAllBoards() {
	h.mu.Lock()
	ids := make([]string, 0, len(h.boards))
	for id := range h.boards {
		ids = append(ids, id)
	}
	h.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), persistTimeout)
	defer cancel()
	for _, id := range ids {
		h.persist.save(ctx, id)
	}
}

// beginSession registers a new connection: under one lock it rejects the
// session if the hub is shutting down, tracks it for graceful drain, and returns
// the board for id (creating and starting it if necessary) with its keep-open
// count incremented. A true result must be paired with exactly one release and
// one sessions.Done. Doing this atomically means a shutdown can never race with
// a new board being created.
func (h *Hub) beginSession(id string) (*Board, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closing {
		return nil, false
	}
	h.sessions.Add(1)

	e, ok := h.boards[id]
	if !ok {
		e = &boardEntry{board: newBoard(id, h.log, h.broker, h.persist, h.metrics)}
		h.boards[id] = e
		go e.board.run()
		h.metrics.BoardOpened()
	}
	e.count++
	return e.board, true
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
		b.closeQuit() // board shuts down and drains all remaining clients
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
