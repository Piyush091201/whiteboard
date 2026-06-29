package hub

import "log/slog"

// broadcast is an internal message to be fanned out to a board's clients.
// origin, when non-nil, is the client that produced the message and is excluded
// from delivery (it already has the update locally). A nil origin means the
// message came from elsewhere (e.g. Redis fan-out in a later phase) and goes to
// everyone.
type broadcast struct {
	origin *Client
	data   []byte
}

// Board is the per-board actor. All access to its client set happens inside the
// single run() goroutine via the channels below, so the set needs no mutex.
//
// For a C# developer: this is the actor model. Instead of locking a shared
// ConcurrentDictionary of connections, we give the board its own goroutine and
// serialize every mutation through channels — "share memory by communicating".
type Board struct {
	id  string
	log *slog.Logger

	register   chan *Client
	unregister chan *Client
	broadcast  chan broadcast
	inspect    chan chan int // request the current client count (used by metrics/tests)
	quit       chan struct{} // closed by the hub when the board should stop
	done       chan struct{} // closed when run() has returned

	// clients is owned exclusively by run(); never touch it from another
	// goroutine.
	clients map[*Client]struct{}
}

func newBoard(id string, log *slog.Logger) *Board {
	return &Board{
		id:         id,
		log:        log.With("board", id),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		broadcast:  make(chan broadcast),
		inspect:    make(chan chan int),
		quit:       make(chan struct{}),
		done:       make(chan struct{}),
		clients:    make(map[*Client]struct{}),
	}
}

// run is the board's event loop. It is the only goroutine that reads or writes
// b.clients. It exits when the hub closes b.quit (which happens only after the
// last client has left), draining every remaining client cleanly first.
func (b *Board) run() {
	defer close(b.done)
	for {
		select {
		case c := <-b.register:
			b.clients[c] = struct{}{}
			b.log.Debug("client registered", "client", c.id, "clients", len(b.clients))
		case c := <-b.unregister:
			b.remove(c)
		case m := <-b.broadcast:
			b.fanout(m)
		case ch := <-b.inspect:
			ch <- len(b.clients)
		case <-b.quit:
			b.shutdown()
			return
		}
	}
}

// remove drops a client from the fan-out set and closes its send channel.
// Idempotent: a client that was already removed (e.g. previously kicked for
// being slow) is a no-op. Because every close of c.send happens here, in the
// single run() goroutine, and is guarded by map membership, a send channel is
// never closed twice.
func (b *Board) remove(c *Client) {
	if _, ok := b.clients[c]; !ok {
		return
	}
	delete(b.clients, c)
	close(c.send)
}

// fanout delivers data to every client except the origin. The send is
// non-blocking: a client whose buffered channel is full is "kicked" rather than
// allowed to stall the whole board. This is the backpressure guarantee — one
// slow consumer can never block fan-out for everyone else.
//
// For a C# developer: the select-with-default is exactly
// Channel<T>.Writer.TryWrite — a non-blocking offer.
func (b *Board) fanout(m broadcast) {
	for c := range b.clients {
		if c == m.origin {
			continue
		}
		select {
		case c.send <- m.data:
		default:
			b.log.Warn("kicking slow client: send buffer full", "client", c.id)
			b.kick(c)
		}
	}
}

// kick forcibly disconnects a client that cannot keep up. Closing its send
// channel stops the write pump; closing the connection unblocks the read pump.
// The client then reconnects and resyncs from the latest snapshot.
//
// Deleting the current key during a range over the same map is safe in Go.
func (b *Board) kick(c *Client) {
	delete(b.clients, c)
	close(c.send)
	_ = c.conn.Close()
}

// shutdown drains every remaining client when the board stops.
func (b *Board) shutdown() {
	for c := range b.clients {
		delete(b.clients, c)
		close(c.send)
		_ = c.conn.Close()
	}
	b.log.Debug("board stopped")
}
