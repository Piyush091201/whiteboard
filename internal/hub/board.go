package hub

import (
	"log/slog"

	"github.com/Piyush091201/whiteboard/internal/protocol"
)

// inbound is a parsed message from a client, handed to the board's run loop for
// sequencing and fan-out. Because the board goroutine is the single writer of
// board state, it can assign sequence numbers with no locking.
type inbound struct {
	origin *Client
	env    protocol.Envelope
}

// Board is the per-board actor. All access to its client set and document
// happens inside the single run() goroutine via the channels below, so neither
// needs a mutex.
//
// For a C# developer: this is the actor model. Instead of locking a shared
// ConcurrentDictionary of connections and a shared document, we give the board
// its own goroutine and serialize every mutation through channels — "share
// memory by communicating".
type Board struct {
	id  string
	log *slog.Logger

	register   chan *Client
	unregister chan *Client
	inbound    chan inbound
	inspect    chan chan int // request the current client count (used by metrics/tests)
	quit       chan struct{} // closed by the hub when the board should stop
	done       chan struct{} // closed when run() has returned

	// clients and doc are owned exclusively by run(); never touch them from
	// another goroutine.
	clients map[*Client]struct{}
	doc     *document
}

func newBoard(id string, log *slog.Logger) *Board {
	return &Board{
		id:         id,
		log:        log.With("board", id),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		inbound:    make(chan inbound),
		inspect:    make(chan chan int),
		quit:       make(chan struct{}),
		done:       make(chan struct{}),
		clients:    make(map[*Client]struct{}),
		doc:        newDocument(),
	}
}

// run is the board's event loop. It is the only goroutine that reads or writes
// b.clients and b.doc. It exits when the hub closes b.quit (which happens only
// after the last client has left), draining every remaining client first.
func (b *Board) run() {
	defer close(b.done)
	for {
		select {
		case c := <-b.register:
			b.clients[c] = struct{}{}
			b.sendSnapshot(c)
			b.log.Debug("client registered", "client", c.id, "clients", len(b.clients))
		case c := <-b.unregister:
			b.remove(c)
		case in := <-b.inbound:
			b.handle(in)
		case ch := <-b.inspect:
			ch <- len(b.clients)
		case <-b.quit:
			b.shutdown()
			return
		}
	}
}

// handle processes one inbound message. Shape operations are applied to the
// document under last-write-wins — the board assigns the authoritative sequence
// number — and the sequenced result is fanned out to every client (including
// the origin, so its optimistic local edit is reconciled with the server's
// ordering).
func (b *Board) handle(in inbound) {
	switch in.env.Type {
	case protocol.TypeShapeCreate, protocol.TypeShapeUpdate, protocol.TypeShapeDelete:
		var op protocol.ShapeOp
		if err := in.env.DecodePayload(&op); err != nil || op.ID == "" {
			b.log.Warn("dropping invalid shape op", "type", in.env.Type, "err", err)
			return
		}
		seq := b.doc.apply(in.env.Type, op)
		out, err := protocol.Marshal(in.env.Type, seq, op)
		if err != nil {
			b.log.Error("failed to encode outbound op", "err", err)
			return
		}
		b.deliver(out, nil) // authoritative op goes to everyone
	default:
		b.log.Warn("ignoring unknown message type", "type", in.env.Type)
	}
}

// sendSnapshot pushes the current board state to a newly joined client so it
// renders in sync immediately.
func (b *Board) sendSnapshot(c *Client) {
	data, err := protocol.Marshal(protocol.TypeSnapshot, 0, b.doc.snapshot())
	if err != nil {
		b.log.Error("failed to encode snapshot", "err", err)
		return
	}
	select {
	case c.send <- data:
	default:
		// A just-registered client has an empty buffer, so this should not
		// happen; kick defensively rather than block the board.
		b.kick(c)
	}
}

// deliver sends data to every client except exclude (nil means all). The send
// is non-blocking: a client whose buffered channel is full is "kicked" rather
// than allowed to stall the whole board. This is the backpressure guarantee —
// one slow consumer can never block fan-out for everyone else.
//
// For a C# developer: the select-with-default is exactly
// Channel<T>.Writer.TryWrite — a non-blocking offer.
func (b *Board) deliver(data []byte, exclude *Client) {
	for c := range b.clients {
		if c == exclude {
			continue
		}
		select {
		case c.send <- data:
		default:
			b.log.Warn("kicking slow client: send buffer full", "client", c.id)
			b.kick(c)
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

// kick forcibly disconnects a client that cannot keep up. Closing its send
// channel stops the write pump; closing the connection unblocks the read pump.
// The client then reconnects and resyncs from the snapshot it receives on join.
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
