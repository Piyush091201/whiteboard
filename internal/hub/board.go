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
			b.join(c)
		case c := <-b.unregister:
			b.detach(c, false)
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

// join admits a client: it tells existing participants the newcomer arrived,
// adds it to the set, and greets it with the current document snapshot and the
// presence roster.
func (b *Board) join(c *Client) {
	b.announceJoin(c) // c is not in the set yet, so it won't receive its own join
	b.clients[c] = struct{}{}
	c.present = true
	b.greet(c)
	b.log.Debug("client joined", "client", c.id, "clients", len(b.clients))
}

// handle processes one inbound message.
//
//   - Shape ops are applied to the document under last-write-wins (the board
//     assigns the authoritative sequence number) and reliably fanned out to
//     every client, including the origin.
//   - Cursor moves are stamped with the origin's id and relayed best-effort to
//     the other clients: ephemeral, so dropped rather than buffered under
//     backpressure.
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
		b.deliverReliable(out, nil)

	case protocol.TypeCursor:
		var cur protocol.Cursor
		if err := in.env.DecodePayload(&cur); err != nil {
			b.log.Warn("dropping invalid cursor", "err", err)
			return
		}
		cur.ClientID = in.origin.id // stamp authoritative id; clients can't spoof
		out, err := protocol.Marshal(protocol.TypeCursor, 0, cur)
		if err != nil {
			return
		}
		b.deliverBestEffort(out, in.origin)

	default:
		b.log.Warn("ignoring unknown message type", "type", in.env.Type)
	}
}

// greet sends a newly joined client the current document snapshot followed by
// the presence roster (itself plus everyone else already here).
func (b *Board) greet(c *Client) {
	snap, err := protocol.Marshal(protocol.TypeSnapshot, 0, b.doc.snapshot())
	if err != nil {
		b.log.Error("failed to encode snapshot", "err", err)
	} else if !b.push(c, snap) {
		return // c was kicked while onboarding; do not touch its closed channel
	}

	others := make([]protocol.Presence, 0, len(b.clients))
	for o := range b.clients {
		if o == c {
			continue
		}
		others = append(others, o.presence())
	}
	state, err := protocol.Marshal(protocol.TypePresenceState, 0, protocol.PresenceState{
		Self:   c.presence(),
		Others: others,
	})
	if err != nil {
		b.log.Error("failed to encode presence state", "err", err)
		return
	}
	b.push(c, state)
}

func (b *Board) announceJoin(c *Client) {
	data, err := protocol.Marshal(protocol.TypePresenceJoin, 0, c.presence())
	if err != nil {
		b.log.Error("failed to encode presence join", "err", err)
		return
	}
	b.deliverReliable(data, nil)
}

func (b *Board) announceLeave(c *Client) {
	data, err := protocol.Marshal(protocol.TypePresenceLeave, 0, protocol.Presence{ClientID: c.id})
	if err != nil {
		b.log.Error("failed to encode presence leave", "err", err)
		return
	}
	b.deliverReliable(data, nil)
}

// deliverReliable sends data to every client except exclude (nil means all),
// guaranteeing delivery for clients that keep up. A client whose buffered
// channel is full is collected and kicked AFTER the loop completes — never
// during iteration — so the follow-on presence.leave fan-out cannot recurse
// into a map we are still ranging.
//
// For a C# developer: the select-with-default is Channel<T>.Writer.TryWrite — a
// non-blocking offer. Reliable messages (shape ops, presence) disconnect a
// client that cannot keep up so it reconnects and resyncs.
func (b *Board) deliverReliable(data []byte, exclude *Client) {
	var slow []*Client
	for c := range b.clients {
		if c == exclude {
			continue
		}
		select {
		case c.send <- data:
		default:
			slow = append(slow, c)
		}
	}
	for _, c := range slow {
		b.log.Warn("kicking slow client: send buffer full", "client", c.id)
		b.detach(c, true)
	}
}

// deliverBestEffort sends data to every client except exclude, dropping the
// message for any client whose buffer is full. Used for cursor updates: a
// stale cursor position is worthless, so it is never worth buffering or kicking
// a client over.
func (b *Board) deliverBestEffort(data []byte, exclude *Client) {
	for c := range b.clients {
		if c == exclude {
			continue
		}
		select {
		case c.send <- data:
		default:
			// drop: the next cursor update supersedes this one
		}
	}
}

// push delivers one reliable message to a single client, kicking it if its
// buffer is full. Returns false if the client was kicked.
func (b *Board) push(c *Client, data []byte) bool {
	select {
	case c.send <- data:
		return true
	default:
		b.log.Warn("kicking slow client during onboarding", "client", c.id)
		b.detach(c, true)
		return false
	}
}

// detach removes a client from the board: it stops fan-out to the client,
// closes its send channel, optionally closes its connection (to unblock the
// read pump when the board initiated the removal), and announces its departure
// to the remaining clients. The present flag makes this idempotent, so a client
// that is kicked and then unregistered is announced exactly once.
//
// Because every close of c.send happens here, in the single run() goroutine,
// and is guarded by the present flag, a send channel is never closed twice.
func (b *Board) detach(c *Client, closeConn bool) {
	if !c.present {
		return
	}
	c.present = false
	delete(b.clients, c)
	close(c.send)
	if closeConn {
		_ = c.conn.Close() // unblocks the client's read pump
	}
	b.announceLeave(c)
}

// shutdown drains every remaining client when the board stops. No leave
// notifications are sent: the board itself is going away.
func (b *Board) shutdown() {
	for c := range b.clients {
		c.present = false
		delete(b.clients, c)
		close(c.send)
		_ = c.conn.Close()
	}
	b.log.Debug("board stopped")
}
