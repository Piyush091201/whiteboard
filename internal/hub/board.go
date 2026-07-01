package hub

import (
	"context"
	"log/slog"

	"github.com/Piyush091201/whiteboard/internal/broker"
	"github.com/Piyush091201/whiteboard/internal/protocol"
)

// inbound is a client message the board handles locally (rather than through the
// broker) — in this phase, cursor moves. Shape ops go straight to the broker
// from the read pump.
type inbound struct {
	origin *Client
	env    protocol.Envelope
}

// snapshotReq carries a snapshot the connection goroutine fetched from the
// broker, for the board to deliver. Routing it through the board keeps the board
// goroutine the sole writer of a client's send channel.
type snapshotReq struct {
	c    *Client
	data []byte
}

// Board is the per-board actor. All access to its client set happens inside the
// single run() goroutine via the channels below, so the set needs no mutex.
// Authoritative shape state now lives in the broker (shared across instances),
// not in the board.
//
// For a C# developer: this is still the actor model, but the aggregate it owned
// (the document) has moved to the broker so multiple instances can share it —
// the board now owns only its local connections and relays the broker's stream.
type Board struct {
	id     string
	log    *slog.Logger
	broker broker.Broker

	register   chan *Client
	unregister chan *Client
	inbound    chan inbound     // local messages (cursors)
	snapshots  chan snapshotReq // snapshots fetched off-goroutine, to deliver
	inspect    chan chan int    // request the current client count
	quit       chan struct{}    // closed by the hub when the board should stop
	done       chan struct{}    // closed when run() has returned

	clients map[*Client]struct{}
}

func newBoard(id string, log *slog.Logger, b broker.Broker) *Board {
	return &Board{
		id:         id,
		log:        log.With("board", id),
		broker:     b,
		register:   make(chan *Client),
		unregister: make(chan *Client),
		inbound:    make(chan inbound),
		snapshots:  make(chan snapshotReq),
		inspect:    make(chan chan int),
		quit:       make(chan struct{}),
		done:       make(chan struct{}),
		clients:    make(map[*Client]struct{}),
	}
}

// run is the board's event loop. It subscribes to the broker's per-board channel
// and delivers every published message to the local clients — the single
// delivery path, so an op from any instance (including this one, on loop-back)
// reaches everyone exactly the same way. It exits when the hub closes b.quit.
func (b *Board) run() {
	defer close(b.done)

	// Subscription lifetime is tied to the board goroutine's lifetime.
	subCtx, cancelSub := context.WithCancel(context.Background())
	defer cancelSub()

	msgs, err := b.broker.Subscribe(subCtx, b.id)
	if err != nil {
		b.log.Error("broker subscribe failed; board runs without fan-out", "err", err)
		msgs = nil // selecting a nil channel blocks forever, which is fine
	}

	for {
		select {
		case c := <-b.register:
			b.join(c)
		case c := <-b.unregister:
			b.detach(c, false)
		case in := <-b.inbound:
			b.handle(in)
		case msg, ok := <-msgs:
			if !ok {
				msgs = nil // subscription ended; stop selecting it
				continue
			}
			b.deliverReliable(msg, nil) // loop-back fan-out to local clients
		case s := <-b.snapshots:
			if s.c.present {
				b.push(s.c, s.data)
			}
		case ch := <-b.inspect:
			ch <- len(b.clients)
		case <-b.quit:
			b.shutdown()
			return
		}
	}
}

// join admits a client: it tells existing local participants the newcomer
// arrived, adds it to the set, and sends the presence roster. The document
// snapshot is delivered separately (fetched from the broker off this goroutine).
func (b *Board) join(c *Client) {
	b.announceJoin(c)
	b.clients[c] = struct{}{}
	c.present = true
	b.sendPresenceState(c)
	b.log.Debug("client joined", "client", c.id, "clients", len(b.clients))
}

// handle processes a locally-handled client message. In this phase that is
// cursor moves: stamped with the origin id and relayed best-effort to the other
// local clients (dropped, never buffered, under backpressure).
func (b *Board) handle(in inbound) {
	switch in.env.Type {
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
		b.log.Warn("ignoring unexpected message type", "type", in.env.Type)
	}
}

// sendPresenceState sends a joining client the local presence roster (itself
// plus the other clients on this instance).
func (b *Board) sendPresenceState(c *Client) {
	others := make([]protocol.Presence, 0, len(b.clients))
	for o := range b.clients {
		if o == c {
			continue
		}
		others = append(others, o.presence())
	}
	data, err := protocol.Marshal(protocol.TypePresenceState, 0, protocol.PresenceState{
		Self:   c.presence(),
		Others: others,
	})
	if err != nil {
		b.log.Error("failed to encode presence state", "err", err)
		return
	}
	b.push(c, data)
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
// guaranteeing delivery for clients that keep up. A client whose buffer is full
// is collected and kicked AFTER the loop completes — never during iteration — so
// the follow-on presence.leave fan-out cannot recurse into a map we are still
// ranging.
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
// message for any client whose buffer is full. Used for cursor updates: a stale
// cursor position is worthless, so it is never worth buffering or kicking over.
func (b *Board) deliverBestEffort(data []byte, exclude *Client) {
	for c := range b.clients {
		if c == exclude {
			continue
		}
		select {
		case c.send <- data:
		default:
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

// detach removes a client from the board: it stops fan-out to the client, closes
// its send channel, optionally closes its connection (to unblock the read pump
// when the board initiated the removal), and announces its departure to the
// remaining clients. The present flag makes this idempotent, so a client that is
// kicked and then unregistered is announced exactly once.
func (b *Board) detach(c *Client, closeConn bool) {
	if !c.present {
		return
	}
	c.present = false
	delete(b.clients, c)
	close(c.send)
	if closeConn {
		_ = c.conn.Close()
	}
	b.announceLeave(c)
}

// shutdown drains every remaining client when the board stops.
func (b *Board) shutdown() {
	for c := range b.clients {
		c.present = false
		delete(b.clients, c)
		close(c.send)
		_ = c.conn.Close()
	}
	b.log.Debug("board stopped")
}
