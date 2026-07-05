package hub

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/Piyush091201/whiteboard/internal/broker"
	"github.com/Piyush091201/whiteboard/internal/protocol"
)

// clientMsg carries a message the connection goroutine wants delivered to one
// specific client (its snapshot, its presence roster). Routing it through the
// board keeps the board goroutine the sole writer of a client's send channel.
type clientMsg struct {
	c    *Client
	data []byte
}

// Board is the per-board actor. It owns only the set of locally-connected
// clients; authoritative state (shapes, sequence, presence roster) lives in the
// broker, shared across instances. The board's job is to relay the broker's
// per-board stream to its local clients.
//
// For a C# developer: still the actor model, but the board is now a thin
// per-instance projection of shared state rather than the owner of it.
type Board struct {
	id      string
	log     *slog.Logger
	broker  broker.Broker
	persist *persistence // nil when persistence is disabled
	metrics Metrics

	register   chan *Client
	unregister chan *Client
	direct     chan clientMsg // deliver a message to one specific client
	inspect    chan chan int  // request the current client count
	quit       chan struct{}  // closed by the hub when the board should stop
	done       chan struct{}  // closed when run() has returned

	clients map[*Client]struct{}
	byID    map[string]*Client // id -> client, for excluding a cursor's origin
}

func newBoard(id string, log *slog.Logger, b broker.Broker, p *persistence, m Metrics) *Board {
	return &Board{
		id:         id,
		log:        log.With("board", id),
		broker:     b,
		persist:    p,
		metrics:    m,
		register:   make(chan *Client),
		unregister: make(chan *Client),
		direct:     make(chan clientMsg),
		inspect:    make(chan chan int),
		quit:       make(chan struct{}),
		done:       make(chan struct{}),
		clients:    make(map[*Client]struct{}),
		byID:       make(map[string]*Client),
	}
}

// run is the board's event loop. It subscribes to the broker's per-board channel
// and dispatches every published message to the local clients — the single
// delivery path, so a message from any instance (including this one, on
// loop-back) reaches everyone the same way. It exits when the hub closes b.quit.
func (b *Board) run() {
	defer close(b.done)

	// Cold-start hydration happens before the select loop, so the first client's
	// register (which blocks until we reach the loop) is served only after the
	// board's durable state has been loaded.
	b.hydrate()

	subCtx, cancelSub := context.WithCancel(context.Background())
	defer cancelSub()

	msgs, err := b.broker.Subscribe(subCtx, b.id)
	if err != nil {
		b.log.Error("broker subscribe failed; board runs without fan-out", "err", err)
		msgs = nil
	}

	for {
		select {
		case c := <-b.register:
			b.join(c)
		case c := <-b.unregister:
			b.detach(c, false)
		case msg, ok := <-msgs:
			if !ok {
				msgs = nil // subscription ended; stop selecting it
				continue
			}
			b.dispatch(msg)
		case d := <-b.direct:
			if d.c.present {
				b.push(d.c, d.data)
			}
		case ch := <-b.inspect:
			ch <- len(b.clients)
		case <-b.quit:
			b.shutdown()
			b.persistOnClose() // final durable checkpoint before the board is gone
			return
		}
	}
}

// hydrate loads durable state on cold start (no-op when persistence is off).
func (b *Board) hydrate() {
	if b.persist == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), persistTimeout)
	defer cancel()
	b.persist.hydrate(ctx, b.id)
}

// persistOnClose saves a final snapshot when the board's last client leaves.
func (b *Board) persistOnClose() {
	if b.persist == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), persistTimeout)
	defer cancel()
	b.persist.save(ctx, b.id)
}

// join adds a client to the local fan-out set. Presence is announced by the
// connection goroutine through the broker, not here.
func (b *Board) join(c *Client) {
	b.clients[c] = struct{}{}
	b.byID[c.id] = c
	c.present = true
	b.log.Debug("client joined", "client", c.id, "clients", len(b.clients))
}

// dispatch routes a message received from the broker to local clients by type:
// cursor updates are best-effort and skip the origin (if it is on this
// instance); everything else (shape ops, presence events) is reliable to all.
func (b *Board) dispatch(msg []byte) {
	var env protocol.Envelope
	if err := json.Unmarshal(msg, &env); err != nil {
		b.deliverReliable(msg, nil) // unknown framing: deliver rather than drop
		return
	}
	if env.Type == protocol.TypeCursor {
		var cur protocol.Cursor
		_ = env.DecodePayload(&cur)
		b.deliverBestEffort(msg, b.byID[cur.ClientID]) // exclude local origin if present
		return
	}
	b.deliverReliable(msg, nil)
}

// deliverReliable sends data to every client except exclude (nil means all),
// guaranteeing delivery for clients that keep up. A client whose buffer is full
// is collected and kicked AFTER the loop completes — never during iteration.
func (b *Board) deliverReliable(data []byte, exclude *Client) {
	delivered := 0
	var slow []*Client
	for c := range b.clients {
		if c == exclude {
			continue
		}
		select {
		case c.send <- data:
			delivered++
		default:
			slow = append(slow, c)
		}
	}
	b.metrics.MessagesDelivered(delivered)
	for _, c := range slow {
		b.log.Warn("kicking slow client: send buffer full", "client", c.id)
		b.metrics.ClientKicked()
		b.detach(c, true)
	}
}

// deliverBestEffort sends data to every client except exclude, dropping the
// message for any client whose buffer is full. Used for cursor updates: a stale
// cursor position is worthless, so it is never worth buffering or kicking over.
func (b *Board) deliverBestEffort(data []byte, exclude *Client) {
	delivered := 0
	for c := range b.clients {
		if c == exclude {
			continue
		}
		select {
		case c.send <- data:
			delivered++
		default:
		}
	}
	b.metrics.MessagesDelivered(delivered)
}

// push delivers one reliable message to a single client, kicking it if its
// buffer is full. Returns false if the client was kicked.
func (b *Board) push(c *Client, data []byte) bool {
	select {
	case c.send <- data:
		b.metrics.MessagesDelivered(1)
		return true
	default:
		b.log.Warn("kicking slow client during onboarding", "client", c.id)
		b.metrics.ClientKicked()
		b.detach(c, true)
		return false
	}
}

// detach removes a client from the board and closes its send channel, optionally
// closing its connection to unblock the read pump. The present flag makes this
// idempotent (kicked, then unregistered). Departure is announced by the
// connection goroutine through the broker, not here.
func (b *Board) detach(c *Client, closeConn bool) {
	if !c.present {
		return
	}
	c.present = false
	delete(b.clients, c)
	delete(b.byID, c.id)
	close(c.send)
	if closeConn {
		_ = c.conn.Close()
	}
}

// shutdown drains every remaining client when the board stops.
func (b *Board) shutdown() {
	for c := range b.clients {
		c.present = false
		delete(b.clients, c)
		delete(b.byID, c.id)
		close(c.send)
		_ = c.conn.Close()
	}
	b.log.Debug("board stopped")
}
