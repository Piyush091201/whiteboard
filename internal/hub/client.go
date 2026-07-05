package hub

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"hash/fnv"
	"log/slog"
	"sync"
	"time"

	"github.com/Piyush091201/whiteboard/internal/protocol"
)

const (
	// sendBuffer is how many messages may queue for a client before it is
	// considered too slow and kicked. Sized to absorb normal bursts.
	sendBuffer = 256
	// pingInterval / pingTimeout drive heartbeats: a connection that fails to
	// answer a ping within the timeout is treated as dead and torn down.
	pingInterval = 30 * time.Second
	pingTimeout  = 10 * time.Second
	// writeTimeout bounds a single outbound write so a stuck socket can't pin a
	// write pump forever.
	writeTimeout = 10 * time.Second
)

// Conn is the minimal transport the hub needs from a WebSocket connection.
// *websocket.Conn is adapted to this in package ws; tests use an in-memory fake.
//
// For a C# developer: in Go an interface is satisfied structurally — the
// implementer never declares "implements Conn". Defining a small interface at
// the point of use (here, in the consumer) rather than alongside the
// implementation is idiomatic. Each method takes a context.Context, the
// analog of threading a CancellationToken through every async call.
type Conn interface {
	Read(ctx context.Context) ([]byte, error)
	Write(ctx context.Context, data []byte) error
	Ping(ctx context.Context) error
	Close() error
}

// ClientInfo is the optional identity a client supplies when joining a board
// (e.g. via WebSocket query parameters). Missing fields are filled with
// defaults.
type ClientInfo struct {
	Name  string
	Color string
}

// Client is one connected participant: a transport, an identity, and a buffered
// outbound queue. The queue is the backpressure boundary — see Board.deliver*.
type Client struct {
	id      string
	boardID string
	name    string
	color   string
	board   *Board
	conn    Conn
	send    chan []byte
	log     *slog.Logger

	// present is owned by the board goroutine. It guards against announcing a
	// client's departure more than once (e.g. kicked, then unregistered).
	present bool
}

// presence describes the client for presence messages.
func (c *Client) presence() protocol.Presence {
	return protocol.Presence{ClientID: c.id, Name: c.name, Color: c.color}
}

// palette provides distinct, stable default cursor colors keyed by client id.
var palette = []string{
	"#e6194b", "#3cb44b", "#4363d8", "#f58231", "#911eb4",
	"#42d4f4", "#f032e6", "#bfef45", "#fabed4", "#469990",
}

func defaultName(name string) string {
	if name == "" {
		return "Anonymous"
	}
	return name
}

func defaultColor(color, id string) string {
	if color != "" {
		return color
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(id))
	return palette[h.Sum32()%uint32(len(palette))]
}

// Serve attaches conn to the board identified by boardID and blocks, pumping
// messages in both directions until the connection ends. On return it
// guarantees that both pump goroutines have exited and the client has been
// released from the hub — there are no leaked goroutines.
//
// For a C# developer: this is the structured-concurrency shape you'd build with
// linked CancellationTokens and Task.WhenAll — here it's a derived context plus
// a WaitGroup. The defers run in LIFO order on every exit path (including
// panics), which is how cleanup is guaranteed without try/finally.
func (h *Hub) Serve(ctx context.Context, boardID string, info ClientInfo, conn Conn) {
	id := newID()
	c := &Client{
		id:      id,
		boardID: boardID,
		name:    defaultName(info.Name),
		color:   defaultColor(info.Color, id),
		conn:    conn,
		send:    make(chan []byte, sendBuffer),
	}
	c.log = h.log.With("client", c.id, "board", boardID)

	// acquire bumps the board's keep-open count under the hub lock, so the
	// board is guaranteed alive for the register send that follows.
	c.board = h.acquire(boardID)
	defer h.release(c)

	c.board.register <- c

	// Fetch the current board snapshot from the broker off the board goroutine
	// (it is network I/O), then hand it to the board to deliver — keeping the
	// board the sole writer of c.send. The client is already registered, so any
	// op published after this point is delivered live; the client reconciles
	// snapshot and live ops by sequence number.
	if snap, err := c.board.broker.Snapshot(ctx, boardID); err != nil {
		c.log.Warn("snapshot fetch failed", "err", err)
	} else if data, err := protocol.Marshal(protocol.TypeSnapshot, 0, snap); err == nil {
		select {
		case c.board.snapshots <- snapshotReq{c: c, data: data}:
		case <-c.board.done:
		case <-ctx.Done():
		}
	}

	// A connection-scoped context so that when one pump stops, the other
	// unwinds promptly.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.writePump(ctx)
	}()

	c.readPump(ctx) // blocks until the connection errors or ctx is cancelled

	cancel()           // signal the write pump to stop
	_ = c.conn.Close() // unblock anything stuck in a read/write
	wg.Wait()          // ensure the write pump is gone before we release
}

// readPump reads inbound messages, parses the envelope, and hands them to the
// board for sequencing and fan-out. A message that fails to parse is logged and
// dropped — one malformed frame should not tear down an otherwise healthy
// connection. readPump returns on any read error (disconnect, dead heartbeat,
// cancelled context), which drives the rest of the teardown.
//
// The cheap, parallelizable JSON parse happens here, in the per-connection
// goroutine; only sequencing and state mutation are serialized through the
// board.
func (c *Client) readPump(ctx context.Context) {
	for {
		data, err := c.conn.Read(ctx)
		if err != nil {
			return
		}
		var env protocol.Envelope
		if err := json.Unmarshal(data, &env); err != nil {
			c.log.Warn("dropping unparseable message", "err", err)
			continue
		}
		switch env.Type {
		case protocol.TypeShapeCreate, protocol.TypeShapeUpdate, protocol.TypeShapeDelete:
			// Shape ops are sequenced and stored via the broker off the board
			// goroutine; the loop-back subscription delivers them to clients.
			c.applyShapeOp(ctx, env)
		default:
			// Locally-handled messages (cursors) go to the board.
			select {
			case c.board.inbound <- inbound{origin: c, env: env}:
			case <-c.board.done: // board is shutting down
				return
			case <-ctx.Done():
				return
			}
		}
	}
}

// applyShapeOp sequences and stores a shape op via the broker, then publishes
// the sequenced message. Delivery to clients (including this one) happens on the
// board's subscription loop-back, so this method never touches client state.
func (c *Client) applyShapeOp(ctx context.Context, env protocol.Envelope) {
	var op protocol.ShapeOp
	if err := env.DecodePayload(&op); err != nil || op.ID == "" {
		c.log.Warn("dropping invalid shape op", "type", env.Type, "err", err)
		return
	}
	del := env.Type == protocol.TypeShapeDelete
	seq, err := c.board.broker.ApplyShape(ctx, c.boardID, op.ID, op.Shape, del)
	if err != nil {
		c.log.Error("broker apply failed", "err", err)
		return
	}
	msg, err := protocol.Marshal(env.Type, seq, op)
	if err != nil {
		return
	}
	if err := c.board.broker.Publish(ctx, c.boardID, msg); err != nil {
		c.log.Error("broker publish failed", "err", err)
	}
}

// writePump drains the client's send queue to the socket and emits periodic
// heartbeats. It returns when the board closes c.send (the client was removed
// or kicked), when a write or ping fails, or when the context is cancelled.
func (c *Client) writePump(ctx context.Context) {
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()

	for {
		select {
		case data, ok := <-c.send:
			if !ok {
				return // board closed our queue: we've been removed
			}
			wctx, cancel := context.WithTimeout(ctx, writeTimeout)
			err := c.conn.Write(wctx, data)
			cancel()
			if err != nil {
				return
			}
		case <-ticker.C:
			pctx, cancel := context.WithTimeout(ctx, pingTimeout)
			err := c.conn.Ping(pctx)
			cancel()
			if err != nil {
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

// newID returns a short random hex identifier for a client.
func newID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
