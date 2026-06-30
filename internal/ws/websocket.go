// Package ws contains the HTTP-to-WebSocket upgrade glue that connects an
// incoming connection to the hub. It adapts *coder/websocket.Conn to the small
// hub.Conn interface so the hub stays transport-agnostic and testable.
//
// coder/websocket was chosen over gorilla/websocket for its context-first API
// (see docs/adr/0001-websocket-library.md).
package ws

import (
	"context"
	"net/http"

	"github.com/coder/websocket"

	"github.com/Piyush091201/whiteboard/internal/hub"
)

// maxMessageBytes caps a single inbound message. Generous enough for board
// snapshots; prevents a client from forcing unbounded allocation.
const maxMessageBytes = 1 << 20 // 1 MiB

// Handler returns an http.Handler that upgrades a request to a WebSocket and
// hands the connection to the hub for the board named by the "board" path
// value (register the route as e.g. "GET /ws/{board}"). It blocks for the life
// of the connection, which is what the net/http server expects.
func Handler(h *hub.Hub) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		boardID := r.PathValue("board")
		if boardID == "" {
			http.Error(w, "missing board id", http.StatusBadRequest)
			return
		}

		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			// Origin checking is disabled for local development. Lock this
			// down (OriginPatterns) before any real deployment.
			InsecureSkipVerify: true,
		})
		if err != nil {
			return // Accept already wrote the error response
		}
		c.SetReadLimit(maxMessageBytes)

		// Serve blocks until the connection ends. Use the request context so the
		// connection is cancelled if the server tears the request down.
		h.Serve(r.Context(), boardID, &conn{ws: c})

		// Best-effort close; the connection is usually already gone.
		_ = c.CloseNow()
	})
}

// conn adapts *websocket.Conn to hub.Conn. The whiteboard protocol is
// text/JSON, so the message type is fixed and the read message type is ignored.
type conn struct {
	ws *websocket.Conn
}

func (c *conn) Read(ctx context.Context) ([]byte, error) {
	_, data, err := c.ws.Read(ctx)
	return data, err
}

func (c *conn) Write(ctx context.Context, data []byte) error {
	return c.ws.Write(ctx, websocket.MessageText, data)
}

func (c *conn) Ping(ctx context.Context) error {
	return c.ws.Ping(ctx)
}

func (c *conn) Close() error {
	return c.ws.CloseNow()
}
