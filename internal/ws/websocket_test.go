package ws

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/Piyush091201/whiteboard/internal/broker"
	"github.com/Piyush091201/whiteboard/internal/hub"
	"github.com/Piyush091201/whiteboard/internal/protocol"
)

// TestWebSocketEndToEnd exercises the real transport path: two browsers dial the
// HTTP handler, get upgraded to WebSocket by coder/websocket, and a shape
// operation from one is sequenced by the hub and fanned out to the other.
func TestWebSocketEndToEnd(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := hub.New(logger, broker.NewMemory(), nil)

	mux := http.NewServeMux()
	mux.Handle("GET /ws/{board}", Handler(h))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	a, _, err := websocket.Dial(ctx, wsURL+"/ws/board1", nil)
	if err != nil {
		t.Fatalf("dial a: %v", err)
	}
	defer func() { _ = a.Close(websocket.StatusNormalClosure, "") }()

	b, _, err := websocket.Dial(ctx, wsURL+"/ws/board1", nil)
	if err != nil {
		t.Fatalf("dial b: %v", err)
	}
	defer func() { _ = b.Close(websocket.StatusNormalClosure, "") }()

	op, err := protocol.Marshal(protocol.TypeShapeCreate, 0, protocol.ShapeOp{
		ID:    "s1",
		Shape: json.RawMessage(`{"kind":"rect"}`),
	})
	if err != nil {
		t.Fatalf("build op: %v", err)
	}

	// We have no synchronous hook for "both clients registered", so a resends
	// periodically until b observes the broadcast (b joins, gets a snapshot,
	// then the sequenced op once it is in the fan-out set).
	send, cancelSend := context.WithCancel(ctx)
	defer cancelSend()
	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			if err := a.Write(send, websocket.MessageText, op); err != nil {
				return
			}
			select {
			case <-ticker.C:
			case <-send.Done():
				return
			}
		}
	}()

	// Read until we see the shape.create (the first message is b's snapshot).
	for {
		_, data, err := b.Read(ctx)
		if err != nil {
			t.Fatalf("b read: %v", err)
		}
		var env protocol.Envelope
		if err := json.Unmarshal(data, &env); err != nil {
			t.Fatalf("b decode: %v", err)
		}
		if env.Type != protocol.TypeShapeCreate {
			continue // skip snapshot / other frames
		}
		if env.Seq == 0 {
			t.Errorf("shape.create seq = 0, want a server-assigned sequence")
		}
		var got protocol.ShapeOp
		if err := env.DecodePayload(&got); err != nil || got.ID != "s1" {
			t.Fatalf("b op = %+v err=%v, want id s1", got, err)
		}
		return
	}
}
