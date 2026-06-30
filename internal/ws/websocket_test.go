package ws

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/Piyush091201/whiteboard/internal/hub"
)

// TestWebSocketEndToEnd exercises the real transport path: two browsers dial the
// HTTP handler, get upgraded to WebSocket by coder/websocket, and a message from
// one is fanned out to the other through the hub.
func TestWebSocketEndToEnd(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := hub.New(logger)

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
	defer a.Close(websocket.StatusNormalClosure, "")

	b, _, err := websocket.Dial(ctx, wsURL+"/ws/board1", nil)
	if err != nil {
		t.Fatalf("dial b: %v", err)
	}
	defer b.Close(websocket.StatusNormalClosure, "")

	// We have no synchronous hook for "both clients registered", so a resends
	// periodically until b receives one. The periodic resend guarantees at least
	// one message is broadcast after b has joined the board's fan-out set.
	send, cancelSend := context.WithCancel(ctx)
	defer cancelSend()
	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			if err := a.Write(send, websocket.MessageText, []byte("hi")); err != nil {
				return
			}
			select {
			case <-ticker.C:
			case <-send.Done():
				return
			}
		}
	}()

	_, data, err := b.Read(ctx)
	if err != nil {
		t.Fatalf("b read: %v", err)
	}
	if string(data) != "hi" {
		t.Fatalf("b received %q, want %q", data, "hi")
	}
}
