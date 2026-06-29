// Package ws contains the HTTP-to-WebSocket upgrade glue that connects an
// incoming connection to a hub Board.
//
// Empty at the scaffold stage. Phase 1 lands the coder/websocket Accept handler
// and the construction of a Client (read pump + write pump) bound to the
// requested board.
//
// We chose coder/websocket over gorilla/websocket for its context-first API:
// conn.Read(ctx)/conn.Write(ctx) map cleanly onto the same cancellation model
// used throughout the server (see docs/adr/0001-websocket-library.md).
package ws
