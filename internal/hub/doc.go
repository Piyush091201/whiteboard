// Package hub is the real-time core of the whiteboard: a hub-and-spoke design
// where each board is an actor goroutine that owns its set of connected clients
// and fans out messages over channels.
//
// This package is intentionally empty at the scaffold stage. Phase 1 lands the
// Board (room actor) run loop, the per-connection Client with its read/write
// pumps, register/unregister choreography, and the non-blocking backpressure
// path — all covered by unit tests run under the race detector and
// goroutine-leak detection.
//
// Design note for a C# developer: rather than guarding a shared
// map[connID]client with a lock, each Board has its own goroutine and all
// mutations are serialized through channels. The board's state is therefore
// touched by exactly one goroutine, so no mutex is needed on the hot path:
// "share memory by communicating."
package hub
