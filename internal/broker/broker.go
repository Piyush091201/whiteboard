// Package broker abstracts the shared board state and message bus so the hub
// works both as a single in-memory instance and across many instances backed by
// Redis.
//
// A Broker owns three things per board:
//
//   - a global sequence counter (so last-write-wins ordering is consistent
//     across instances),
//   - the authoritative shape state (so any instance can serve a correct
//     snapshot), and
//   - a pub/sub channel (so a message published by one instance reaches all).
//
// ApplyShape assigns the next sequence and updates the store atomically, which
// makes sequence order equal to store order — so plain overwrite/delete is
// correct last-write-wins, with no compare-and-set or tombstones needed. See
// docs/adr/0002-redis-fanout-vs-inmemory.md.
package broker

import (
	"context"

	"github.com/Piyush091201/whiteboard/internal/protocol"
)

// Broker is the shared state and message bus behind the hub. Implementations
// must be safe for concurrent use.
type Broker interface {
	// ApplyShape atomically assigns the next global sequence number for the
	// board and applies the shape operation to the authoritative store under
	// that sequence (overwrite for create/update, remove for delete). It
	// returns the assigned sequence. It does not publish; the caller builds the
	// sequenced wire message and calls Publish.
	ApplyShape(ctx context.Context, boardID, shapeID string, shape []byte, del bool) (uint64, error)

	// Publish sends a raw message to the board's channel. Every subscriber
	// (including the publishing instance) receives it.
	Publish(ctx context.Context, boardID string, message []byte) error

	// Subscribe returns a stream of messages published to the board's channel.
	// The stream stops when ctx is cancelled. The returned channel is not
	// closed while ctx is live.
	Subscribe(ctx context.Context, boardID string) (<-chan []byte, error)

	// Snapshot returns the board's current shapes, ordered by sequence.
	Snapshot(ctx context.Context, boardID string) (protocol.Snapshot, error)

	// SetPresence adds or updates a participant in the board's global roster.
	// presence is the encoded protocol.Presence.
	SetPresence(ctx context.Context, boardID, clientID string, presence []byte) error

	// RemovePresence removes a participant from the board's global roster.
	RemovePresence(ctx context.Context, boardID, clientID string) error

	// Presence returns the board's current roster across all instances.
	Presence(ctx context.Context, boardID string) ([]protocol.Presence, error)

	// Close releases resources held by the broker.
	Close() error
}
