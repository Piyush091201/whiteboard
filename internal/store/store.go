// Package store is the durable system of record for board state. It persists a
// whole-board snapshot per board so that boards survive a Redis restart; the
// broker (Redis) remains the hot tier on the message path.
//
// See docs/adr/0004-persistence-postgres-snapshots.md.
package store

import (
	"context"

	"github.com/Piyush091201/whiteboard/internal/protocol"
)

// Store persists and loads board snapshots. Implementations must be safe for
// concurrent use.
type Store interface {
	// LoadSnapshot returns the persisted snapshot for a board. The bool is false
	// (with a zero snapshot and nil error) when no snapshot exists yet.
	LoadSnapshot(ctx context.Context, boardID string) (protocol.Snapshot, bool, error)

	// SaveSnapshot persists a board snapshot. It must not overwrite a stored
	// snapshot that has a higher sequence number (a stale write from a lagging
	// instance is a no-op).
	SaveSnapshot(ctx context.Context, boardID string, snap protocol.Snapshot) error

	// Close releases resources held by the store.
	Close() error
}
