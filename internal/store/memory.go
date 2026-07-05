package store

import (
	"context"
	"sync"

	"github.com/Piyush091201/whiteboard/internal/protocol"
)

// Memory is an in-process Store used by unit tests and to exercise the
// persistence coordinator without a database. It applies the same seq guard as
// the real store.
type Memory struct {
	mu    sync.Mutex
	snaps map[string]protocol.Snapshot
}

// NewMemory returns an empty in-process store.
func NewMemory() *Memory {
	return &Memory{snaps: make(map[string]protocol.Snapshot)}
}

func (m *Memory) LoadSnapshot(_ context.Context, boardID string) (protocol.Snapshot, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	snap, ok := m.snaps[boardID]
	return snap, ok, nil
}

func (m *Memory) SaveSnapshot(_ context.Context, boardID string, snap protocol.Snapshot) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if cur, ok := m.snaps[boardID]; ok && snap.Seq < cur.Seq {
		return nil // stale write: keep the newer snapshot
	}
	m.snaps[boardID] = snap
	return nil
}

func (m *Memory) Close() error { return nil }
