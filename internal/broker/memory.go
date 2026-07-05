package broker

import (
	"context"
	"encoding/json"
	"sort"
	"sync"

	"github.com/Piyush091201/whiteboard/internal/protocol"
)

// Memory is an in-process Broker: it keeps board state in maps and delivers
// published messages to in-process subscribers. It is the single-instance
// implementation and the one used by unit tests — no Redis required. Two hubs
// sharing one *Memory behave like two instances sharing a bus.
type Memory struct {
	mu     sync.Mutex
	boards map[string]*memBoard
}

type memBoard struct {
	seq      uint64
	shapes   map[string]memShape
	presence map[string][]byte
	subs     map[*memSub]struct{}
}

type memShape struct {
	seq   uint64
	shape []byte
}

type memSub struct {
	ch chan []byte
}

// NewMemory returns an empty in-process broker.
func NewMemory() *Memory {
	return &Memory{boards: make(map[string]*memBoard)}
}

// board returns the board's state, creating it on first use. Caller holds m.mu.
func (m *Memory) board(id string) *memBoard {
	b := m.boards[id]
	if b == nil {
		b = &memBoard{
			shapes:   make(map[string]memShape),
			presence: make(map[string][]byte),
			subs:     make(map[*memSub]struct{}),
		}
		m.boards[id] = b
	}
	return b
}

// ApplyShape assigns the next sequence and updates the store atomically under
// the lock, so sequence order equals store order (see the package doc).
func (m *Memory) ApplyShape(_ context.Context, boardID, shapeID string, shape []byte, del bool) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	b := m.board(boardID)
	b.seq++
	if del {
		delete(b.shapes, shapeID)
	} else {
		b.shapes[shapeID] = memShape{seq: b.seq, shape: append([]byte(nil), shape...)}
	}
	return b.seq, nil
}

// Publish delivers message to every current subscriber of the board. Delivery
// is non-blocking: a subscriber whose buffer is full drops the message, exactly
// like lossy Redis pub/sub. This is safe because the shape store is
// authoritative — a dropped live message costs a client only a brief staleness
// until its next op or reconnect.
func (m *Memory) Publish(_ context.Context, boardID string, message []byte) error {
	m.mu.Lock()
	b := m.board(boardID)
	subs := make([]*memSub, 0, len(b.subs))
	for s := range b.subs {
		subs = append(subs, s)
	}
	m.mu.Unlock()

	for _, s := range subs {
		select {
		case s.ch <- message:
		default:
		}
	}
	return nil
}

// Subscribe registers an in-process subscriber and returns its channel. The
// subscriber is removed when ctx is cancelled. The channel is never closed
// (removal is enough to stop delivery), which avoids racing a Publish against a
// channel close.
func (m *Memory) Subscribe(ctx context.Context, boardID string) (<-chan []byte, error) {
	sub := &memSub{ch: make(chan []byte, 256)}

	m.mu.Lock()
	m.board(boardID).subs[sub] = struct{}{}
	m.mu.Unlock()

	go func() {
		<-ctx.Done()
		m.mu.Lock()
		if b := m.boards[boardID]; b != nil {
			delete(b.subs, sub)
		}
		m.mu.Unlock()
	}()

	return sub.ch, nil
}

// Snapshot returns the board's shapes ordered by sequence.
func (m *Memory) Snapshot(_ context.Context, boardID string) (protocol.Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	b := m.board(boardID)
	shapes := make([]protocol.SnapshotShape, 0, len(b.shapes))
	for id, s := range b.shapes {
		shapes = append(shapes, protocol.SnapshotShape{
			Seq:   s.seq,
			ID:    id,
			Shape: append([]byte(nil), s.shape...),
		})
	}
	sort.Slice(shapes, func(i, j int) bool { return shapes[i].Seq < shapes[j].Seq })
	return protocol.Snapshot{Seq: b.seq, Shapes: shapes}, nil
}

// Hydrate loads a snapshot into the board only if it is currently empty.
func (m *Memory) Hydrate(_ context.Context, boardID string, snap protocol.Snapshot) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	b := m.board(boardID)
	if b.seq != 0 || len(b.shapes) > 0 {
		return false, nil // already has state
	}
	b.seq = snap.Seq
	for _, s := range snap.Shapes {
		b.shapes[s.ID] = memShape{seq: s.Seq, shape: append([]byte(nil), s.Shape...)}
	}
	return true, nil
}

// SetPresence adds or updates a participant in the roster.
func (m *Memory) SetPresence(_ context.Context, boardID, clientID string, presence []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.board(boardID).presence[clientID] = append([]byte(nil), presence...)
	return nil
}

// RemovePresence removes a participant from the roster.
func (m *Memory) RemovePresence(_ context.Context, boardID, clientID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.board(boardID).presence, clientID)
	return nil
}

// Presence returns the roster ordered by client id (stable output).
func (m *Memory) Presence(_ context.Context, boardID string) ([]protocol.Presence, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	b := m.board(boardID)
	out := make([]protocol.Presence, 0, len(b.presence))
	for _, raw := range b.presence {
		var p protocol.Presence
		if err := json.Unmarshal(raw, &p); err != nil {
			continue
		}
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ClientID < out[j].ClientID })
	return out, nil
}

// Close is a no-op for the in-process broker.
func (m *Memory) Close() error { return nil }
