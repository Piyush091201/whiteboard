package hub

import (
	"context"
	"log/slog"
	"time"

	"github.com/Piyush091201/whiteboard/internal/broker"
	"github.com/Piyush091201/whiteboard/internal/store"
)

const (
	// snapshotInterval is how often the coordinator persists active boards.
	snapshotInterval = 30 * time.Second
	// persistTimeout bounds a single hydrate/save round of broker + store I/O.
	persistTimeout = 5 * time.Second
)

// persistence connects the hot broker to the durable store: it hydrates a board
// from the store on cold start and saves board snapshots back to the store. It
// is nil when persistence is disabled (no store configured).
type persistence struct {
	broker broker.Broker
	store  store.Store
	log    *slog.Logger
}

// hydrate loads a board's last durable snapshot into the broker if the broker
// has no state for it (idempotent across instances via Broker.Hydrate).
func (p *persistence) hydrate(ctx context.Context, boardID string) {
	snap, ok, err := p.store.LoadSnapshot(ctx, boardID)
	if err != nil {
		p.log.Error("load snapshot failed", "board", boardID, "err", err)
		return
	}
	if !ok {
		return
	}
	if loaded, err := p.broker.Hydrate(ctx, boardID, snap); err != nil {
		p.log.Error("hydrate failed", "board", boardID, "err", err)
	} else if loaded {
		p.log.Info("hydrated board from durable snapshot", "board", boardID, "seq", snap.Seq)
	}
}

// save reads the board's current snapshot from the broker and persists it.
func (p *persistence) save(ctx context.Context, boardID string) {
	snap, err := p.broker.Snapshot(ctx, boardID)
	if err != nil {
		p.log.Error("snapshot for persist failed", "board", boardID, "err", err)
		return
	}
	if err := p.store.SaveSnapshot(ctx, boardID, snap); err != nil {
		p.log.Error("save snapshot failed", "board", boardID, "err", err)
	}
}
