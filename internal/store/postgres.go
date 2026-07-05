package store

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Piyush091201/whiteboard/internal/protocol"
)

// schema is applied on startup. One JSONB row per board (ADR 0004).
const schema = `
CREATE TABLE IF NOT EXISTS board_snapshots (
	board_id   TEXT PRIMARY KEY,
	seq        BIGINT NOT NULL,
	shapes     JSONB NOT NULL,
	updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
)`

// Postgres is the durable Store backed by a Postgres database via pgx.
type Postgres struct {
	pool *pgxpool.Pool
}

// NewPostgres connects to Postgres using the given DSN and ensures the schema
// exists. The caller owns the returned store and must Close it.
func NewPostgres(ctx context.Context, dsn string) (*Postgres, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if _, err := pool.Exec(ctx, schema); err != nil {
		pool.Close()
		return nil, err
	}
	return &Postgres{pool: pool}, nil
}

func (p *Postgres) LoadSnapshot(ctx context.Context, boardID string) (protocol.Snapshot, bool, error) {
	var seq int64
	var shapesJSON []byte
	err := p.pool.QueryRow(ctx,
		`SELECT seq, shapes FROM board_snapshots WHERE board_id = $1`, boardID,
	).Scan(&seq, &shapesJSON)
	if errors.Is(err, pgx.ErrNoRows) {
		return protocol.Snapshot{}, false, nil
	}
	if err != nil {
		return protocol.Snapshot{}, false, err
	}

	var shapes []protocol.SnapshotShape
	if err := json.Unmarshal(shapesJSON, &shapes); err != nil {
		return protocol.Snapshot{}, false, err
	}
	return protocol.Snapshot{Seq: uint64(seq), Shapes: shapes}, true, nil
}

func (p *Postgres) SaveSnapshot(ctx context.Context, boardID string, snap protocol.Snapshot) error {
	shapesJSON, err := json.Marshal(snap.Shapes)
	if err != nil {
		return err
	}
	// The seq guard makes a stale write (from a lagging instance) a no-op.
	_, err = p.pool.Exec(ctx, `
		INSERT INTO board_snapshots (board_id, seq, shapes, updated_at)
		VALUES ($1, $2, $3, now())
		ON CONFLICT (board_id) DO UPDATE
		SET seq = EXCLUDED.seq, shapes = EXCLUDED.shapes, updated_at = now()
		WHERE EXCLUDED.seq >= board_snapshots.seq`,
		boardID, int64(snap.Seq), shapesJSON,
	)
	return err
}

func (p *Postgres) Close() error {
	p.pool.Close()
	return nil
}
