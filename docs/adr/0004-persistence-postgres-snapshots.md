# ADR 0004: Durable persistence — Postgres board snapshots

- **Status:** Accepted
- **Date:** 2026-07-02
- **Deciders:** Project owner

## Context

Board state currently lives only in Redis (ADR 0002): fast and shared across
instances, but not durable. A Redis restart or eviction loses every board. We
need durability so boards survive restarts, while keeping Redis as the hot tier
on the message path.

## Decision

Add **Postgres as the durable system of record** and keep **Redis as a cache of
hot state**. A new `Store` interface (pgx v5, `pgxpool`) sits alongside the
broker; a small persistence coordinator in the hub connects them.

- **Schema — one JSONB row per board.**
  `board_snapshots(board_id PK, seq BIGINT, shapes JSONB, updated_at)`. We already
  read a whole-board snapshot from the broker, so a single upsert per snapshot is
  the natural fit. Fits the last-write-wins + snapshot model (ADR 0003).
- **Save trigger — periodic plus on board close.** A coordinator goroutine saves
  active boards on an interval (~30s), and a board saves once more when its last
  local client leaves. This bounds worst-case data loss to the interval and
  guarantees a final checkpoint. Writes never happen on the per-op hot path.
- **Seq-guarded upsert.** `ON CONFLICT (board_id) DO UPDATE ... WHERE
  EXCLUDED.seq >= board_snapshots.seq` so a stale snapshot (from a lagging
  instance) can never overwrite a newer one.
- **Cold-start hydration.** When a board is first opened and the broker has no
  state for it, the last Postgres snapshot is loaded into the broker before the
  board serves any client. Hydration is idempotent across instances: the broker's
  `Hydrate` only loads when the board is empty (a Redis Lua guard on the sequence
  key), so two instances cold-starting the same board cannot clobber each other.
- **Optional.** With no `WB_DATABASE_URL`, the `Store` is nil and persistence is
  disabled — single-instance dev and unit tests run unchanged.

## Consequences

- Boards survive Redis restarts; a cold instance rehydrates from Postgres.
- Redis remains the sole hot path; Postgres sees at most one write per board per
  interval, off the message path.
- Worst-case data loss is one snapshot interval on a hard crash (acceptable for a
  whiteboard; documented).
- Redundant snapshots from multiple instances are harmless: idempotent,
  seq-guarded upserts.

## Alternatives considered

- **Write-through on every op.** Zero data loss but a Postgres write on the hot
  path — defeats the Redis tier. Rejected.
- **On-close only.** Less DB traffic but a mid-session crash loses everything
  since the board opened. Rejected in favour of periodic + on-close.
- **Row per shape.** More normalized and allows incremental per-shape upserts,
  but many more writes and more code for whole-board snapshots we already have in
  hand. Rejected for now.
- **Op-log / event sourcing.** Full history and replay, but a larger model than
  the chosen LWW + snapshot approach (ADR 0003). Out of scope.

## Testing

- Coordinator and hydration logic are tested against an in-memory fake `Store`
  and the in-memory broker — fast and hermetic.
- The pgx `Store` is tested against a throwaway Postgres via testcontainers-go:
  runs in CI (Docker available) and auto-skips where Docker is not present.
