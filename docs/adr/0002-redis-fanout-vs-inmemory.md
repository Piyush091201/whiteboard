# ADR 0002: Cross-instance fan-out — Redis pub/sub with Redis as state authority

- **Status:** Accepted
- **Date:** 2026-06-30
- **Deciders:** Project owner

## Context

A single in-memory hub fans out messages only to clients connected to that one
process. To run more than one server instance behind a load balancer — the point
of "horizontally scalable" — instances must share board state. Two things break
specifically when a second instance is added:

1. **Fan-out** does not cross instances: a client on instance A never sees a
   client on instance B.
2. **Sequencing** breaks: each instance's in-memory `document` has its own
   monotonic counter, so the sequence numbers that drive last-write-wins
   (ADR 0003) collide across instances.

This is the same role that SignalR's Redis backplane plays for a scaled-out
ASP.NET app; here we build the backplane explicitly.

## Decision

Use **Redis** as both the message bus and the authoritative store of board
state:

- **Pub/Sub, one channel per board** (`board:{id}`) for live fan-out of shape
  ops, cursors, and presence events.
- **Redis is the source of truth**, so instances are stateless and any instance
  can serve a correct snapshot:
  - `board:{id}:seq` — an `INCR` counter is the **global** sequencer. The
    sequence number moves out of process.
  - `board:{id}:shapes` — a HASH (`shapeId → {seq, shape}`) holds authoritative
    shape state.
  - `board:{id}:presence` — a HASH (`clientId → Presence`) holds the global
    roster (Phase 4b).
- **Single delivery path.** A client op is sequenced and written to Redis, then
  `PUBLISH`ed; every instance — *including the originating one* — delivers to its
  local clients only when the message loops back through its subscription. One
  code path, globally consistent ordering, no double-delivery and no dedup logic.
- **Atomic last-write-wins on write.** Because two instances can `INCR` and then
  `HSET` in different wall-clock orders, a lower-seq write could otherwise clobber
  a higher-seq one. A small **Lua script** performs a compare-and-set: it writes
  the shape only if the incoming `seq` is greater than the stored one. LWW is
  thus enforced atomically at the store, independent of delivery order.
- **Client:** `github.com/redis/go-redis/v9` (context-first API, built-in
  pub/sub, pipelining, `EVAL` for the Lua script).
- **A `Broker` interface** abstracts publish/subscribe/sequence/state with two
  implementations: an in-memory broker (keeps single-instance mode and unit
  tests working with no Redis) and the Redis broker. Redis calls happen off the
  board goroutine so network I/O never blocks the single board loop.

## Consequences

- Instances become stateless with respect to board content; scaling out is
  adding processes. A freshly started instance serves correct snapshots by
  reading Redis.
- Each op costs a small, pipelineable amount of Redis work (`INCR` +
  scripted `HSET` + `PUBLISH`) and one pub/sub round-trip of loop-back latency on
  the local echo — negligible for a whiteboard.
- Redis becomes a hard dependency and a single point of failure for multi-instance
  operation. The Redis broker must handle disconnect/resubscribe; Phase 5
  (Postgres) adds durability so Redis is a cache of hot state, not the system of
  record.
- Ordering is robust to pub/sub reordering because clients and the store apply
  strictly by `(shapeId, seq)`.

## Alternatives considered

- **Pub/sub only, per-instance replicas (no Redis-side state).** Simpler, but a
  freshly started instance cannot build a correct snapshot from a history-less
  pub/sub stream until persistence exists. Rejected: correct snapshots on any
  instance are a Phase 4 requirement, not a Phase 5 one.
- **Redis Streams (XADD) as an ordered op-log with replay.** Stream IDs provide
  ordering and snapshots can be rebuilt by replay; it doubles as persistence.
  Rejected for now as heavier and a shift away from the LWW-plus-snapshot model
  chosen in ADR 0003; revisit if an op-log/history feature is wanted.
- **Board-owner instance does all sequencing.** Avoids per-op `INCR` but requires
  ownership election and failover. Rejected as unnecessary complexity.

## Rollout

- **Phase 4a:** cross-instance shape sync — Broker abstraction, `INCR` sequencer,
  Lua LWW write, pub/sub delivery, snapshot from Redis. Tested against miniredis.
- **Phase 4b:** cross-instance cursors (best-effort through the same channel) and
  a global presence roster backed by `board:{id}:presence`.
