# ADR 0002: Cross-instance fan-out — Redis pub/sub vs in-memory only

- **Status:** Proposed (stub — to be finalized in Phase 4)
- **Date:** 2026-06-29
- **Deciders:** Project owner

## Context

A single in-memory hub can fan out messages only to clients connected to that
one process. To run more than one server instance behind a load balancer — the
whole point of "horizontally scalable" — instances must share board state.

> This ADR is a stub. It will be filled in when Phase 4 (Redis fan-out) is
> built. The outline below records the intended shape of the decision.

## Decision (intended)

Use **Redis pub/sub**, one channel per board, as the cross-instance message bus.
Each Board subscribes to its channel; local operations are published to Redis and
delivered back to every instance (including the originator) through a single
delivery path.

## To be documented in Phase 4

- Single-path delivery (publish then receive from Redis, even for the local
  instance) vs local-broadcast-plus-publish, and how we avoid double delivery.
- Ordering guarantees and their interaction with last-write-wins (ADR 0003).
- Reconnect/resubscribe behaviour when the Redis connection drops.
- Tradeoffs vs alternatives: in-memory only (simplest, single instance),
  sticky-session routing (no shared bus but fragile), and a dedicated broker
  such as NATS (more capable, heavier dependency).
