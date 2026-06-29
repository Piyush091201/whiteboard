# ADR 0003: Conflict resolution — last-write-wins per shape

- **Status:** Accepted
- **Date:** 2026-06-29
- **Deciders:** Project owner

## Context

Multiple users edit the same board concurrently. When two edits touch the same
shape, the server must converge all clients to one agreed state. The options
range from simple (last-write-wins) to complex (operational transforms, CRDTs).
The project explicitly prefers shipping a correct simple model over a
half-finished complex one.

## Decision

Use **last-write-wins (LWW) per shape**, keyed by a stable shape `id` and ordered
by a **server-assigned monotonic sequence number** per board.

- Each board has a single writer goroutine (the Board actor), so the sequence
  number is assigned without locks or atomics — ordering is a natural
  consequence of the single-writer design.
- A create/update/delete with a higher sequence number wins for that shape.
- In-progress freehand strokes are treated as **ephemeral** until committed as a
  single shape, so we do not run conflict resolution on every pointer move.

## Tradeoffs

- **Pros:** trivial to reason about, no merge logic, no per-character state;
  clients recover to a consistent state simply by reloading the latest snapshot.
- **Cons:** concurrent edits to the *same* shape lose one side's change. The
  lost-update window is a few milliseconds and same-shape simultaneous edits are
  rare on a whiteboard, so this is an acceptable cost.

## Alternatives considered

- **Operation log** — append-only ordered ops replayed on join; enables history
  and undo but adds state and edge cases. Rejected for v1; may revisit if
  undo/redo becomes a requirement.
- **CRDTs** — strongest convergence guarantees but substantial complexity.
  Explicitly out of scope.

## Consequences

- The protocol carries a shape `id` and the server stamps a sequence number;
  clients apply incoming ops by `(id, seq)`.
- Snapshot persistence (Phase 5) stores the latest shape state, which is
  sufficient for reconnect/resync precisely because state is LWW.
