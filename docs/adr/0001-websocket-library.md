# ADR 0001: WebSocket library — coder/websocket

- **Status:** Accepted
- **Date:** 2026-06-29
- **Deciders:** Project owner

## Context

The whiteboard's real-time transport is WebSockets. The two mainstream Go
libraries are `github.com/coder/websocket` (formerly `nhooyr.io/websocket`) and
`github.com/gorilla/websocket`. We need to pick one for the whole project.

The project's primary goal is to showcase an idiomatic Go concurrency design,
with `context.Context` threaded end-to-end for cancellation, timeouts, and
graceful shutdown.

## Decision

Use **`github.com/coder/websocket`**.

## Rationale

- **Context-first API.** `conn.Read(ctx)` and `conn.Write(ctx)` take a context
  directly, which matches the cancellation model used everywhere else in the
  server (graceful shutdown, per-read/write deadlines via `context.WithTimeout`).
  This keeps the concurrency story consistent and is the closest analog to
  .NET's `ReadAsync(cancellationToken)`.
- **Actively maintained**, whereas gorilla/websocket is effectively in
  maintenance mode.
- **Clean net/http integration** via `websocket.Accept(w, r, opts)`.
- Built-in `conn.Ping(ctx)` simplifies the heartbeat implementation (Phase 1/4).

## Alternatives considered

- **gorilla/websocket** — battle-tested, lowest per-message allocation, and home
  to the canonical hub/chat example that defines this pattern. Rejected as the
  primary choice because its deadline-based API (`SetReadDeadline`) is less
  consistent with our context-everywhere design, and it is no longer actively
  developed. We still treat its `chat` example as required reading for the hub
  design.

## Consequences

- All connection code depends on coder/websocket's API shape.
- Slightly higher per-message overhead than gorilla in some modes; we will let
  the Phase 8 load test confirm this is not a practical bottleneck before
  considering any change.
