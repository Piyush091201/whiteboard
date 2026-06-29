# Collaborative Whiteboard

A real-time collaborative whiteboard built to showcase an idiomatic Go
concurrency design. Multiple users join a board by URL and draw shapes
(rectangles, lines, freehand, sticky notes) that sync to everyone in real time,
with live cursors and presence.

The whiteboard UI is deliberately the simple part. **The engineering focus is the
real-time backend**: a hub-and-spoke concurrency core, horizontal scalability via
Redis fan-out, robust connection handling, graceful shutdown, and observability —
all proven correct under the race detector.

> **Status:** scaffold. The repository layout, tooling, and cross-cutting
> concerns (config, structured logging, graceful shutdown) are in place. The
> real-time hub lands in Phase 1.

---

## Architecture

```
                          ┌─────────────────────────────────────┐
   Browser (React/TS)     │            Go instance A             │
   ┌──────────────┐  WS   │  ┌────────┐   register/unregister    │
   │ Canvas + WS  │◄─────►│  │ Client │──────┐                   │
   │   client     │       │  │readPump│      ▼                   │
   └──────────────┘       │  │writePmp│◄──┌────────┐  publish    │   ┌─────────┐
                          │  └────────┘   │ Board  │────────────►│──►│  Redis  │
   ┌──────────────┐  WS   │  ┌────────┐   │ (actor │             │   │ pub/sub │
   │ Canvas + WS  │◄─────►│  │ Client │──►│  loop) │◄────────────│◄──│ per-    │
   └──────────────┘       │  └────────┘   └───┬────┘  subscribe  │   │ board   │
                          │                   │ snapshot         │   └────┬────┘
                          └───────────────────┼──────────────────┘        │
                                              ▼                           │
                                        ┌──────────┐                 ┌────┴────────┐
                                        │ Postgres │                 │ Go instance │
                                        │ snapshots│                 │      B       │
                                        └──────────┘                 └─────────────┘
```

- **Client** — one per WebSocket connection; owns exactly two goroutines
  (`readPump`, `writePump`) and a buffered outbound channel. Guaranteed to clean
  up on disconnect (no goroutine leaks).
- **Board (room actor)** — one goroutine per active board; owns its client set
  and the fan-out `select` loop. No mutex on the hot path — state is touched by a
  single goroutine.
- **Redis pub/sub** — cross-instance fan-out so multiple server instances share
  board state.
- **Postgres** — board snapshots for persistence and reconnect resync.

## Concurrency design

> Detailed write-up lands with Phase 1. Summary of the model:
>
> - **Hub-and-spoke:** one goroutine per connection for reads, one for writes; a
>   central per-board actor fans out messages over channels.
> - **Backpressure:** the board never blocks on a slow client — a non-blocking
>   channel send (`select { case c.send <- msg: default: }`) drops ephemeral
>   cursor updates and disconnects clients that fall too far behind, which then
>   reconnect and resync.
> - **Graceful shutdown:** a root `context.Context` cancelled on SIGTERM drains
>   connections cleanly; a `sync.WaitGroup` blocks exit until every board and
>   client has finished.
>
> **For .NET developers**, the design notes throughout the code call out where
> idiomatic Go differs from C#: channels vs locks, goroutines vs Tasks,
> `context.Context` vs `CancellationToken`, error returns vs exceptions, `defer`
> vs `using`/`finally`.

## Key decisions (ADRs)

| ADR | Decision |
|-----|----------|
| [0001](docs/adr/0001-websocket-library.md) | WebSocket library: **coder/websocket** (context-first API) |
| [0002](docs/adr/0002-redis-fanout-vs-inmemory.md) | Cross-instance fan-out: **Redis pub/sub** (stub, finalized in Phase 4) |
| [0003](docs/adr/0003-conflict-resolution-lww.md) | Conflict resolution: **last-write-wins per shape** |

## Repository layout

```
cmd/server/        # entrypoint: config, logging, http.Server, graceful shutdown
internal/hub/      # real-time core: Board actor + Client pumps (Phase 1)
internal/ws/       # HTTP -> WebSocket upgrade glue (Phase 1)
internal/protocol/ # message envelope + shape op types (Phase 2)
web/               # React + TS + Canvas frontend (Phase 9)
docs/adr/          # architecture decision records
deploy/            # docker-compose for local dev (app + Postgres + Redis)
```

## Build plan

| Phase | Deliverable |
|-------|-------------|
| 0 ✅ | Scaffold: layout, tooling, CI, docker-compose, graceful-shutdown skeleton |
| 1 | Hub + WebSocket plumbing (tests, race detector, goroutine-leak checks) |
| 2 | Drawing sync (shape ops, last-write-wins) |
| 3 | Presence / live cursors |
| 4 | Redis cross-instance fan-out |
| 5 | Postgres snapshots + reconnect resync |
| 6 | Observability: structured logging, `/metrics` |
| 7 | Graceful shutdown end-to-end (SIGTERM drain) |
| 8 | Load test: N concurrent clients; documented connections/instance |
| 9 | Frontend polish |

## Development

Requires Go 1.26+.

```sh
go run ./cmd/server      # start the server (serves GET /healthz for now)
go build ./...           # compile everything
go test ./...            # run tests
go test -race ./...      # run tests under the race detector (how CI runs them)
gofmt -l .               # list any unformatted files
go vet ./...             # static checks
```

Or use the `Makefile` targets (`make run`, `make test-race`, `make lint`, …) if
`make` is available.

Local full stack (app + Postgres + Redis), from `deploy/`:

```sh
docker compose -f deploy/docker-compose.yml up --build
```

Configuration (environment variables):

| Var | Default | Meaning |
|-----|---------|---------|
| `WB_ADDR` | `:8080` | HTTP listen address |

## Load-test numbers

> Filled in at Phase 8 — the proof that the concurrency design holds up.

## License

TBD.
