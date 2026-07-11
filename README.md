# Collaborative Whiteboard

A real-time collaborative whiteboard built to showcase an idiomatic Go
concurrency design. Multiple users join a board by URL and draw shapes that sync
to everyone in real time, with live cursors and presence.

The whiteboard UI is deliberately the simple part. **The engineering focus is the
real-time backend**: a hub-and-spoke concurrency core, horizontal scalability via
Redis fan-out, last-write-wins conflict resolution, backpressure, graceful
shutdown, persistence, and observability — all proven correct under the race
detector in CI.

> **Status:** backend complete (phases 0–8). One instance sustains **10,000
> concurrent WebSocket connections** and fans out **250,000 messages/second** on a
> single dev laptop — see [Load test](#load-test). Frontend polish is the
> remaining phase.

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
   │ Canvas + WS  │◄─────►│  │ Client │──►│  loop) │◄────────────│◄──│ + state │
   └──────────────┘       │  └────────┘   └───┬────┘  subscribe  │   └────┬────┘
                          │                   │ snapshot         │        │
                          └───────────────────┼──────────────────┘        │
                                              ▼                           │
                                        ┌──────────┐                 ┌────┴────────┐
                                        │ Postgres │                 │ Go instance │
                                        │ snapshots│                 │      B       │
                                        └──────────┘                 └─────────────┘
```

- **Client** — one per WebSocket connection; owns exactly two goroutines
  (`readPump`, `writePump`) and a buffered outbound channel. Guaranteed to clean
  up on disconnect (no goroutine leaks, enforced by `goleak` in tests).
- **Board (room actor)** — one goroutine per active board; owns its local client
  set and relays the broker's per-board stream. No mutex on the hot path — the
  set is touched by a single goroutine.
- **Broker** — the shared sequencer, state store, and message bus. An in-memory
  implementation for single-instance/dev; a Redis implementation for horizontal
  scaling (per-board pub/sub, an `INCR` global sequence, and a `HASH` of
  authoritative shape state, all updated atomically via a Lua script).
- **Store** — Postgres, the durable system of record for board snapshots. Boards
  hydrate from it on cold start and snapshot back periodically and on close.

## Concurrency design

- **Hub-and-spoke:** one goroutine per connection for reads, one for writes; a
  central per-board actor owns the local client set and fans out over channels.
  State that must be shared across instances lives in the broker, not the actor.
- **Backpressure, two policies:** the board never blocks on a slow client — a
  non-blocking channel send (`select { case c.send <- msg: default: }`). Reliable
  traffic (shape ops, presence) *kicks* a client that falls too far behind so it
  reconnects and resyncs; ephemeral cursor updates are simply *dropped*.
- **Conflict resolution:** last-write-wins per shape, ordered by a
  server-assigned sequence. Because sequencing and the store write happen in one
  atomic step, sequence order equals store order, so plain overwrite is correct
  with no tombstones ([ADR 0003](docs/adr/0003-conflict-resolution-lww.md),
  [ADR 0002](docs/adr/0002-redis-fanout-vs-inmemory.md)).
- **Graceful shutdown:** on SIGTERM the hub stops accepting connections and drains
  live sessions (`http.Server.Shutdown` does *not* close hijacked WebSockets, so
  the hub closes them itself), writes a final snapshot per board, and waits for
  every `Serve` to return within a timeout.

**For .NET developers**, design notes throughout the code call out where
idiomatic Go differs from C#: channels vs locks, goroutines vs Tasks,
`context.Context` vs `CancellationToken`, error returns vs exceptions, `defer` vs
`using`/`finally`, functional options, and ports-and-adapters seams
(`Broker`/`Store`/`Metrics` interfaces).

## Key decisions (ADRs)

| ADR | Decision |
|-----|----------|
| [0001](docs/adr/0001-websocket-library.md) | WebSocket library: **coder/websocket** (context-first API) |
| [0002](docs/adr/0002-redis-fanout-vs-inmemory.md) | Cross-instance fan-out: **Redis pub/sub**, with Redis as state authority |
| [0003](docs/adr/0003-conflict-resolution-lww.md) | Conflict resolution: **last-write-wins per shape** |
| [0004](docs/adr/0004-persistence-postgres-snapshots.md) | Persistence: **Postgres board snapshots** (Redis as cache) |

## Repository layout

```
cmd/server/         # entrypoint: config, logging, http.Server, graceful shutdown
cmd/loadtest/       # concurrent-WebSocket load generator
internal/hub/       # real-time core: Hub, Board actor, Client pumps, shutdown
internal/broker/    # shared sequencer/state/bus: in-memory + Redis
internal/store/     # durable snapshots: Postgres + in-memory fake
internal/protocol/  # tagged JSON envelope + shape/cursor/presence types
internal/metrics/   # Prometheus exporter for the hub's Metrics interface
internal/ws/         # HTTP -> WebSocket upgrade glue
docs/adr/           # architecture decision records
deploy/             # docker-compose for local dev (app + Postgres + Redis)
```

## Build plan

| Phase | Deliverable | |
|-------|-------------|--|
| 0 | Scaffold: layout, tooling, CI, docker-compose | ✅ |
| 1 | Hub + WebSocket plumbing (tests, race detector, goleak) | ✅ |
| 2 | Drawing sync (shape ops, last-write-wins) | ✅ |
| 3 | Presence / live cursors | ✅ |
| 4 | Redis cross-instance fan-out | ✅ |
| 5 | Postgres snapshots + cold-start hydration | ✅ |
| 6 | Observability: structured logging, `/metrics` | ✅ |
| 7 | Graceful shutdown end-to-end (SIGTERM drain) | ✅ |
| 8 | Load test: N concurrent clients; documented numbers | ✅ |
| 9 | Frontend polish | ⬜ |

## Development

Requires Go 1.26+.

```sh
go run ./cmd/server      # start the server (GET /healthz, /metrics, /ws/{board})
go build ./...           # compile everything
go test ./...            # run tests
go test -race ./...      # run tests under the race detector (how CI runs them)
gofmt -l .               # list any unformatted files
go vet ./...             # static checks
```

Local full stack (app + Postgres + Redis), from the repo root:

```sh
docker compose -f deploy/docker-compose.yml up --build
```

Configuration (environment variables):

| Var | Default | Meaning |
|-----|---------|---------|
| `WB_ADDR` | `:8080` | HTTP listen address |
| `WB_REDIS_ADDR` | *(empty)* | Redis `host:port`; empty ⇒ in-memory single-instance broker |
| `WB_DATABASE_URL` | *(empty)* | Postgres DSN; empty ⇒ persistence disabled |

Endpoints: `GET /healthz`, `GET /metrics` (Prometheus), `GET /ws/{board}`
(WebSocket; optional `?name=&color=`).

## Load test

`cmd/loadtest` spins up N concurrent WebSocket clients across B boards. Each
client drains its inbound stream continuously (so the server never has to kick
it) and, at a configurable rate, sends shape updates with an embedded timestamp;
because ops fan out to every client on a board — including the sender — each
client measures round-trip latency by timing its own ops looping back.

```sh
# max connections (hold connections, no traffic)
go run ./cmd/loadtest -clients 10000 -boards 200 -rate 0 -duration 8s

# throughput + latency
go run ./cmd/loadtest -clients 1000 -boards 20 -rate 5 -duration 15s
```

### Results

Measured on a single Windows dev laptop, server and load generator on the same
machine over loopback, **in-memory broker** (single instance, no Redis/Postgres),
Go 1.26.

| Scenario | Clients | Result |
|----------|---------|--------|
| Max connections | 10,000 across 200 boards | **10,000/10,000** connected in **1.18 s**, 0 errors (≈20,000 server goroutines) |
| Throughput | 1,000 across 20 boards (~50/board), 5 ops/s each | **250,005 messages/s** delivered (fan-out); latency **p50 8.3 ms · p95 42 ms · p99 60 ms** |
| Steady mid-load | 500 across 20 boards, 2 ops/s each | ~25,000 messages/s delivered; latency **p99 29 ms** |

The fan-out multiplier is real work: at 1,000 clients on 20 boards, 5,000 inbound
ops/s become 250,000 outbound deliveries/s (each op goes to ~50 board members).
The hub sustains that with a p99 of 60 ms on one machine.

**Caveats (honest):** these numbers exercise the hub's concurrency core with the
in-memory broker on loopback — no network, no Redis/Postgres, client and server
sharing one CPU. A multi-instance deployment with Redis fan-out would trade some
latency for horizontal scale. The point of these figures is the single-instance
connection and fan-out ceiling of the goroutine/channel design.

## License

TBD.
