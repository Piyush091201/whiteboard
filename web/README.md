# Frontend (React + TypeScript + Canvas)

A deliberately lean client — the engineering focus of this project is the Go
real-time backend, so the UI is just enough to exercise the whole protocol:
draw rectangles, see everyone's shapes sync live (last-write-wins by server
sequence), live cursors, and presence.

## Run

Start the backend (in-memory broker is fine for local dev):

```sh
# from the repo root
go run ./cmd/server
```

Then the frontend dev server:

```sh
# from web/
npm install
npm run dev
```

Open <http://localhost:5173/#demo> in two browser windows to collaborate on the
board `demo`. Optional query params: `?name=Ada` sets a display name; the board
id is the URL hash (`#demo`, `#room`, …).

The client connects directly to the server's WebSocket endpoint at
`ws://<host>:8080/ws/<board>`; no proxy is required.

## What it exercises

- **Drawing sync** — drag to draw a rectangle; it is sent as `shape.create`,
  sequenced by the server, and echoed to every participant.
- **Snapshot on join** — a newly opened window receives the current board state.
- **Live cursors** — pointer moves are throttled and broadcast; other users'
  cursors render as colored dots.
- **Presence** — the roster updates as users join and leave.
- **Reconnect + resync** — if the socket drops, the client reconnects and
  rehydrates from the snapshot.
