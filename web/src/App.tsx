import { useEffect, useRef, useState } from "react";
import {
  Cursor,
  Envelope,
  MsgType,
  Presence,
  PresenceState,
  Rect,
  ShapeOp,
  Snapshot,
} from "./protocol";

type StoredShape = { seq: number; shape: Rect };
type LiveCursor = { x: number; y: number; color: string; name: string };
type DrawState = { startX: number; startY: number; x: number; y: number };

const CURSOR_INTERVAL_MS = 40;

function wsURL(board: string, name: string): string {
  const q = name ? `?name=${encodeURIComponent(name)}` : "";
  return `ws://${location.hostname}:8080/ws/${encodeURIComponent(board)}${q}`;
}

export function App() {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const wsRef = useRef<WebSocket | null>(null);

  // Canvas state lives in refs so the animation loop can read it without
  // re-rendering React on every message.
  const shapes = useRef(new Map<string, StoredShape>());
  const cursors = useRef(new Map<string, LiveCursor>());
  const presence = useRef(new Map<string, Presence>());
  const self = useRef<Presence>({ clientId: "", color: "#334155", name: "" });
  const draw = useRef<DrawState | null>(null);
  const lastCursorSent = useRef(0);

  // React state drives only the chrome (roster + connection status).
  const [roster, setRoster] = useState<Presence[]>([]);
  const [status, setStatus] = useState<"connecting" | "connected" | "disconnected">("connecting");

  const board = location.hash.slice(1) || "demo";
  const name = new URLSearchParams(location.search).get("name") || "";

  const refreshRoster = () => setRoster([...presence.current.values()]);

  // --- WebSocket lifecycle -------------------------------------------------
  useEffect(() => {
    let stopped = false;
    let ws: WebSocket;

    const connect = () => {
      ws = new WebSocket(wsURL(board, name));
      wsRef.current = ws;
      ws.onopen = () => setStatus("connected");
      ws.onclose = () => {
        setStatus("disconnected");
        if (!stopped) setTimeout(connect, 1000); // reconnect + resync from snapshot
      };
      ws.onmessage = (e) => handleMessage(JSON.parse(e.data) as Envelope);
    };
    connect();

    return () => {
      stopped = true;
      ws?.close();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  function handleMessage(env: Envelope) {
    switch (env.type) {
      case MsgType.Snapshot: {
        const snap = env.payload as Snapshot;
        shapes.current.clear();
        for (const s of snap.shapes ?? []) shapes.current.set(s.id, { seq: s.seq, shape: s.shape });
        break;
      }
      case MsgType.ShapeCreate:
      case MsgType.ShapeUpdate: {
        const op = env.payload as ShapeOp;
        const cur = shapes.current.get(op.id);
        // Last-write-wins by server sequence.
        if (op.shape && (!cur || (env.seq ?? 0) >= cur.seq)) {
          shapes.current.set(op.id, { seq: env.seq ?? 0, shape: op.shape });
        }
        break;
      }
      case MsgType.ShapeDelete: {
        shapes.current.delete((env.payload as ShapeOp).id);
        break;
      }
      case MsgType.Cursor: {
        const c = env.payload as Cursor;
        const p = presence.current.get(c.clientId);
        cursors.current.set(c.clientId, {
          x: c.x,
          y: c.y,
          color: p?.color ?? "#888",
          name: p?.name ?? "",
        });
        break;
      }
      case MsgType.PresenceState: {
        const ps = env.payload as PresenceState;
        self.current = ps.self;
        presence.current.clear();
        for (const p of [ps.self, ...ps.others]) presence.current.set(p.clientId, p);
        refreshRoster();
        break;
      }
      case MsgType.PresenceJoin: {
        const p = env.payload as Presence;
        presence.current.set(p.clientId, p);
        refreshRoster();
        break;
      }
      case MsgType.PresenceLeave: {
        const p = env.payload as Presence;
        presence.current.delete(p.clientId);
        cursors.current.delete(p.clientId);
        refreshRoster();
        break;
      }
    }
  }

  function send(env: Envelope) {
    const ws = wsRef.current;
    if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(env));
  }

  // --- pointer handling ----------------------------------------------------
  function pointerPos(e: React.PointerEvent<HTMLCanvasElement>) {
    const r = canvasRef.current!.getBoundingClientRect();
    return { x: e.clientX - r.left, y: e.clientY - r.top };
  }

  function onPointerDown(e: React.PointerEvent<HTMLCanvasElement>) {
    const { x, y } = pointerPos(e);
    draw.current = { startX: x, startY: y, x, y };
    canvasRef.current!.setPointerCapture(e.pointerId);
  }

  function onPointerMove(e: React.PointerEvent<HTMLCanvasElement>) {
    const { x, y } = pointerPos(e);
    if (draw.current) {
      draw.current.x = x;
      draw.current.y = y;
    }
    const now = performance.now();
    if (now - lastCursorSent.current >= CURSOR_INTERVAL_MS) {
      lastCursorSent.current = now;
      send({ type: MsgType.Cursor, payload: { x, y } });
    }
  }

  function onPointerUp() {
    const d = draw.current;
    draw.current = null;
    if (!d) return;
    const rect = normalize(d);
    if (rect.w < 3 && rect.h < 3) return; // ignore accidental clicks
    const id = crypto.randomUUID();
    const shape: Rect = { type: "rect", ...rect, color: self.current.color ?? "#334155" };
    shapes.current.set(id, { seq: 0, shape }); // optimistic; server echo reconciles
    send({ type: MsgType.ShapeCreate, payload: { id, shape } });
  }

  // --- render loop ---------------------------------------------------------
  useEffect(() => {
    let raf = 0;
    const render = () => {
      const cv = canvasRef.current;
      if (cv) {
        if (cv.width !== cv.clientWidth) cv.width = cv.clientWidth;
        if (cv.height !== cv.clientHeight) cv.height = cv.clientHeight;
        paint(cv.getContext("2d")!, shapes.current, cursors.current, draw.current, self.current);
      }
      raf = requestAnimationFrame(render);
    };
    raf = requestAnimationFrame(render);
    return () => cancelAnimationFrame(raf);
  }, []);

  const statusColor =
    status === "connected" ? "#22c55e" : status === "connecting" ? "#eab308" : "#ef4444";

  return (
    <div className="app">
      <header className="bar">
        <div className="brand">
          <span className="dot" style={{ background: statusColor }} />
          Whiteboard <span className="board">#{board}</span>
        </div>
        <div className="roster">
          {roster.map((p) => (
            <span key={p.clientId} className="chip" title={p.name}>
              <span className="dot" style={{ background: p.color ?? "#888" }} />
              {p.clientId === self.current.clientId ? `${p.name || "you"} (you)` : p.name || "anon"}
            </span>
          ))}
        </div>
        <div className="hint">drag to draw</div>
      </header>
      <canvas
        ref={canvasRef}
        className="board-canvas"
        onPointerDown={onPointerDown}
        onPointerMove={onPointerMove}
        onPointerUp={onPointerUp}
      />
    </div>
  );
}

function normalize(d: DrawState) {
  return {
    x: Math.min(d.startX, d.x),
    y: Math.min(d.startY, d.y),
    w: Math.abs(d.x - d.startX),
    h: Math.abs(d.y - d.startY),
  };
}

function paint(
  ctx: CanvasRenderingContext2D,
  shapes: Map<string, StoredShape>,
  cursors: Map<string, LiveCursor>,
  draw: DrawState | null,
  self: Presence,
) {
  ctx.clearRect(0, 0, ctx.canvas.width, ctx.canvas.height);

  for (const { shape } of shapes.values()) {
    ctx.lineWidth = 2;
    ctx.strokeStyle = shape.color;
    ctx.fillStyle = shape.color + "22";
    ctx.fillRect(shape.x, shape.y, shape.w, shape.h);
    ctx.strokeRect(shape.x, shape.y, shape.w, shape.h);
  }

  if (draw) {
    const r = normalize(draw);
    ctx.setLineDash([6, 4]);
    ctx.strokeStyle = self.color ?? "#334155";
    ctx.strokeRect(r.x, r.y, r.w, r.h);
    ctx.setLineDash([]);
  }

  ctx.font = "12px system-ui, sans-serif";
  for (const [id, c] of cursors) {
    if (id === self.clientId) continue;
    ctx.fillStyle = c.color;
    ctx.beginPath();
    ctx.arc(c.x, c.y, 5, 0, Math.PI * 2);
    ctx.fill();
    if (c.name) {
      ctx.fillText(c.name, c.x + 9, c.y + 4);
    }
  }
}
