// Wire types mirroring internal/protocol. The server treats a shape's body as an
// opaque blob, so the client owns its structure — here, an axis-aligned rect.

export type Rect = {
  type: "rect";
  x: number;
  y: number;
  w: number;
  h: number;
  color: string;
};

export type Presence = { clientId: string; name?: string; color?: string };

export type SnapshotShape = { seq: number; id: string; shape: Rect };
export type Snapshot = { seq: number; shapes: SnapshotShape[] };

export type PresenceState = { self: Presence; others: Presence[] };
export type Cursor = { clientId: string; x: number; y: number };
export type ShapeOp = { id: string; shape?: Rect };

// A tagged envelope. Payload is decoded based on `type`.
export type Envelope = {
  type: string;
  seq?: number;
  payload?: unknown;
};

export const MsgType = {
  ShapeCreate: "shape.create",
  ShapeUpdate: "shape.update",
  ShapeDelete: "shape.delete",
  Cursor: "cursor",
  PresenceJoin: "presence.join",
  PresenceLeave: "presence.leave",
  PresenceState: "presence.state",
  Snapshot: "snapshot",
} as const;
