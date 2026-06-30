// Package protocol defines the wire format exchanged between clients and the
// server: a single tagged JSON envelope plus the per-type payloads.
//
// Decoding is two-stage: unmarshal the Envelope (which keeps Payload as a raw,
// not-yet-parsed json.RawMessage), switch on Type, then unmarshal Payload into
// the concrete payload struct. This is the idiomatic Go alternative to .NET's
// polymorphic [JsonDerivedType] deserialization — explicit rather than
// reflective.
package protocol

import "encoding/json"

// Type tags every message. The string values are the on-the-wire discriminator.
type Type string

const (
	// Client -> server (and echoed, sequenced, server -> all clients).
	TypeShapeCreate Type = "shape.create"
	TypeShapeUpdate Type = "shape.update"
	TypeShapeDelete Type = "shape.delete"

	// Server -> a single client on join.
	TypeSnapshot Type = "snapshot"

	// Server -> client on a protocol error.
	TypeError Type = "error"
)

// Envelope is the tagged container for every message.
//
// Seq is the authoritative, server-assigned sequence number for shape
// operations (zero on inbound client messages and on snapshots, which carry
// their own Seq inside the payload). Payload is decoded in a second step based
// on Type.
type Envelope struct {
	Type    Type            `json:"type"`
	Seq     uint64          `json:"seq,omitempty"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// DecodePayload unmarshals the envelope payload into v. A nil/empty payload is a
// no-op so that payload-less messages decode cleanly.
func (e Envelope) DecodePayload(v any) error {
	if len(e.Payload) == 0 {
		return nil
	}
	return json.Unmarshal(e.Payload, v)
}

// ShapeOp is the payload for shape.create / shape.update / shape.delete.
//
// Shape carries the opaque, client-defined shape data (geometry, style, …). The
// server is intentionally agnostic about its contents: it only sequences and
// stores the blob, leaving rendering semantics to the client. Shape is absent
// for shape.delete.
type ShapeOp struct {
	ID    string          `json:"id"`
	Shape json.RawMessage `json:"shape,omitempty"`
}

// Snapshot is the payload sent to a client when it joins a board. Seq is the
// board's current sequence number; Shapes are ordered by their own Seq, which
// gives a stable draw order (later writes on top).
type Snapshot struct {
	Seq    uint64          `json:"seq"`
	Shapes []SnapshotShape `json:"shapes"`
}

// SnapshotShape is one shape's current state within a Snapshot.
type SnapshotShape struct {
	Seq   uint64          `json:"seq"`
	ID    string          `json:"id"`
	Shape json.RawMessage `json:"shape"`
}

// Marshal builds an envelope of type t carrying payload (already-sequenced with
// seq) and returns its JSON encoding, ready to send on the wire.
func Marshal(t Type, seq uint64, payload any) ([]byte, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return json.Marshal(Envelope{Type: t, Seq: seq, Payload: raw})
}
