package protocol

import (
	"encoding/json"
	"testing"
)

// TestEnvelopeTwoStageDecode covers the round trip: marshal an op into an
// envelope, then decode it back in two stages (envelope, then payload).
func TestEnvelopeTwoStageDecode(t *testing.T) {
	data, err := Marshal(TypeShapeCreate, 7, ShapeOp{
		ID:    "s1",
		Shape: json.RawMessage(`{"kind":"rect","x":1}`),
	})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	var env Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	if env.Type != TypeShapeCreate {
		t.Errorf("type = %q, want %q", env.Type, TypeShapeCreate)
	}
	if env.Seq != 7 {
		t.Errorf("seq = %d, want 7", env.Seq)
	}

	var op ShapeOp
	if err := env.DecodePayload(&op); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if op.ID != "s1" {
		t.Errorf("op.ID = %q, want s1", op.ID)
	}
	if string(op.Shape) != `{"kind":"rect","x":1}` {
		t.Errorf("op.Shape = %s, want the original blob", op.Shape)
	}
}

// TestDeleteOmitsShape verifies a delete op marshals without a shape body.
func TestDeleteOmitsShape(t *testing.T) {
	data, err := Marshal(TypeShapeDelete, 3, ShapeOp{ID: "gone"})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	// The payload must not contain a "shape" field.
	var probe struct {
		Payload map[string]json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal(data, &probe); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, present := probe.Payload["shape"]; present {
		t.Errorf("delete payload unexpectedly carried a shape field: %s", data)
	}
}

// TestDecodeEmptyPayload ensures payload-less messages decode without error.
func TestDecodeEmptyPayload(t *testing.T) {
	var env Envelope
	if err := json.Unmarshal([]byte(`{"type":"snapshot"}`), &env); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	var snap Snapshot
	if err := env.DecodePayload(&snap); err != nil {
		t.Fatalf("DecodePayload on empty payload should be a no-op, got %v", err)
	}
}
