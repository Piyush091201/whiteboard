package hub

import (
	"encoding/json"
	"testing"

	"github.com/Piyush091201/whiteboard/internal/protocol"
)

func raw(s string) json.RawMessage { return json.RawMessage(s) }

// TestDocumentLastWriteWins verifies that repeated writes to the same shape id
// converge on the most recent one, with monotonically increasing sequence
// numbers, and that delete removes the shape.
func TestDocumentLastWriteWins(t *testing.T) {
	d := newDocument()

	if seq := d.apply(protocol.TypeShapeCreate, protocol.ShapeOp{ID: "a", Shape: raw(`"v1"`)}); seq != 1 {
		t.Fatalf("first seq = %d, want 1", seq)
	}
	if seq := d.apply(protocol.TypeShapeUpdate, protocol.ShapeOp{ID: "a", Shape: raw(`"v2"`)}); seq != 2 {
		t.Fatalf("second seq = %d, want 2", seq)
	}

	snap := d.snapshot()
	if len(snap.Shapes) != 1 {
		t.Fatalf("want 1 shape, got %d", len(snap.Shapes))
	}
	if got := string(snap.Shapes[0].Shape); got != `"v2"` {
		t.Errorf("shape = %s, want the latest write %q", got, `"v2"`)
	}
	if snap.Seq != 2 {
		t.Errorf("snapshot seq = %d, want 2", snap.Seq)
	}

	d.apply(protocol.TypeShapeDelete, protocol.ShapeOp{ID: "a"})
	if got := d.snapshot(); len(got.Shapes) != 0 {
		t.Errorf("after delete want 0 shapes, got %d", len(got.Shapes))
	}
}

// TestDocumentSnapshotOrderedBySeq verifies the snapshot is ordered by sequence
// number — i.e. by most-recent write — which gives a stable draw order. A shape
// that is updated jumps ahead of one created after it.
func TestDocumentSnapshotOrderedBySeq(t *testing.T) {
	d := newDocument()
	d.apply(protocol.TypeShapeCreate, protocol.ShapeOp{ID: "a", Shape: raw(`1`)}) // seq 1
	d.apply(protocol.TypeShapeCreate, protocol.ShapeOp{ID: "b", Shape: raw(`2`)}) // seq 2
	d.apply(protocol.TypeShapeUpdate, protocol.ShapeOp{ID: "a", Shape: raw(`3`)}) // seq 3 -> a now newest

	snap := d.snapshot()
	if len(snap.Shapes) != 2 {
		t.Fatalf("want 2 shapes, got %d", len(snap.Shapes))
	}
	if snap.Shapes[0].ID != "b" || snap.Shapes[1].ID != "a" {
		t.Errorf("order = [%s %s], want [b a]", snap.Shapes[0].ID, snap.Shapes[1].ID)
	}
}
