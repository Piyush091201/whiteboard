package hub

import (
	"encoding/json"
	"sort"

	"github.com/Piyush091201/whiteboard/internal/protocol"
)

// storedShape is the latest known state of one shape, plus the sequence number
// of the write that produced it.
type storedShape struct {
	seq   uint64
	shape json.RawMessage
}

// document is a board's in-memory shape state, owned exclusively by the board
// goroutine. It implements last-write-wins per shape: the board assigns a
// monotonically increasing sequence number to every write, so the most recent
// write for a given shape id always wins. No locking is required because only
// the single board goroutine ever touches it.
//
// For a C# developer: this is the aggregate the board "actor" owns. Because the
// sequence counter only advances inside one goroutine, it needs neither a lock
// nor Interlocked.Increment — the single-writer design gives ordering for free.
type document struct {
	seq    uint64
	shapes map[string]storedShape
}

func newDocument() *document {
	return &document{shapes: make(map[string]storedShape)}
}

// apply mutates the document for a shape operation and returns the sequence
// number assigned to it. Because seq is monotonically increasing, applying ops
// in arrival order is exactly last-write-wins.
func (d *document) apply(t protocol.Type, op protocol.ShapeOp) uint64 {
	d.seq++
	if t == protocol.TypeShapeDelete {
		delete(d.shapes, op.ID)
	} else {
		d.shapes[op.ID] = storedShape{seq: d.seq, shape: op.Shape}
	}
	return d.seq
}

// snapshot returns the current shapes ordered by sequence number, giving a
// stable draw order (later writes render on top).
func (d *document) snapshot() protocol.Snapshot {
	shapes := make([]protocol.SnapshotShape, 0, len(d.shapes))
	for id, s := range d.shapes {
		shapes = append(shapes, protocol.SnapshotShape{Seq: s.seq, ID: id, Shape: s.shape})
	}
	sort.Slice(shapes, func(i, j int) bool { return shapes[i].Seq < shapes[j].Seq })
	return protocol.Snapshot{Seq: d.seq, Shapes: shapes}
}
