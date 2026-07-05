package store_test

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/Piyush091201/whiteboard/internal/protocol"
	"github.com/Piyush091201/whiteboard/internal/store"
)

// jsonEqual compares two JSON blobs by value. Postgres JSONB canonicalizes JSON
// (whitespace, key order), so a round-tripped shape is semantically equal but
// not byte-identical to the original — which is fine, shape blobs are opaque
// JSON to the server.
func jsonEqual(t *testing.T, a, b []byte) bool {
	t.Helper()
	var av, bv any
	if err := json.Unmarshal(a, &av); err != nil {
		t.Fatalf("unmarshal %s: %v", a, err)
	}
	if err := json.Unmarshal(b, &bv); err != nil {
		t.Fatalf("unmarshal %s: %v", b, err)
	}
	return reflect.DeepEqual(av, bv)
}

// startPostgres spins a throwaway Postgres via testcontainers. It skips the test
// when Docker is unavailable (e.g. local dev without Docker), so the SQL is
// still covered in CI where Docker is present.
func startPostgres(t *testing.T) string {
	t.Helper()
	ctx := context.Background()

	container, err := tcpostgres.Run(ctx, "postgres:17-alpine",
		tcpostgres.WithDatabase("whiteboard"),
		tcpostgres.WithUsername("wb"),
		tcpostgres.WithPassword("wb"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		t.Skipf("skipping: cannot start postgres container (docker unavailable?): %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	return dsn
}

func snap(seq uint64, shape string) protocol.Snapshot {
	return protocol.Snapshot{
		Seq:    seq,
		Shapes: []protocol.SnapshotShape{{Seq: seq, ID: "s1", Shape: json.RawMessage(shape)}},
	}
}

func TestPostgresStoreRoundTrip(t *testing.T) {
	ctx := context.Background()
	s, err := store.NewPostgres(ctx, startPostgres(t))
	if err != nil {
		t.Fatalf("new postgres store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	// Missing board -> not found.
	if _, ok, err := s.LoadSnapshot(ctx, "nope"); err != nil || ok {
		t.Fatalf("load missing = (ok=%v, err=%v), want (false, nil)", ok, err)
	}

	// Save then load round-trips.
	if err := s.SaveSnapshot(ctx, "board1", snap(2, `{"kind":"rect"}`)); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, ok, err := s.LoadSnapshot(ctx, "board1")
	if err != nil || !ok {
		t.Fatalf("load = (ok=%v, err=%v), want present", ok, err)
	}
	if got.Seq != 2 || len(got.Shapes) != 1 || got.Shapes[0].ID != "s1" ||
		!jsonEqual(t, got.Shapes[0].Shape, []byte(`{"kind":"rect"}`)) {
		t.Fatalf("loaded snapshot = %+v, want the saved one", got)
	}
}

func TestPostgresStoreSeqGuard(t *testing.T) {
	ctx := context.Background()
	s, err := store.NewPostgres(ctx, startPostgres(t))
	if err != nil {
		t.Fatalf("new postgres store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	if err := s.SaveSnapshot(ctx, "b", snap(5, `"v5"`)); err != nil {
		t.Fatalf("save seq 5: %v", err)
	}
	// A stale write (lower seq) must not overwrite the newer snapshot.
	if err := s.SaveSnapshot(ctx, "b", snap(3, `"v3"`)); err != nil {
		t.Fatalf("save seq 3: %v", err)
	}
	got, _, err := s.LoadSnapshot(ctx, "b")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got.Seq != 5 || string(got.Shapes[0].Shape) != `"v5"` {
		t.Fatalf("after stale write, snapshot = %+v, want seq 5 kept", got)
	}

	// A newer write wins.
	if err := s.SaveSnapshot(ctx, "b", snap(7, `"v7"`)); err != nil {
		t.Fatalf("save seq 7: %v", err)
	}
	got, _, err = s.LoadSnapshot(ctx, "b")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got.Seq != 7 || string(got.Shapes[0].Shape) != `"v7"` {
		t.Fatalf("after newer write, snapshot = %+v, want seq 7", got)
	}
}
