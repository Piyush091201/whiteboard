package broker

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"

	"github.com/Piyush091201/whiteboard/internal/protocol"
)

// applyShapeScript atomically assigns the next sequence and updates the shapes
// hash. Because INCR and the write happen in one atomic script, sequence order
// equals store order across all instances, so a plain HSET/HDEL is correct
// last-write-wins — no compare-and-set or tombstones are required.
//
// KEYS[1] = seq counter key, KEYS[2] = shapes hash key.
// ARGV[1] = shape id, ARGV[2] = "1" for delete else "0", ARGV[3] = shape blob.
// The stored hash value is "<seq>\n<blob>" so a snapshot can recover the
// per-shape sequence without a second read.
var applyShapeScript = redis.NewScript(`
local seq = redis.call('INCR', KEYS[1])
if ARGV[2] == '1' then
  redis.call('HDEL', KEYS[2], ARGV[1])
else
  redis.call('HSET', KEYS[2], ARGV[1], seq .. '\n' .. ARGV[3])
end
return seq
`)

// hydrateScript loads a snapshot into a board only if it has no state yet
// (the sequence key does not exist), making cold-start hydration idempotent
// across instances.
//
// KEYS[1] = seq counter key, KEYS[2] = shapes hash key.
// ARGV[1] = board sequence, then (shapeId, "<seq>\n<blob>") pairs.
var hydrateScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 1 then
  return 0
end
redis.call('SET', KEYS[1], ARGV[1])
for i = 2, #ARGV, 2 do
  redis.call('HSET', KEYS[2], ARGV[i], ARGV[i+1])
end
return 1
`)

// Redis is the multi-instance Broker backed by a Redis server: INCR for the
// global sequence, a HASH for authoritative shape state, and pub/sub for
// fan-out.
type Redis struct {
	rdb *redis.Client
}

// NewRedis wraps a go-redis client as a Broker.
func NewRedis(rdb *redis.Client) *Redis { return &Redis{rdb: rdb} }

func seqKey(boardID string) string      { return "board:" + boardID + ":seq" }
func shapesKey(boardID string) string   { return "board:" + boardID + ":shapes" }
func presenceKey(boardID string) string { return "board:" + boardID + ":presence" }
func channelKey(boardID string) string  { return "board:" + boardID }

func (r *Redis) ApplyShape(ctx context.Context, boardID, shapeID string, shape []byte, del bool) (uint64, error) {
	delArg := "0"
	if del {
		delArg = "1"
	}
	seq, err := applyShapeScript.Run(ctx, r.rdb,
		[]string{seqKey(boardID), shapesKey(boardID)},
		shapeID, delArg, string(shape),
	).Int64()
	if err != nil {
		return 0, err
	}
	return uint64(seq), nil
}

func (r *Redis) Publish(ctx context.Context, boardID string, message []byte) error {
	return r.rdb.Publish(ctx, channelKey(boardID), message).Err()
}

func (r *Redis) Subscribe(ctx context.Context, boardID string) (<-chan []byte, error) {
	pubsub := r.rdb.Subscribe(ctx, channelKey(boardID))
	// Block until the subscription is confirmed so no early publish is missed.
	if _, err := pubsub.Receive(ctx); err != nil {
		_ = pubsub.Close()
		return nil, err
	}

	out := make(chan []byte, 256)
	go func() {
		defer close(out)
		defer func() { _ = pubsub.Close() }()
		in := pubsub.Channel()
		for {
			select {
			case msg, ok := <-in:
				if !ok {
					return
				}
				select {
				case out <- []byte(msg.Payload):
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

func (r *Redis) Snapshot(ctx context.Context, boardID string) (protocol.Snapshot, error) {
	fields, err := r.rdb.HGetAll(ctx, shapesKey(boardID)).Result()
	if err != nil {
		return protocol.Snapshot{}, err
	}

	shapes := make([]protocol.SnapshotShape, 0, len(fields))
	for id, v := range fields {
		nl := strings.IndexByte(v, '\n')
		if nl < 0 {
			continue // malformed; skip defensively
		}
		seq, _ := strconv.ParseUint(v[:nl], 10, 64)
		shapes = append(shapes, protocol.SnapshotShape{
			Seq:   seq,
			ID:    id,
			Shape: json.RawMessage(v[nl+1:]),
		})
	}
	sort.Slice(shapes, func(i, j int) bool { return shapes[i].Seq < shapes[j].Seq })

	// The counter is the board's current sequence; missing key means zero.
	cur, err := r.rdb.Get(ctx, seqKey(boardID)).Uint64()
	if err != nil && !errors.Is(err, redis.Nil) {
		return protocol.Snapshot{}, err
	}
	return protocol.Snapshot{Seq: cur, Shapes: shapes}, nil
}

func (r *Redis) Hydrate(ctx context.Context, boardID string, snap protocol.Snapshot) (bool, error) {
	argv := make([]any, 0, 1+2*len(snap.Shapes))
	argv = append(argv, snap.Seq)
	for _, s := range snap.Shapes {
		argv = append(argv, s.ID, strconv.FormatUint(s.Seq, 10)+"\n"+string(s.Shape))
	}
	loaded, err := hydrateScript.Run(ctx, r.rdb,
		[]string{seqKey(boardID), shapesKey(boardID)}, argv...,
	).Int64()
	if err != nil {
		return false, err
	}
	return loaded == 1, nil
}

func (r *Redis) SetPresence(ctx context.Context, boardID, clientID string, presence []byte) error {
	return r.rdb.HSet(ctx, presenceKey(boardID), clientID, presence).Err()
}

func (r *Redis) RemovePresence(ctx context.Context, boardID, clientID string) error {
	return r.rdb.HDel(ctx, presenceKey(boardID), clientID).Err()
}

func (r *Redis) Presence(ctx context.Context, boardID string) ([]protocol.Presence, error) {
	fields, err := r.rdb.HGetAll(ctx, presenceKey(boardID)).Result()
	if err != nil {
		return nil, err
	}
	out := make([]protocol.Presence, 0, len(fields))
	for _, raw := range fields {
		var p protocol.Presence
		if err := json.Unmarshal([]byte(raw), &p); err != nil {
			continue
		}
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ClientID < out[j].ClientID })
	return out, nil
}

func (r *Redis) Close() error { return r.rdb.Close() }
