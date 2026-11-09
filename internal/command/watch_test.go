package command

import (
	"context"
	"testing"

	"github.com/maltemindedal/stash/internal/protocol"
)

// TestWatchIsInvalidatedOnlyByWritesThatChangeTheKey runs every write command
// against a key another client watches, once so that it changes the key and,
// where the command can succeed without changing it, once so that it does not.
// SADD, ZADD, GEOADD, PFADD, HSET and SETBIT invalidate WATCH whenever they
// succeed, even when they leave the value as it was, unlike Redis.
func TestWatchIsInvalidatedOnlyByWritesThatChangeTheKey(t *testing.T) {
	type command []string
	tests := []struct {
		name        string
		setup       []command
		write       []command
		invalidates bool
	}{
		{name: "SET", write: []command{{"SET", "k", "v"}}, invalidates: true},
		{name: "SET of the value the key holds", setup: []command{{"SET", "k", "v"}}, write: []command{{"SET", "k", "v"}}, invalidates: true},
		{name: "SETBIT", write: []command{{"SETBIT", "k", "7", "1"}}, invalidates: true},
		{name: "SETBIT of the bit the key holds", setup: []command{{"SETBIT", "k", "7", "1"}}, write: []command{{"SETBIT", "k", "7", "1"}}, invalidates: true},
		{name: "PFADD", write: []command{{"PFADD", "k", "a"}}, invalidates: true},
		{name: "PFADD of an element already counted", setup: []command{{"PFADD", "k", "a"}}, write: []command{{"PFADD", "k", "a"}}, invalidates: true},
		{name: "INCR", write: []command{{"INCR", "k"}}, invalidates: true},
		{name: "DEL", setup: []command{{"SET", "k", "v"}}, write: []command{{"DEL", "k"}}, invalidates: true},
		{name: "DEL of a missing key", write: []command{{"DEL", "k"}}},
		{name: "DEL of several keys", setup: []command{{"SET", "k", "v"}}, write: []command{{"DEL", "other", "k"}}, invalidates: true},
		{name: "DEL of several keys, the watched one missing", setup: []command{{"SET", "other", "v"}}, write: []command{{"DEL", "other", "k"}}},
		{name: "LPUSH", write: []command{{"LPUSH", "k", "a"}}, invalidates: true},
		{name: "RPUSH", write: []command{{"RPUSH", "k", "a"}}, invalidates: true},
		{name: "LPOP", setup: []command{{"RPUSH", "k", "a", "b"}}, write: []command{{"LPOP", "k"}}, invalidates: true},
		{name: "LPOP of a missing key", write: []command{{"LPOP", "k"}}},
		{name: "LPOP with a count", setup: []command{{"RPUSH", "k", "a", "b"}}, write: []command{{"LPOP", "k", "1"}}, invalidates: true},
		{name: "LPOP with a count of 0", setup: []command{{"RPUSH", "k", "a", "b"}}, write: []command{{"LPOP", "k", "0"}}},
		{name: "RPOP", setup: []command{{"RPUSH", "k", "a", "b"}}, write: []command{{"RPOP", "k"}}, invalidates: true},
		{name: "RPOP of a missing key", write: []command{{"RPOP", "k"}}},
		{name: "RPOP with a count", setup: []command{{"RPUSH", "k", "a", "b"}}, write: []command{{"RPOP", "k", "2"}}, invalidates: true},
		{name: "RPOP with a count of 0", setup: []command{{"RPUSH", "k", "a", "b"}}, write: []command{{"RPOP", "k", "0"}}},
		{name: "BLPOP", setup: []command{{"RPUSH", "k", "a"}}, write: []command{{"BLPOP", "k"}}, invalidates: true},
		{name: "BLPOP of a missing key inside a transaction", write: []command{{"MULTI"}, {"BLPOP", "k"}, {"EXEC"}}},
		{name: "ZADD", write: []command{{"ZADD", "k", "1", "a"}}, invalidates: true},
		{name: "ZADD of a member with the score it has", setup: []command{{"ZADD", "k", "1", "a"}}, write: []command{{"ZADD", "k", "1", "a"}}, invalidates: true},
		{name: "GEOADD", write: []command{{"GEOADD", "k", "13.361389", "38.115556", "Palermo"}}, invalidates: true},
		{name: "XADD", write: []command{{"XADD", "k", "*", "field", "value"}}, invalidates: true},
		{name: "HSET", write: []command{{"HSET", "k", "f", "v"}}, invalidates: true},
		{name: "HSET of the value the field holds", setup: []command{{"HSET", "k", "f", "v"}}, write: []command{{"HSET", "k", "f", "v"}}, invalidates: true},
		{name: "HDEL", setup: []command{{"HSET", "k", "f", "v"}}, write: []command{{"HDEL", "k", "f"}}, invalidates: true},
		{name: "HDEL of a missing field", setup: []command{{"HSET", "k", "f", "v"}}, write: []command{{"HDEL", "k", "other"}}},
		{name: "HDEL of a missing key", write: []command{{"HDEL", "k", "f"}}},
		{name: "SADD", write: []command{{"SADD", "k", "a"}}, invalidates: true},
		{name: "SADD of a member the set holds", setup: []command{{"SADD", "k", "a"}}, write: []command{{"SADD", "k", "a"}}, invalidates: true},
		{name: "SREM", setup: []command{{"SADD", "k", "a"}}, write: []command{{"SREM", "k", "a"}}, invalidates: true},
		{name: "SREM of a missing member", setup: []command{{"SADD", "k", "a"}}, write: []command{{"SREM", "k", "other"}}},
		{name: "SREM of a missing key", write: []command{{"SREM", "k", "a"}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			executor := newTestExecutor()
			watcher := withClientStateForExecutor(context.Background(), executor, 1)
			writer := withClientStateForExecutor(context.Background(), executor, 2)
			run := func(ctx context.Context, commands ...command) protocol.Value {
				t.Helper()
				var reply protocol.Value
				for _, c := range commands {
					var err error
					if reply, err = executor.Execute(ctx, requestValue(c...)); err != nil {
						t.Fatalf("%v: %v", c, err)
					}
				}
				return reply
			}

			run(writer, tt.setup...)
			run(watcher, command{"WATCH", "k"}, command{"MULTI"}, command{"PING"})
			run(writer, tt.write...)

			reply, ok := run(watcher, command{"EXEC"}).(protocol.Array)
			if !ok {
				t.Fatal("EXEC did not answer an array")
			}
			if invalidated := reply.Null; invalidated != tt.invalidates {
				t.Fatalf("WATCH invalidated = %v, want %v", invalidated, tt.invalidates)
			}
		})
	}
}
