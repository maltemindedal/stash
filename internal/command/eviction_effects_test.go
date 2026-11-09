package command

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/record"
	"github.com/maltemindedal/stash/internal/storage"
)

// mustRun runs commands for the client of ctx and fails the test on an error.
func mustRun(t *testing.T, ctx context.Context, executor *Executor, commands ...[]string) protocol.Value {
	t.Helper()
	var reply protocol.Value
	for _, command := range commands {
		var err error
		if reply, err = executor.Execute(ctx, requestValue(command...)); err != nil {
			t.Fatalf("%v: %v", command, err)
		}
	}
	return reply
}

// storeExpiredKey stores key under maxmemory with a TTL deadline and returns
// once the deadline has passed, so that the sweep of the next accounted write
// removes it.
func storeExpiredKey(t *testing.T, ctx context.Context, executor *Executor, key string) {
	t.Helper()
	executor.store.ConfigureMaxMemory(nil, 1<<20, 0)
	expiresAt := time.Now().UnixMilli() + 1
	mustRun(t, ctx, executor, []string{"SET", key, "v", "PXAT", strconv.FormatInt(expiresAt, 10)})
	waitFor(t, "the key's TTL to pass", func() bool { return time.Now().UnixMilli() > expiresAt+1 })
}

// storeKeyTheNextWritesEvict stores victim and sets maxmemory so that writing
// every key of fitting afterwards needs exactly the memory victim holds: the
// write that no longer fits evicts victim, the least recently used key.
func storeKeyTheNextWritesEvict(t *testing.T, ctx context.Context, executor *Executor, victim string, fitting map[string]string) {
	t.Helper()
	store := executor.store
	store.ConfigureMaxMemory(nil, 1<<20, 0)
	mustRun(t, ctx, executor, []string{"SET", victim, strings.Repeat("v", 200)})
	usedByVictim := store.UsedMemory()
	written := time.Now().UnixMilli()
	waitFor(t, "a later clock reading than the victim's", func() bool { return time.Now().UnixMilli() > written })

	scratch := storage.NewStore()
	scratch.ConfigureMaxMemory(nil, 1<<20, 0)
	for key, value := range fitting {
		if err := scratch.Set(nil, key, []byte(value), 0); err != nil {
			t.Fatal(err)
		}
	}
	store.ConfigureMaxMemory(nil, usedByVictim+scratch.UsedMemory()-1, 0)
}

func TestBothKindsOfEvictionInvalidateWatch(t *testing.T) {
	tests := []struct {
		name string
		// evictOnNextWrite stores victim so that SET writer value removes it.
		evictOnNextWrite func(t *testing.T, ctx context.Context, executor *Executor)
	}{
		{
			name: "a memory-pressure eviction",
			evictOnNextWrite: func(t *testing.T, ctx context.Context, executor *Executor) {
				storeKeyTheNextWritesEvict(t, ctx, executor, "victim", map[string]string{"writer": "value"})
			},
		},
		{
			name: "a TTL eviction by the sweep of an accounted write",
			evictOnNextWrite: func(t *testing.T, ctx context.Context, executor *Executor) {
				storeExpiredKey(t, ctx, executor, "victim")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			executor := newTestExecutor()
			watcher := withClientStateForExecutor(context.Background(), executor, 1)
			writer := withClientStateForExecutor(context.Background(), executor, 2)

			tt.evictOnNextWrite(t, writer, executor)
			mustRun(t, watcher, executor, []string{"WATCH", "victim"}, []string{"MULTI"}, []string{"PING"})
			mustRun(t, writer, executor, []string{"SET", "writer", "value"})

			if _, found, _ := executor.store.Get("victim"); found {
				t.Fatal("the write did not remove the victim")
			}
			reply, ok := mustRun(t, watcher, executor, []string{"EXEC"}).(protocol.Array)
			if !ok || !reply.Null {
				t.Fatalf("EXEC = %#v, want a null array: the Eviction changed a watched key", reply)
			}
		})
	}
}

func TestAnAccountedWriteRecordsTheKeysItsSweepRemovedAfterItsOwnFrame(t *testing.T) {
	executor := newTestExecutor()
	ctx := clientContext(executor)
	storeExpiredKey(t, ctx, executor, "stale")

	result, err := handle(ctx, executor, requestValue("SET", "k", "v"), false)
	if err != nil {
		t.Fatal(err)
	}

	for _, to := range []record.Dest{record.ToAOF, record.ToReplicas} {
		assertPropagationFrames(t, result.Write.Frames(to), requestValue("SET", "k", "v"), requestValue("DEL", "stale"))
	}
}

func TestAWriteRefusedForMemoryStillRecordsTheKeysItsSweepRemoved(t *testing.T) {
	executor := newTestExecutor()
	ctx := clientContext(executor)
	storeExpiredKey(t, ctx, executor, "stale")

	result, err := handle(ctx, executor, requestValue("SET", "k", strings.Repeat("v", 2<<20)), false)
	if !errors.Is(err, ErrOutOfMemory) {
		t.Fatalf("SET of a value over the limit error = %v, want ErrOutOfMemory", err)
	}

	if _, found, _ := executor.store.Get("stale"); found {
		t.Fatal("the refused write did not sweep the expired key")
	}
	for _, to := range []record.Dest{record.ToAOF, record.ToReplicas} {
		assertPropagationFrames(t, result.Write.Frames(to), requestValue("DEL", "stale"))
	}
}

func TestAnEvictionInsideExecIsLoggedAfterTheCommandsBeforeIt(t *testing.T) {
	executor := newTestExecutor()
	ctx := clientContext(executor)
	big := strings.Repeat("w", 100)
	storeKeyTheNextWritesEvict(t, ctx, executor, "victim", map[string]string{"first": "1", "writer": big})

	mustRun(t, ctx, executor, []string{"MULTI"}, []string{"SET", "first", "1"}, []string{"SET", "writer", big}, []string{"SET", "last", "1"})
	result, err := handle(ctx, executor, requestValue("EXEC"), false)
	if err != nil {
		t.Fatal(err)
	}

	assertPropagationFrames(t, result.Write.Frames(record.ToBoth),
		requestValue("SET", "first", "1"),
		requestValue("SET", "writer", big),
		requestValue("DEL", "victim"),
		requestValue("SET", "last", "1"),
	)
}
