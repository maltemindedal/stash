package command

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/record"
	"github.com/maltemindedal/stash/internal/server"
	"github.com/maltemindedal/stash/internal/storage"
)

// newLoggingExecutor builds an executor that records to log.
func newLoggingExecutor(log record.Log) *Executor {
	return newTestExecutorWith(func(s *server.Services) {
		s.Recorder = record.New(s.Master, s.Logger)
		s.Recorder.SetLog(log)
	})
}

// TestAnActiveTTLEvictionIsRecordedAsADelAndInvalidatesWatch runs the store's
// expiry loop through the executor's Background unit, as the server wires it.
func TestAnActiveTTLEvictionIsRecordedAsADelAndInvalidatesWatch(t *testing.T) {
	log := newHoldingLog()
	executor := newLoggingExecutor(log)
	writer := withClientStateForExecutor(context.Background(), executor, 1)
	watcher := withClientStateForExecutor(context.Background(), executor, 2)

	expiresAt := time.Now().UnixMilli() + 20
	mustRun(t, writer, executor, []string{"SET", "k", "v", "PXAT", strconv.FormatInt(expiresAt, 10)})
	mustRun(t, watcher, executor, []string{"WATCH", "k"}, []string{"MULTI"}, []string{"PING"})

	executor.store.SetBackgroundRunner(func(work func(changes storage.Changes)) {
		if err := executor.RunBackgroundUnit(work); err != nil {
			t.Error(err)
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := executor.store.StartEviction(ctx, 5*time.Millisecond, 10)
	defer func() {
		cancel()
		<-done
	}()

	waitFor(t, "the sweep's DEL to reach the log", func() bool {
		return bytes.Contains(log.Bytes(), []byte("DEL"))
	})
	commands := loggedCommands(t, log)
	if last := commands[len(commands)-1]; last.Name != "DEL" || len(last.Args) != 1 || string(last.Args[0]) != "k" {
		t.Fatalf("the log ends with %s, want DEL k", listCommands(commands))
	}
	reply, ok := mustRun(t, watcher, executor, []string{"EXEC"}).(protocol.Array)
	if !ok || !reply.Null {
		t.Fatalf("EXEC = %#v, want a null array: the TTL eviction changed a watched key", reply)
	}
}

// TestABackgroundUnitWaitsForAWriteThatIsHalfwayToTheLog holds a client's SET
// between the store and the log. A Background unit that removed the same key
// then must not be recorded first: replay would run the DEL before the SET and
// bring the key back.
func TestABackgroundUnitWaitsForAWriteThatIsHalfwayToTheLog(t *testing.T) {
	log := newHoldingLog()
	log.hold = []byte("SET")
	executor := newLoggingExecutor(log)

	setting := handleInBackground(t, executor, requestValue("SET", "k", "v"))
	waitFor(t, "the SET's frame to be held on its way to the log", isClosed(log.held))

	stillBlocked, finished := blocked(func() {
		err := executor.RunBackgroundUnit(func(changes storage.Changes) {
			if executor.store.Delete(nil, "k") {
				changes.KeyChanged("k", storage.Expired)
			}
		})
		if err != nil {
			t.Error(err)
		}
	})
	if !stillBlocked {
		t.Fatal("the Background unit ran while a write was halfway to the log")
	}
	close(log.released)
	waitFor(t, "the SET to return", isClosed(setting))
	if !finished() {
		t.Fatal("the Background unit never ran")
	}

	if commands := loggedCommands(t, log); liveAfterReplay(commands, []byte("k")) {
		t.Fatalf("replaying the log brings back k, which the Background unit removed:%s", listCommands(commands))
	}
}

func TestABackgroundUnitReturnsTheErrorOfAnAppendThatFailed(t *testing.T) {
	log := newHoldingLog()
	executor := newLoggingExecutor(log)
	diskFull := errors.New("no space left on device")
	log.Fail(diskFull, true)

	err := executor.RunBackgroundUnit(func(changes storage.Changes) {
		changes.KeyChanged("k", storage.MemoryEvicted)
	})
	if !errors.Is(err, diskFull) {
		t.Fatalf("RunBackgroundUnit() error = %v, want the append's", err)
	}

	if err := executor.RunBackgroundUnit(func(storage.Changes) {}); err != nil {
		t.Fatalf("RunBackgroundUnit() of work that removed nothing error = %v, want nil", err)
	}
}
