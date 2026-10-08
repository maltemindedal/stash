package aof

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/storage"
)

var (
	// errDiskFull is what a faultyFile's failing write returns, standing in for
	// ENOSPC.
	errDiskFull = errors.New("no space left on device")
	// errTruncate is what a faultyFile's failing truncate returns.
	errTruncate = errors.New("truncate refused")
)

// faultyFile sits between a Writer and its append-only file and fails on demand,
// the way a disk that is full for a moment does.
type faultyFile struct {
	*os.File
	// failWrites is how many writes, from the next one, fail.
	failWrites int
	// short makes each failing write put the first half of its bytes in the file
	// before it fails, as a write that runs out of space part way through does.
	short bool
	// failTruncates is how many truncates, from the next one, fail.
	failTruncates int
}

func (f *faultyFile) Write(p []byte) (int, error) {
	if f.failWrites == 0 {
		return f.File.Write(p)
	}
	f.failWrites--
	if !f.short {
		return 0, errDiskFull
	}
	n, err := f.File.Write(p[:len(p)/2])
	if err != nil {
		return n, err
	}
	return n, errDiskFull
}

func (f *faultyFile) Truncate(size int64) error {
	if f.failTruncates > 0 {
		f.failTruncates--
		return errTruncate
	}
	return f.File.Truncate(size)
}

// injectFaults puts f between w and its file, from w's next write on.
func injectFaults(w *Writer, f *faultyFile) {
	w.mu.Lock()
	defer w.mu.Unlock()
	f.File = w.file.(*os.File)
	w.file = f
}

// openTestWriter opens a Writer on a new file in a temporary directory. Its
// context is canceled, so the everysec ticker stops at once: the test runs the
// tick itself, through sync, exactly when it means to.
func openTestWriter(t *testing.T, policy Policy, logger *slog.Logger) (*Writer, string) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	w, err := OpenWriter(ctx, path, policy, logger)
	if err != nil {
		t.Fatalf("OpenWriter() error = %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })
	return w, path
}

// appendAndTick appends payload the way the server does under w's policy and,
// under everysec, then runs the tick that writes and fsyncs it.
func appendAndTick(w *Writer, payload []byte) error {
	switch w.Policy() {
	case PolicyAlways:
		return w.AppendSync(payload)
	case PolicyEverysec:
		if err := w.Append(payload); err != nil {
			return err
		}
		return w.sync()
	default:
		return w.Append(payload)
	}
}

// setCommand encodes SET key value as the append-only file stores it.
func setCommand(key, value string) []byte {
	return []byte(fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(value), value))
}

// replayFile loads path and returns each command it replays, with its arguments
// joined by spaces. LoadFile fails on an unfinished command before the end of the
// file, so a torn command anywhere but the end fails the test here.
func replayFile(t *testing.T, path string) ([]string, LoadStats) {
	t.Helper()

	var commands []string
	stats, err := LoadFile(context.Background(), path, func(_ context.Context, value protocol.Value) error {
		array, ok := value.(protocol.Array)
		if !ok {
			return fmt.Errorf("replayed a %T, want an array", value)
		}
		args := make([]string, 0, len(array.Elements))
		for _, element := range array.Elements {
			bulk, ok := element.(protocol.BulkString)
			if !ok {
				return fmt.Errorf("replayed an argument of type %T, want a bulk string", element)
			}
			args = append(args, string(bulk.Data))
		}
		commands = append(commands, strings.Join(args, " "))
		return nil
	})
	if err != nil {
		t.Fatalf("LoadFile() error = %v", err)
	}
	return commands, stats
}

// messageHandler is a slog.Handler that sends the message of each record to its
// channel, and drops the message when the channel is full.
type messageHandler chan string

func (h messageHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h messageHandler) Handle(_ context.Context, record slog.Record) error {
	select {
	case h <- record.Message:
	default:
	}
	return nil
}

func (h messageHandler) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h messageHandler) WithGroup(string) slog.Handler { return h }

var everyPolicy = []Policy{PolicyAlways, PolicyEverysec, PolicyNo}

// TestWriterResumesAppendingAfterAFailedWrite covers a disk that is full for a
// moment. The writer wrote through a bufio.Writer, whose error is sticky, so after
// one failed write no command reached the file again until a restart, and the
// command whose write failed was lost although the server had applied it.
func TestWriterResumesAppendingAfterAFailedWrite(t *testing.T) {
	for _, policy := range everyPolicy {
		t.Run(policy.String(), func(t *testing.T) {
			w, path := openTestWriter(t, policy, nil)
			if err := appendAndTick(w, setCommand("a", "1")); err != nil {
				t.Fatalf("append before the failure: %v", err)
			}

			injectFaults(w, &faultyFile{failWrites: 1})
			if err := appendAndTick(w, setCommand("b", "2")); !errors.Is(err, errDiskFull) {
				t.Fatalf("append on a full disk: error = %v, want %v", err, errDiskFull)
			}
			if w.LastWriteOK() {
				t.Fatal("LastWriteOK() = true after a failed write")
			}

			for _, payload := range [][]byte{setCommand("c", "3"), setCommand("d", "4")} {
				if err := appendAndTick(w, payload); err != nil {
					t.Fatalf("append once the disk has room again: %v", err)
				}
			}
			if !w.LastWriteOK() {
				t.Fatal("LastWriteOK() = false after a write succeeded again")
			}
			if err := w.Close(); err != nil {
				t.Fatalf("Close() error = %v", err)
			}

			commands, _ := replayFile(t, path)
			if want := []string{"SET a 1", "SET b 2", "SET c 3", "SET d 4"}; !slices.Equal(commands, want) {
				t.Fatalf("replayed %q, want %q: every applied command, in order, the failed one included", commands, want)
			}
		})
	}
}

// TestWriterRetriesAShortWriteWithoutLeavingATornCommand covers a write that runs
// out of space part way through a command. The part that reached the file has to
// be cut off before the command is written again: the loader accepts an
// unfinished command only at the very end of the file, and refuses to start on
// one with more commands after it.
func TestWriterRetriesAShortWriteWithoutLeavingATornCommand(t *testing.T) {
	for _, policy := range everyPolicy {
		t.Run(policy.String(), func(t *testing.T) {
			w, path := openTestWriter(t, policy, nil)
			payloads := [][]byte{setCommand("a", "1"), setCommand("b", "2"), setCommand("c", "3")}
			if err := appendAndTick(w, payloads[0]); err != nil {
				t.Fatalf("append before the failure: %v", err)
			}

			injectFaults(w, &faultyFile{failWrites: 1, short: true})
			if err := appendAndTick(w, payloads[1]); !errors.Is(err, errDiskFull) {
				t.Fatalf("append that runs out of space: error = %v, want %v", err, errDiskFull)
			}
			if err := appendAndTick(w, payloads[2]); err != nil {
				t.Fatalf("append once the disk has room again: %v", err)
			}
			if err := w.Close(); err != nil {
				t.Fatalf("Close() error = %v", err)
			}

			got, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("ReadFile() error = %v", err)
			}
			if want := bytes.Join(payloads, nil); !bytes.Equal(got, want) {
				t.Fatalf("file = %q, want %q", got, want)
			}
			commands, stats := replayFile(t, path)
			if want := []string{"SET a 1", "SET b 2", "SET c 3"}; !slices.Equal(commands, want) || stats.TornTail {
				t.Fatalf("replayed %q (torn tail %v), want %q and no torn tail", commands, stats.TornTail, want)
			}
		})
	}
}

// TestWriterKeepsFailingWhileItCannotCutOffATornCommand covers a short write that
// cannot be undone. A command written after the torn one would turn it into
// corruption before the end of the file, which stops the next startup, so every
// flush fails without writing, and the torn command stays the last thing in the
// file, until a truncate succeeds.
func TestWriterKeepsFailingWhileItCannotCutOffATornCommand(t *testing.T) {
	for _, policy := range everyPolicy {
		t.Run(policy.String(), func(t *testing.T) {
			w, path := openTestWriter(t, policy, nil)
			if err := appendAndTick(w, setCommand("a", "1")); err != nil {
				t.Fatalf("append before the failure: %v", err)
			}

			injectFaults(w, &faultyFile{failWrites: 1, short: true, failTruncates: 2})
			if err := appendAndTick(w, setCommand("b", "2")); !errors.Is(err, errDiskFull) {
				t.Fatalf("append that runs out of space: error = %v, want %v", err, errDiskFull)
			}
			for _, payload := range [][]byte{setCommand("c", "3"), setCommand("d", "4")} {
				if err := appendAndTick(w, payload); !errors.Is(err, errTruncate) {
					t.Fatalf("append while the torn command cannot be cut off: error = %v, want %v", err, errTruncate)
				}
			}
			if w.LastWriteOK() {
				t.Fatal("LastWriteOK() = true while the torn command cannot be cut off")
			}
			commands, stats := replayFile(t, path)
			if want := []string{"SET a 1"}; !slices.Equal(commands, want) || !stats.TornTail {
				t.Fatalf("replayed %q (torn tail %v) while failing, want %q and then the torn command", commands, stats.TornTail, want)
			}

			if err := appendAndTick(w, setCommand("e", "5")); err != nil {
				t.Fatalf("append once the truncate works: %v", err)
			}
			if !w.LastWriteOK() {
				t.Fatal("LastWriteOK() = false after a write succeeded again")
			}
			if err := w.Close(); err != nil {
				t.Fatalf("Close() error = %v", err)
			}
			commands, stats = replayFile(t, path)
			if want := []string{"SET a 1", "SET b 2", "SET c 3", "SET d 4", "SET e 5"}; !slices.Equal(commands, want) || stats.TornTail {
				t.Fatalf("replayed %q (torn tail %v), want %q and no torn tail", commands, stats.TornTail, want)
			}
		})
	}
}

// TestRewriteCompletesWithTheWritesThatFailedWhileItRan covers BGREWRITEAOF while
// the old file refuses writes. The swap flushed the old file first, got its stale
// error back and gave up. And a command whose write failed never reached the
// rewrite buffer, so even a rewrite that completed would have lost it.
func TestRewriteCompletesWithTheWritesThatFailedWhileItRan(t *testing.T) {
	for _, policy := range everyPolicy {
		t.Run(policy.String(), func(t *testing.T) {
			logged := make(messageHandler, 16)
			w, path := openTestWriter(t, policy, slog.New(logged))
			store := storage.NewStore()
			// Two writes to one key: the rewritten file holds only the second.
			for _, value := range []string{"0", "1"} {
				if _, err := store.Set("a", []byte(value), 0); err != nil {
					t.Fatalf("Set() error = %v", err)
				}
				if err := appendAndTick(w, setCommand("a", value)); err != nil {
					t.Fatalf("append before the rewrite: %v", err)
				}
			}

			// The guard is released once the rewrite has its snapshot and buffers new
			// commands, and before it writes the new file: the commands appended here
			// are the ones it has to carry over, and the old file refuses them all.
			appendErrs := make(chan error, 2)
			w.SetRewriteGuard(func() func() {
				return func() {
					injectFaults(w, &faultyFile{failWrites: math.MaxInt})
					for _, kv := range [][2]string{{"b", "2"}, {"c", "3"}} {
						if _, err := store.Set(kv[0], []byte(kv[1]), 0); err != nil {
							appendErrs <- err
							continue
						}
						appendErrs <- appendAndTick(w, setCommand(kv[0], kv[1]))
					}
				}
			})
			if err := w.BeginRewrite(context.Background(), store); err != nil {
				t.Fatalf("BeginRewrite() error = %v", err)
			}
			if message := <-logged; message != "completed append-only file rewrite" {
				t.Fatalf("rewrite logged %q, want it to complete", message)
			}
			for i := 0; i < 2; i++ {
				if err := <-appendErrs; !errors.Is(err, errDiskFull) {
					t.Fatalf("append while the old file refuses writes: error = %v, want %v", err, errDiskFull)
				}
			}

			if err := appendAndTick(w, setCommand("d", "4")); err != nil {
				t.Fatalf("append to the rewritten file: %v", err)
			}
			if !w.LastWriteOK() {
				t.Fatal("LastWriteOK() = false after a write to the rewritten file succeeded")
			}
			if err := w.Close(); err != nil {
				t.Fatalf("Close() error = %v", err)
			}
			commands, _ := replayFile(t, path)
			if want := []string{"SET a 1", "SET b 2", "SET c 3", "SET d 4"}; !slices.Equal(commands, want) {
				t.Fatalf("replayed %q, want %q: the snapshot, then every command appended during and after the rewrite", commands, want)
			}
		})
	}
}
