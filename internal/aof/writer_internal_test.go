package aof

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/storage"
)

var (
	errDiskFull      = errors.New("no space left on device")
	errTruncateFails = errors.New("truncate failed")
	errSyncFails     = errors.New("input/output error")
	errCloseFails    = errors.New("close failed")
	errSwapStep      = errors.New("swap step failed")
)

// faultyFile stands in for the append-only file the way a disk that fills up
// and then has room again does. While failing is set every write fails, after
// writing the first half of what it was given when partial is set. With
// writeLimit set, a write of more bytes writes that many and fails. While
// failSync is set Sync fails; syncs counts the calls. With failClose set, Close closes the file, cuts
// it to sizeAfterClose as if the kernel had dropped writes it could not finish,
// and fails. The writer calls it under its lock, so the test changes the fields
// only through setFaults.
type faultyFile struct {
	*os.File
	failing        bool
	partial        bool
	writeLimit     int
	failSync       bool
	syncs          int
	failClose      bool
	sizeAfterClose int64
}

func (f *faultyFile) Write(p []byte) (int, error) {
	switch {
	case f.failing:
		n := 0
		if f.partial {
			n, _ = f.File.Write(p[:len(p)/2])
		}
		return n, errDiskFull
	case f.writeLimit > 0 && len(p) > f.writeLimit:
		n, _ := f.File.Write(p[:f.writeLimit])
		return n, errDiskFull
	default:
		return f.File.Write(p)
	}
}

func (f *faultyFile) Sync() error {
	f.syncs++
	if f.failSync {
		return errSyncFails
	}
	return f.File.Sync()
}

func (f *faultyFile) Close() error {
	err := f.File.Close()
	if !f.failClose {
		return err
	}
	if truncateErr := os.Truncate(f.Name(), f.sizeAfterClose); truncateErr != nil {
		return truncateErr
	}
	return errCloseFails
}

// openFaultyWriter opens a writer on a new append-only file and puts a
// faultyFile, working for now, between the writer and that file.
func openFaultyWriter(t *testing.T, policy Policy) (*Writer, *faultyFile, string) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "appendonly.aof")
	writer, err := OpenWriter(context.Background(), path, policy, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("OpenWriter() error = %v", err)
	}
	t.Cleanup(func() { _ = writer.Close() })

	writer.mu.Lock()
	defer writer.mu.Unlock()
	file, ok := writer.file.(*os.File)
	if !ok {
		t.Fatalf("writer.file is %T, want *os.File", writer.file)
	}
	faulty := &faultyFile{File: file}
	writer.file = faulty
	return writer, faulty, path
}

// setFaults changes what the faulty file does, under the writer's lock: the
// everysec goroutine writes through it too.
func setFaults(writer *Writer, change func()) {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	change()
}

// fakeSeam sets one of the package's seams (cutFile, syncDir, renameFile) to
// fake, and returns a function that restores it. The writer reads them under its
// lock, from the everysec goroutine too, so both happen under that lock.
func fakeSeam[T any](writer *Writer, seam *T, fake T) (restore func()) {
	var original T
	setFaults(writer, func() { original, *seam = *seam, fake })
	return func() { setFaults(writer, func() { *seam = original }) }
}

func setCommand(key, value string) []byte {
	return []byte(fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(value), value))
}

// appendAndWrite appends payload the way the server does under the writer's
// policy (with an fsync under always) and, under everysec, runs the sync the
// background goroutine runs once a second. When it returns, every policy has
// tried to write payload to the file.
func appendAndWrite(writer *Writer, payload []byte) error {
	switch writer.policy {
	case PolicyAlways:
		return writer.AppendSync(payload)
	case PolicyEverysec:
		if err := writer.Append(payload); err != nil {
			return err
		}
		return writer.sync()
	default:
		return writer.Append(payload)
	}
}

func mustAppend(t *testing.T, writer *Writer, payloads ...[]byte) {
	t.Helper()

	for _, payload := range payloads {
		if err := appendAndWrite(writer, payload); err != nil {
			t.Fatalf("append %q: %v", payload, err)
		}
	}
}

// rewriteFile creates the temp file a rewrite would have written, holding
// commands, next to path.
func rewriteFile(t *testing.T, path string, commands ...[]byte) *os.File {
	t.Helper()

	tempFile, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".rewrite-*")
	if err != nil {
		t.Fatalf("CreateTemp() error = %v", err)
	}
	for _, command := range commands {
		if _, err := tempFile.Write(command); err != nil {
			t.Fatalf("write the rewrite: %v", err)
		}
	}
	return tempFile
}

// loadCommands replays the append-only file at path and returns its commands,
// each as its arguments joined by spaces.
func loadCommands(t *testing.T, path string) ([]string, LoadStats) {
	t.Helper()

	var commands []string
	stats, err := LoadFile(context.Background(), path, func(_ context.Context, value protocol.Value) error {
		array, ok := value.(protocol.Array)
		if !ok {
			return fmt.Errorf("replayed %T, want an array", value)
		}
		args := make([]string, 0, len(array.Elements))
		for _, element := range array.Elements {
			bulk, ok := element.(protocol.BulkString)
			if !ok {
				return fmt.Errorf("replayed argument %T, want a bulk string", element)
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

// wantReplay closes the writer and checks that the file at path replays want,
// with no torn tail.
func wantReplay(t *testing.T, writer *Writer, path string, want ...string) {
	t.Helper()

	if err := writer.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	got, stats := loadCommands(t, path)
	if !reflect.DeepEqual(got, want) || stats.TornTail {
		t.Fatalf("replayed %q (torn tail %v), want %q with no torn tail", got, stats.TornTail, want)
	}
}

// keptUnder returns commands without the failed ones under always. There the
// server appends with AppendSync, which leaves a command it could not make
// durable out of the file, as the client got an error and the replicas never
// got the command. The other policies keep it and write it later.
func keptUnder(policy Policy, commands []string, failed ...string) []string {
	if policy != PolicyAlways {
		return commands
	}
	kept := make([]string, 0, len(commands))
	for _, command := range commands {
		isFailed := false
		for _, f := range failed {
			isFailed = isFailed || command == f
		}
		if !isFailed {
			kept = append(kept, command)
		}
	}
	return kept
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat(%q) error = %v", path, err)
	}
	return info.Size()
}

var allPolicies = []Policy{PolicyAlways, PolicyEverysec, PolicyNo}

// TestWriterResumesAppendingAfterAFailedWrite covers a disk that is full for a
// moment. The writer wrote through a bufio.Writer whose error is sticky, so
// after one failed write every later append failed without touching the file,
// under every policy, until a restart.
func TestWriterResumesAppendingAfterAFailedWrite(t *testing.T) {
	for _, policy := range allPolicies {
		t.Run(policy.String(), func(t *testing.T) {
			writer, faulty, path := openFaultyWriter(t, policy)
			mustAppend(t, writer, setCommand("a", "1"))

			setFaults(writer, func() { faulty.failing = true })
			if err := appendAndWrite(writer, setCommand("b", "2")); !errors.Is(err, errDiskFull) {
				t.Fatalf("append on a full disk: error = %v, want it to wrap %v", err, errDiskFull)
			}
			if writer.LastWriteOK() {
				t.Fatal("LastWriteOK() = true after a failed write")
			}

			setFaults(writer, func() { faulty.failing = false })
			mustAppend(t, writer, setCommand("c", "0"), setCommand("c", "1"), setCommand("c", "2"))
			if !writer.LastWriteOK() {
				t.Fatal("LastWriteOK() = false after later writes succeeded")
			}
			wantReplay(t, writer, path, keptUnder(policy, []string{"SET a 1", "SET b 2", "SET c 0", "SET c 1", "SET c 2"}, "SET b 2")...)
		})
	}
}

// TestWriterCutsAPartlyWrittenCommandBeforeRetrying covers a write that runs out
// of space part way through a command. The loader accepts an unfinished command
// only at the end of the file, so writing anything after the part that reached
// the disk would make the file refuse to load.
func TestWriterCutsAPartlyWrittenCommandBeforeRetrying(t *testing.T) {
	for _, policy := range allPolicies {
		t.Run(policy.String(), func(t *testing.T) {
			writer, faulty, path := openFaultyWriter(t, policy)
			mustAppend(t, writer, setCommand("a", "1"))
			before := fileSize(t, path)

			setFaults(writer, func() { faulty.failing, faulty.partial = true, true })
			if err := appendAndWrite(writer, setCommand("b", "2")); !errors.Is(err, errDiskFull) {
				t.Fatalf("append on a full disk: error = %v, want it to wrap %v", err, errDiskFull)
			}
			// The failed write leaves part of b in the file, which AppendSync cuts
			// back at once and the other policies cut before their next write.
			if size := fileSize(t, path); (size == before) != (policy == PolicyAlways) {
				t.Fatalf("file is %d bytes after the failed write, %d before it: want part of a command there, except under always", size, before)
			}

			setFaults(writer, func() { faulty.failing = false })
			mustAppend(t, writer, setCommand("c", "3"))
			if !writer.LastWriteOK() {
				t.Fatal("LastWriteOK() = false after a later write succeeded")
			}
			wantReplay(t, writer, path, keptUnder(policy, []string{"SET a 1", "SET b 2", "SET c 3"}, "SET b 2")...)
		})
	}
}

// TestWriterKeepsFailingWhileItCannotCutAPartlyWrittenCommand pins what happens
// when the part of a command a failed write left behind cannot be cut off:
// nothing is appended after it, so the file still loads, and the status stays
// failed until the cut and the write succeed.
func TestWriterKeepsFailingWhileItCannotCutAPartlyWrittenCommand(t *testing.T) {
	for _, policy := range allPolicies {
		t.Run(policy.String(), func(t *testing.T) {
			writer, faulty, path := openFaultyWriter(t, policy)
			mustAppend(t, writer, setCommand("a", "1"))

			restore := fakeSeam(writer, &cutFile, func(string, int64) error { return errTruncateFails })
			setFaults(writer, func() { faulty.failing, faulty.partial = true, true })
			if err := appendAndWrite(writer, setCommand("b", "2")); !errors.Is(err, errDiskFull) {
				t.Fatalf("append on a full disk: error = %v, want it to wrap %v", err, errDiskFull)
			}
			torn := fileSize(t, path)

			setFaults(writer, func() { faulty.failing = false })
			if err := appendAndWrite(writer, setCommand("c", "3")); !errors.Is(err, errTruncateFails) {
				t.Fatalf("append while the cut fails: error = %v, want it to wrap %v", err, errTruncateFails)
			}
			if writer.LastWriteOK() {
				t.Fatal("LastWriteOK() = true while the partly written command cannot be cut off")
			}
			if size := fileSize(t, path); size != torn {
				t.Fatalf("file grew from %d to %d bytes after a partly written command", torn, size)
			}
			got, stats := loadCommands(t, path)
			if want := []string{"SET a 1"}; !reflect.DeepEqual(got, want) || !stats.TornTail {
				t.Fatalf("replayed %q (torn tail %v), want %q and the torn tail", got, stats.TornTail, want)
			}

			restore()
			mustAppend(t, writer, setCommand("d", "4"))
			if !writer.LastWriteOK() {
				t.Fatal("LastWriteOK() = false after the cut and the write succeeded")
			}
			wantReplay(t, writer, path, keptUnder(policy, []string{"SET a 1", "SET b 2", "SET c 3", "SET d 4"}, "SET b 2", "SET c 3")...)
		})
	}
}

// TestFailedFsyncKeepsTheStatusFailedUntilARewrite covers an fsync that fails
// once. On Linux the kernel may drop the pages it could not write and let the
// next fsync succeed without them, so a later success does not prove the file
// whole. The status went back to ok with the next successful fsync. Under no
// the writer fsyncs only on Close, so the policies here are the two that fsync;
// under always the command after the failure is refused, as
// TestAppendSyncRefusesPayloadsAfterAFailedFsyncUntilARewrite covers.
func TestFailedFsyncKeepsTheStatusFailedUntilARewrite(t *testing.T) {
	for _, policy := range []Policy{PolicyAlways, PolicyEverysec} {
		t.Run(policy.String(), func(t *testing.T) {
			writer, faulty, path := openFaultyWriter(t, policy)
			mustAppend(t, writer, setCommand("a", "1"))

			setFaults(writer, func() { faulty.failSync = true })
			if err := appendAndWrite(writer, setCommand("b", "2")); !errors.Is(err, errSyncFails) {
				t.Fatalf("append whose fsync fails: error = %v, want it to wrap %v", err, errSyncFails)
			}
			setFaults(writer, func() { faulty.failSync = false })
			if err := appendAndWrite(writer, setCommand("c", "3")); (err != nil) != (policy == PolicyAlways) {
				t.Fatalf("append after the failed fsync: error = %v, want one only under always", err)
			}
			if writer.LastWriteOK() {
				t.Fatal("LastWriteOK() = true after a failed fsync and a later successful one")
			}
			got, _ := loadCommands(t, path)
			if want := keptUnder(policy, []string{"SET a 1", "SET b 2", "SET c 3"}, "SET b 2", "SET c 3"); !reflect.DeepEqual(got, want) {
				t.Fatalf("before the rewrite the file replays %q, want %q", got, want)
			}

			tempFile := rewriteFile(t, path, setCommand("a", "1"), setCommand("b", "2"), setCommand("c", "3"))
			if completed, err := writer.finalizeRewrite(tempFile, tempFile.Name()); !completed || err != nil {
				t.Fatalf("finalizeRewrite() = (%v, %v), want (true, nil)", completed, err)
			}
			if !writer.LastWriteOK() {
				t.Fatal("LastWriteOK() = false after a rewrite replaced the file")
			}
			mustAppend(t, writer, setCommand("d", "4"))
			wantReplay(t, writer, path, "SET a 1", "SET b 2", "SET c 3", "SET d 4")
		})
	}
}

// TestRewriteKeepsTheCommandsAppendedWhileTheFileFails covers BGREWRITEAOF
// while the append-only file is failing. The swap flushed the old file's
// buffered writer first, got its stale error and abandoned the rewrite, and a
// failed append returned before copying the command into the rewrite buffer, so
// a rewrite that did complete would have lost it.
func TestRewriteKeepsTheCommandsAppendedWhileTheFileFails(t *testing.T) {
	for _, policy := range allPolicies {
		t.Run(policy.String(), func(t *testing.T) {
			writer, faulty, path := openFaultyWriter(t, policy)
			store := storage.NewStore()

			if _, err := store.Set("a", []byte("1"), 0); err != nil {
				t.Fatalf("Set(a) error = %v", err)
			}
			mustAppend(t, writer, setCommand("a", "1"))

			// Hold the rewrite between its snapshot and its swap.
			snapshotTaken := make(chan struct{})
			resume := make(chan struct{})
			var resumeOnce sync.Once
			resumeRewrite := func() { resumeOnce.Do(func() { close(resume) }) }
			t.Cleanup(resumeRewrite)
			writer.SetRewriteGuard(func() func() {
				return func() {
					close(snapshotTaken)
					<-resume
				}
			})
			if err := writer.BeginRewrite(context.Background(), store); err != nil {
				t.Fatalf("BeginRewrite() error = %v", err)
			}
			select {
			case <-snapshotTaken:
			case <-time.After(10 * time.Second):
				t.Fatal("the rewrite did not take its snapshot")
			}

			setFaults(writer, func() { faulty.failing = true })
			if _, err := store.Set("b", []byte("2"), 0); err != nil {
				t.Fatalf("Set(b) error = %v", err)
			}
			if err := appendAndWrite(writer, setCommand("b", "2")); !errors.Is(err, errDiskFull) {
				t.Fatalf("append on a full disk: error = %v, want it to wrap %v", err, errDiskFull)
			}

			resumeRewrite()
			deadline := time.Now().Add(10 * time.Second)
			for {
				writer.mu.Lock()
				done := !writer.rewritePending && !writer.rewriteActive
				writer.mu.Unlock()
				if done {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("the rewrite did not finish")
				}
				time.Sleep(time.Millisecond)
			}

			got, _ := loadCommands(t, path)
			if want := keptUnder(policy, []string{"SET a 1", "SET b 2"}, "SET b 2"); !reflect.DeepEqual(got, want) {
				t.Fatalf("after the rewrite the file replays %q, want the rewrite of the store and the command appended during the failure, %q", got, want)
			}
			if !writer.LastWriteOK() {
				t.Fatal("LastWriteOK() = false after the rewrite replaced the failing file")
			}
			mustAppend(t, writer, setCommand("c", "3"))
			wantReplay(t, writer, path, keptUnder(policy, []string{"SET a 1", "SET b 2", "SET c 3"}, "SET b 2")...)
		})
	}
}

// TestRewriteSwapFailureKeepsEveryCommandExactlyOnce covers a swap that fails
// while a command is still waiting for a failing file. If the rename fails, the
// old file stays, and the command must still reach it once the part of it a
// failed write left there is cut off. If only the directory sync after the
// rename fails, the rewritten file is in place and already holds the command,
// so writing it again, or cutting the new file at the old file's length, would
// replay the wrong commands.
func TestRewriteSwapFailureKeepsEveryCommandExactlyOnce(t *testing.T) {
	tests := []struct {
		name     string
		failStep func(writer *Writer) (restore func())
		want     []string
	}{
		{
			name: "the rename fails",
			failStep: func(writer *Writer) func() {
				return fakeSeam(writer, &renameFile, func(string, string) error { return errSwapStep })
			},
			want: []string{"SET a 0", "SET a 1", "SET b 2", "SET c 3"},
		},
		{
			name: "the directory sync after the rename fails",
			failStep: func(writer *Writer) func() {
				return fakeSeam(writer, &syncDir, func(string) error { return errSwapStep })
			},
			want: []string{"SET a 1", "SET b 2", "SET c 3"},
		},
	}

	for _, policy := range allPolicies {
		for _, tt := range tests {
			t.Run(policy.String()+"/"+tt.name, func(t *testing.T) {
				writer, faulty, path := openFaultyWriter(t, policy)
				mustAppend(t, writer, setCommand("a", "0"), setCommand("a", "1"))

				// The state the snapshot barrier leaves behind: a rewrite is active,
				// so appended commands are also copied into the rewrite buffer.
				setFaults(writer, func() {
					writer.rewriteActive = true
					faulty.failing, faulty.partial = true, true
				})
				if err := appendAndWrite(writer, setCommand("b", "2")); !errors.Is(err, errDiskFull) {
					t.Fatalf("append on a full disk: error = %v, want it to wrap %v", err, errDiskFull)
				}

				// The rewrite of the store at the snapshot, shorter than the old file.
				tempFile := rewriteFile(t, path, setCommand("a", "1"))
				restore := tt.failStep(writer)
				completed, err := writer.finalizeRewrite(tempFile, tempFile.Name())
				restore()
				if completed || !errors.Is(err, errSwapStep) {
					t.Fatalf("finalizeRewrite() = (%v, %v), want (false, an error wrapping %v)", completed, err, errSwapStep)
				}

				mustAppend(t, writer, setCommand("c", "3"))
				wantReplay(t, writer, path, keptUnder(policy, tt.want, "SET b 2")...)
			})
		}
	}
}

// TestRewriteSwapRetriesAnUnsyncedDirectoryBeforeReportingSuccess covers a
// rewrite renamed into place whose directory could not be synced: until it is,
// a crash can bring the old file back without the commands appended since. The
// swap reported the error once, never synced the directory again, and left the
// status ok.
func TestRewriteSwapRetriesAnUnsyncedDirectoryBeforeReportingSuccess(t *testing.T) {
	for _, policy := range allPolicies {
		t.Run(policy.String(), func(t *testing.T) {
			writer, _, path := openFaultyWriter(t, policy)
			mustAppend(t, writer, setCommand("a", "1"))

			dirErr := errSwapStep
			dirSyncs := 0
			restore := fakeSeam(writer, &syncDir, func(string) error {
				dirSyncs++
				return dirErr
			})
			defer restore()

			tempFile := rewriteFile(t, path, setCommand("a", "1"))
			if completed, err := writer.finalizeRewrite(tempFile, tempFile.Name()); completed || !errors.Is(err, errSwapStep) {
				t.Fatalf("finalizeRewrite() = (%v, %v), want (false, an error wrapping %v)", completed, err, errSwapStep)
			}
			if writer.LastWriteOK() {
				t.Fatal("LastWriteOK() = true while the rewrite's directory entry is not synced")
			}
			if err := appendAndWrite(writer, setCommand("b", "2")); !errors.Is(err, errSwapStep) {
				t.Fatalf("append while the directory cannot be synced: error = %v, want it to wrap %v", err, errSwapStep)
			}
			if writer.LastWriteOK() {
				t.Fatal("LastWriteOK() = true while the rewrite's directory entry is not synced")
			}

			var syncsBefore int
			setFaults(writer, func() { dirErr, syncsBefore = nil, dirSyncs })
			mustAppend(t, writer, setCommand("c", "3"))
			if !writer.LastWriteOK() {
				t.Fatal("LastWriteOK() = false after the directory was synced")
			}
			var syncsAfter int
			setFaults(writer, func() { syncsAfter = dirSyncs })
			if syncsAfter != syncsBefore+1 {
				t.Fatalf("directory syncs = %d after the append, want %d: one retry that succeeds", syncsAfter, syncsBefore+1)
			}
			mustAppend(t, writer, setCommand("d", "4"))
			setFaults(writer, func() { syncsAfter = dirSyncs })
			if syncsAfter != syncsBefore+1 {
				t.Fatalf("directory syncs = %d, want no more once one succeeded", syncsAfter)
			}
			wantReplay(t, writer, path, keptUnder(policy, []string{"SET a 1", "SET b 2", "SET c 3", "SET d 4"}, "SET b 2")...)
		})
	}
}

// TestRewriteSwapChecksAnOldFileThatFailedToCloseBeforeAppending covers a swap
// that could not close the old file and then could not rename the rewrite over
// it. A failed close reports writes the kernel could not finish, so the old file
// may be shorter than the writer counted, ending in part of a command.
// Appending to it put the next command after that part, in the middle of the
// file, which the loader refuses. The writer now finds the file short, appends
// nothing and keeps failing until a rewrite replaces the file.
func TestRewriteSwapChecksAnOldFileThatFailedToCloseBeforeAppending(t *testing.T) {
	for _, policy := range allPolicies {
		t.Run(policy.String(), func(t *testing.T) {
			writer, faulty, path := openFaultyWriter(t, policy)
			mustAppend(t, writer, setCommand("a", "0"), setCommand("a", "1"))

			// Closing loses the second half of the last command.
			lost := int64(len(setCommand("a", "1")) / 2)
			short := fileSize(t, path) - lost
			setFaults(writer, func() { faulty.failClose, faulty.sizeAfterClose = true, short })
			tempFile := rewriteFile(t, path, setCommand("a", "1"))
			restore := fakeSeam(writer, &renameFile, func(string, string) error { return errSwapStep })
			completed, err := writer.finalizeRewrite(tempFile, tempFile.Name())
			restore()
			if completed || !errors.Is(err, errSwapStep) {
				t.Fatalf("finalizeRewrite() = (%v, %v), want (false, an error wrapping %v)", completed, err, errSwapStep)
			}
			if writer.LastWriteOK() {
				t.Fatal("LastWriteOK() = true after the old file failed to close")
			}

			// The next rewrite's snapshot is taken before this command.
			setFaults(writer, func() { writer.rewriteActive = true })
			if err := appendAndWrite(writer, setCommand("c", "3")); err == nil {
				t.Fatal("append to an old file shorter than the writer counted: error = nil, want a failed cut")
			}
			if size := fileSize(t, path); size != short {
				t.Fatalf("file is %d bytes, want %d: nothing appended after the part of a command", size, short)
			}
			got, stats := loadCommands(t, path)
			if want := []string{"SET a 0"}; !reflect.DeepEqual(got, want) || !stats.TornTail {
				t.Fatalf("replayed %q (torn tail %v), want %q and the torn tail", got, stats.TornTail, want)
			}
			if writer.LastWriteOK() {
				t.Fatal("LastWriteOK() = true while the file is shorter than the writer counted")
			}

			tempFile = rewriteFile(t, path, setCommand("a", "1"))
			if completed, err := writer.finalizeRewrite(tempFile, tempFile.Name()); !completed || err != nil {
				t.Fatalf("finalizeRewrite() = (%v, %v), want (true, nil)", completed, err)
			}
			if !writer.LastWriteOK() {
				t.Fatal("LastWriteOK() = false after a rewrite replaced the file")
			}
			mustAppend(t, writer, setCommand("d", "4"))
			wantReplay(t, writer, path, keptUnder(policy, []string{"SET a 1", "SET c 3", "SET d 4"}, "SET c 3")...)
		})
	}
}

// TestWriterRetriesTheReopenARewriteSwapCouldNotDo covers a swap that renamed
// the rewrite into place and then could not open it. The writer kept the closed
// old file, so every later command failed until a restart. Here the reopen
// fails because a directory stands where the file should be, until the test
// puts the rewrite back.
func TestWriterRetriesTheReopenARewriteSwapCouldNotDo(t *testing.T) {
	for _, policy := range allPolicies {
		t.Run(policy.String(), func(t *testing.T) {
			writer, _, path := openFaultyWriter(t, policy)
			mustAppend(t, writer, setCommand("a", "0"))

			held := path + ".held"
			restore := fakeSeam(writer, &renameFile, func(from, to string) error {
				if err := os.Rename(from, held); err != nil {
					return err
				}
				if err := os.Remove(to); err != nil {
					return err
				}
				return os.Mkdir(to, 0o700)
			})
			tempFile := rewriteFile(t, path, setCommand("a", "1"))
			completed, err := writer.finalizeRewrite(tempFile, tempFile.Name())
			restore()
			if completed || err == nil {
				t.Fatalf("finalizeRewrite() = (%v, %v), want (false, a reopen error)", completed, err)
			}
			if writer.LastWriteOK() {
				t.Fatal("LastWriteOK() = true while the file cannot be opened")
			}
			if err := appendAndWrite(writer, setCommand("b", "2")); err == nil {
				t.Fatal("append while the file cannot be opened: error = nil")
			}

			if err := os.Remove(path); err != nil {
				t.Fatalf("remove the directory: %v", err)
			}
			if err := os.Rename(held, path); err != nil {
				t.Fatalf("put the rewrite back: %v", err)
			}
			mustAppend(t, writer, setCommand("c", "3"))
			if !writer.LastWriteOK() {
				t.Fatal("LastWriteOK() = false after the file was reopened and written")
			}
			wantReplay(t, writer, path, keptUnder(policy, []string{"SET a 1", "SET b 2", "SET c 3"}, "SET b 2")...)
		})
	}
}

// TestEverysecWritesCommandsOnceTheyFillTheFlushSize pins that under everysec
// Append writes collected commands to the file once they reach pendingFlushSize,
// without waiting for the once-a-second sync, and writes whole commands only.
func TestEverysecWritesCommandsOnceTheyFillTheFlushSize(t *testing.T) {
	// A canceled context stops the once-a-second sync, so only Append writes.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	writer, err := OpenWriter(ctx, path, PolicyEverysec, nil)
	if err != nil {
		t.Fatalf("OpenWriter() error = %v", err)
	}
	defer func() { _ = writer.Close() }()

	command := setCommand("key", strings.Repeat("v", 100))
	below := (pendingFlushSize - 1) / len(command)
	for i := 0; i < below; i++ {
		if err := writer.Append(command); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}
	if size := fileSize(t, path); size != 0 {
		t.Fatalf("file is %d bytes with %d bytes collected, want 0 below %d", size, below*len(command), pendingFlushSize)
	}
	if err := writer.Append(command); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if size, want := fileSize(t, path), int64((below+1)*len(command)); size != want {
		t.Fatalf("file is %d bytes once %d bytes were collected, want all %d", size, want, want)
	}
}

// TestWriterReleasesAPendingBufferGrownPastItsIdleCap pins that the buffer for
// pending commands is kept for reuse after a write, unless a large command or a
// long write failure grew it past maxIdlePendingCap.
func TestWriterReleasesAPendingBufferGrownPastItsIdleCap(t *testing.T) {
	tests := []struct {
		name     string
		valueLen int
		wantKept bool
	}{
		{name: "a small command", valueLen: 10, wantKept: true},
		{name: "a command larger than the cap", valueLen: maxIdlePendingCap + 1, wantKept: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			writer, _, path := openFaultyWriter(t, PolicyNo)
			command := setCommand("k", strings.Repeat("v", tt.valueLen))
			mustAppend(t, writer, command)

			var pendingLen, pendingCap int
			setFaults(writer, func() { pendingLen, pendingCap = len(writer.pending), cap(writer.pending) })
			if pendingLen != 0 {
				t.Fatalf("len(pending) = %d after a successful write, want 0", pendingLen)
			}
			if kept := pendingCap > 0; kept != tt.wantKept || pendingCap > maxIdlePendingCap {
				t.Fatalf("cap(pending) = %d after writing %d bytes, want it kept: %v, and at most %d", pendingCap, len(command), tt.wantKept, maxIdlePendingCap)
			}
			if size := fileSize(t, path); size != int64(len(command)) {
				t.Fatalf("file is %d bytes, want %d", size, len(command))
			}
		})
	}
}

// TestWriterKeepsTheWholeCommandsAFailedWriteGotIntoTheFile covers a backlog
// larger than one write can take. A failed write cut the file back to the last
// command written before it, even past the commands it had written in full, so
// every retry wrote the same commands, failed at the same place and made no
// progress. Under always nothing piles up: AppendSync leaves a failed command
// out.
func TestWriterKeepsTheWholeCommandsAFailedWriteGotIntoTheFile(t *testing.T) {
	for _, policy := range []Policy{PolicyEverysec, PolicyNo} {
		t.Run(policy.String(), func(t *testing.T) {
			writer, faulty, path := openFaultyWriter(t, policy)
			mustAppend(t, writer, setCommand("a", "1"))
			want := []string{"SET a 1"}

			setFaults(writer, func() { faulty.failing = true })
			for i := 0; i < 50; i++ {
				key := fmt.Sprintf("k%02d", i)
				if err := appendAndWrite(writer, setCommand(key, "v")); !errors.Is(err, errDiskFull) {
					t.Fatalf("append %d on a full disk: error = %v, want it to wrap %v", i, err, errDiskFull)
				}
				want = append(want, "SET "+key+" v")
			}

			// Each write now takes 40 commands and half of the next one.
			size := len(setCommand("k00", "v"))
			setFaults(writer, func() { faulty.failing, faulty.writeLimit = false, 40*size+size/2 })
			if err := appendAndWrite(writer, setCommand("x", "1")); !errors.Is(err, errDiskFull) {
				t.Fatalf("append of a backlog larger than one write: error = %v, want it to wrap %v", err, errDiskFull)
			}
			got, stats := loadCommands(t, path)
			if len(got) != 41 || !stats.TornTail {
				t.Fatalf("after one limited write the file replays %d commands (torn tail %v), want 41 and part of the next", len(got), stats.TornTail)
			}

			if err := appendAndWrite(writer, setCommand("y", "2")); err != nil {
				t.Fatalf("append of the rest of the backlog, which fits one write: %v", err)
			}
			if !writer.LastWriteOK() {
				t.Fatal("LastWriteOK() = false after the backlog was written")
			}
			wantReplay(t, writer, path, append(want, "SET x 1", "SET y 2")...)
		})
	}
}

// TestAppendSyncRefusesPayloadsAfterAFailedFsyncUntilARewrite covers always
// after an fsync failure, or a failed close of the old file in a swap whose
// rename then failed. A later AppendSync whose own fsync succeeded returned nil,
// so the client got OK although earlier commands may have been lost from the
// file.
func TestAppendSyncRefusesPayloadsAfterAFailedFsyncUntilARewrite(t *testing.T) {
	tests := []struct {
		name string
		fail func(t *testing.T, writer *Writer, faulty *faultyFile, path string)
	}{
		{
			name: "an fsync fails",
			fail: func(t *testing.T, writer *Writer, faulty *faultyFile, _ string) {
				setFaults(writer, func() { faulty.failSync = true })
				if err := writer.AppendSync(setCommand("b", "2")); !errors.Is(err, errSyncFails) {
					t.Fatalf("AppendSync() whose fsync fails: error = %v, want it to wrap %v", err, errSyncFails)
				}
				setFaults(writer, func() { faulty.failSync = false })
			},
		},
		{
			name: "the old file fails to close in a swap whose rename fails",
			fail: func(t *testing.T, writer *Writer, faulty *faultyFile, path string) {
				full := fileSize(t, path)
				setFaults(writer, func() { faulty.failClose, faulty.sizeAfterClose = true, full })
				tempFile := rewriteFile(t, path, setCommand("a", "1"))
				restore := fakeSeam(writer, &renameFile, func(string, string) error { return errSwapStep })
				defer restore()
				if completed, err := writer.finalizeRewrite(tempFile, tempFile.Name()); completed || !errors.Is(err, errSwapStep) {
					t.Fatalf("finalizeRewrite() = (%v, %v), want (false, an error wrapping %v)", completed, err, errSwapStep)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			writer, faulty, path := openFaultyWriter(t, PolicyAlways)
			mustAppend(t, writer, setCommand("a", "1"))
			tt.fail(t, writer, faulty, path)

			if err := writer.AppendSync(setCommand("c", "3")); !errors.Is(err, errEarlierSyncFailed) {
				t.Fatalf("AppendSync() after the failure: error = %v, want it to wrap %v", err, errEarlierSyncFailed)
			}
			if got, _ := loadCommands(t, path); !reflect.DeepEqual(got, []string{"SET a 1"}) {
				t.Fatalf("the file replays %q, want only the command before the failure", got)
			}

			tempFile := rewriteFile(t, path, setCommand("a", "1"), setCommand("c", "3"))
			if completed, err := writer.finalizeRewrite(tempFile, tempFile.Name()); !completed || err != nil {
				t.Fatalf("finalizeRewrite() = (%v, %v), want (true, nil)", completed, err)
			}
			if err := writer.AppendSync(setCommand("d", "4")); err != nil {
				t.Fatalf("AppendSync() after a rewrite replaced the file: %v", err)
			}
			wantReplay(t, writer, path, "SET a 1", "SET c 3", "SET d 4")
		})
	}
}

// TestAppendSyncLeavesACommandItCouldNotMakeDurableOutOfTheFile covers always,
// where the client gets ERR for such a command and the replicas never get it.
// The writer kept the command and wrote it with the next one, so the file held
// a write the replicas lacked: SET k 1 answered ERR, then APPEND k x, left "1x"
// in the file and "x" on the replicas. The rewrite buffer must not carry it into
// a rewritten file either.
func TestAppendSyncLeavesACommandItCouldNotMakeDurableOutOfTheFile(t *testing.T) {
	tests := []struct {
		name string
		// refusesAfter is set when the failure also makes AppendSync refuse what
		// follows (TestAppendSyncRefusesPayloadsAfterAFailedFsyncUntilARewrite).
		refusesAfter bool
		fault        func(faulty *faultyFile, on bool)
	}{
		{name: "the write fails", fault: func(f *faultyFile, on bool) { f.failing = on }},
		{name: "the write fails part way", fault: func(f *faultyFile, on bool) { f.failing, f.partial = on, on }},
		{name: "the fsync fails", refusesAfter: true, fault: func(f *faultyFile, on bool) { f.failSync = on }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			writer, faulty, path := openFaultyWriter(t, PolicyAlways)
			mustAppend(t, writer, setCommand("a", "1"))
			setFaults(writer, func() {
				writer.rewriteActive = true
				tt.fault(faulty, true)
			})
			if err := writer.AppendSync(setCommand("b", "2")); err == nil {
				t.Fatal("AppendSync() error = nil, want the failure")
			}
			setFaults(writer, func() { tt.fault(faulty, false) })

			got, stats := loadCommands(t, path)
			if want := []string{"SET a 1"}; !reflect.DeepEqual(got, want) || stats.TornTail {
				t.Fatalf("after the failure the file replays %q (torn tail %v), want %q and nothing of the failed command", got, stats.TornTail, want)
			}
			var rewriteBuffer string
			setFaults(writer, func() { rewriteBuffer = writer.rewriteBuffer.String() })
			if rewriteBuffer != "" {
				t.Fatalf("rewrite buffer = %q, want the failed command left out", rewriteBuffer)
			}

			want := []string{"SET a 1"}
			if !tt.refusesAfter {
				if err := writer.AppendSync(setCommand("c", "3")); err != nil {
					t.Fatalf("AppendSync() after the failure: %v", err)
				}
				want = append(want, "SET c 3")
				setFaults(writer, func() { rewriteBuffer = writer.rewriteBuffer.String() })
				if rewriteBuffer != string(setCommand("c", "3")) {
					t.Fatalf("rewrite buffer = %q, want only the command that succeeded", rewriteBuffer)
				}
			}
			wantReplay(t, writer, path, want...)
		})
	}
}

// TestAppendSyncRetriesCuttingATakenBackCommandWhileItRefuses covers always
// when the fsync of a command fails and so does cutting it back out, as when
// no file handle is free for the cut. The command got ERR and never reached the
// replicas, but every later AppendSync was refused before the cut was tried
// again, so it stayed in the file and a crash would replay it.
func TestAppendSyncRetriesCuttingATakenBackCommandWhileItRefuses(t *testing.T) {
	writer, faulty, path := openFaultyWriter(t, PolicyAlways)
	mustAppend(t, writer, setCommand("a", "1"))

	restore := fakeSeam(writer, &cutFile, func(string, int64) error { return errTruncateFails })
	setFaults(writer, func() { faulty.failSync = true })
	if err := writer.AppendSync(setCommand("b", "2")); !errors.Is(err, errSyncFails) {
		t.Fatalf("AppendSync() whose fsync fails: error = %v, want it to wrap %v", err, errSyncFails)
	}
	if got, _ := loadCommands(t, path); !reflect.DeepEqual(got, []string{"SET a 1", "SET b 2"}) {
		t.Fatalf("the file replays %q, want the command the failed cut left there", got)
	}

	restore()
	setFaults(writer, func() { faulty.failSync = false })
	if err := writer.AppendSync(setCommand("c", "3")); !errors.Is(err, errEarlierSyncFailed) {
		t.Fatalf("AppendSync() after the failed fsync: error = %v, want it to wrap %v", err, errEarlierSyncFailed)
	}
	got, stats := loadCommands(t, path)
	if want := []string{"SET a 1"}; !reflect.DeepEqual(got, want) || stats.TornTail {
		t.Fatalf("the file replays %q (torn tail %v), want %q: the refused AppendSync cuts the taken-back command out", got, stats.TornTail, want)
	}
}

// TestSyncFsyncsTheCommandsAFailedFlushWrote covers everysec while writes keep
// failing part way. The commands those writes got into the file in full went
// unsynced for the whole failure, because a failed flush returned before the
// fsync. If that fsync fails, the status must stay err like any other failed
// fsync.
func TestSyncFsyncsTheCommandsAFailedFlushWrote(t *testing.T) {
	tests := []struct {
		name       string
		failSync   bool
		wantOKLate bool
	}{
		{name: "the fsync succeeds", failSync: false, wantOKLate: true},
		{name: "the fsync fails", failSync: true, wantOKLate: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			writer, faulty, _ := openFaultyWriter(t, PolicyEverysec)
			size := len(setCommand("k00", "v"))
			for i := 0; i < 20; i++ {
				if err := writer.Append(setCommand(fmt.Sprintf("k%02d", i), "v")); err != nil {
					t.Fatalf("Append() error = %v", err)
				}
			}

			// Each write takes one command and half of the next, so twenty need
			// many ticks; the everysec goroutine cannot finish them during the
			// test.
			var before int
			setFaults(writer, func() {
				faulty.writeLimit, faulty.failSync = size+size/2, tt.failSync
				before = faulty.syncs
			})
			err := writer.sync()
			if !errors.Is(err, errDiskFull) {
				t.Fatalf("sync() of a backlog larger than one write: error = %v, want it to wrap %v", err, errDiskFull)
			}
			if errors.Is(err, errSyncFails) != tt.failSync {
				t.Fatalf("sync() error = %v, want it to wrap the fsync failure exactly when the fsync fails", err)
			}
			var after int
			setFaults(writer, func() { after = faulty.syncs })
			if after <= before {
				t.Fatal("sync() wrote whole commands and did not fsync them")
			}

			setFaults(writer, func() { faulty.writeLimit, faulty.failSync = 0, false })
			if err := writer.sync(); err != nil {
				t.Fatalf("sync() once writes work: %v", err)
			}
			if writer.LastWriteOK() != tt.wantOKLate {
				t.Fatalf("LastWriteOK() = %v after the backlog was written, want %v", writer.LastWriteOK(), tt.wantOKLate)
			}
		})
	}
}
