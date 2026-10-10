package aof

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maltemindedal/stash/internal/storage"
)

var (
	renameFile = os.Rename
	removeFile = os.Remove
	syncDir    = defaultSyncDir
)

// defaultSyncDir fsyncs the directory containing path so a preceding rename is
// durable across a crash. On POSIX a rename's new directory entry is not
// guaranteed to survive a crash until the parent directory's inode is fsynced;
// without this a crash right after the swap can lose the just-written file. It
// is a no-op on Windows, where directory handles cannot be synced this way.
func defaultSyncDir(path string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	syncErr := dir.Sync()
	closeErr := dir.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

const (
	// pendingFlushSize is how many bytes of commands Append collects under
	// everysec before it writes them to the file, so that between the
	// once-a-second fsyncs commands reach the OS a few kilobytes at a time.
	pendingFlushSize = 4096
	// maxIdlePendingCap is the largest buffer the writer keeps for pending
	// commands once they are written. One grown past it, by a large command or
	// by commands held through a write failure, is released.
	maxIdlePendingCap = 1 << 20
)

// errEarlierSyncFailed is AppendSync's error from a failed fsync until a
// rewrite replaces the file.
var errEarlierSyncFailed = errors.New("an earlier fsync failed, so no later one shows the file whole; BGREWRITEAOF replaces it")

// appendFile is what the writer needs of its append-only file: an *os.File, or
// in tests a stand-in that fails.
type appendFile interface {
	Write(p []byte) (int, error)
	Sync() error
	Close() error
}

// Writer appends RESP payloads to an append-only file and can rewrite it in the background.
type Writer struct {
	path   string
	policy Policy
	logger *slog.Logger

	mu sync.Mutex
	// file is nil after a rewrite swap could not reopen the file at path. The
	// next flush tries again.
	file appendFile
	// pending holds the commands Append has accepted that the file has not yet
	// taken in full, oldest first. They stay here until a write takes them, so a
	// failed write is retried by the next flush, ahead of later commands,
	// instead of being dropped.
	pending []byte
	// ends holds where each payload in pending ends, ascending. A write that
	// fails part way still counts the payloads it wrote in full, so the next
	// attempt starts after them.
	ends []int
	// written is the size of the file up to the end of the last command written
	// to it in full. Writes count whole payloads only, so it is always a command
	// boundary.
	written int64
	// synced is written as of the last successful fsync.
	synced int64
	// tornTail is set when the file may hold bytes after written: part of a
	// command a failed write left there, or a reopened file whose size is not
	// written. The loader accepts an unfinished command only at the end of the
	// file, so the next flush cuts the file back to written before it writes
	// anything.
	tornTail bool
	// adoptSize makes the next reopen take the file's size as written: the file
	// at path is a rewrite, not the one written counts.
	adoptSize bool
	// syncFailed is set by a failed fsync, or by a failed close of the old file
	// during a rewrite swap whose rename then fails, and cleared only when a
	// rewrite replaces the file. After such a failure the kernel may have dropped
	// data it could not write, and a later fsync can succeed without it, so no
	// later success proves the file whole. AppendSync refuses payloads while it
	// is set.
	syncFailed bool
	// dirUnsynced is set when a rewrite was renamed into place but its directory
	// could not be synced. The next sync retries it before it reports success.
	dirUnsynced    bool
	rewriteBuffer  bytes.Buffer
	rewriteActive  bool
	rewritePending bool
	closed         bool
	closeOnce      sync.Once
	closeCh        chan struct{}
	wg             sync.WaitGroup

	// closing is set, under mu, before Close waits for background work. Adds to wg
	// happen under mu and only while closing is false, so they cannot race with
	// that Wait.
	closing bool

	// writeFailed is true from a failed write until the next one that succeeds,
	// and from a failed fsync until a rewrite replaces the file. Under everysec
	// and no the server acknowledges a command before it is on disk, so this is
	// how an operator finds out the log has stopped growing; see LastWriteOK.
	writeFailed atomic.Bool

	// rewriteGuard, when set, is held across the snapshot a rewrite takes. See
	// SetRewriteGuard.
	rewriteGuard func() (release func())
}

// OpenWriter opens or creates an append-only file writer for the supplied path.
func OpenWriter(ctx context.Context, path string, policy Policy, logger *slog.Logger) (*Writer, error) {
	if path == "" {
		return nil, errors.New("aof: empty file path")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	dir := filepath.Dir(path)
	if dir == "" {
		dir = "."
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("aof: create directory %q: %w", dir, err)
	}

	file, size, err := openAppendOnlyFile(path)
	if err != nil {
		return nil, fmt.Errorf("aof: open %q: %w", path, err)
	}

	writer := &Writer{
		path:    path,
		policy:  policy,
		logger:  logger,
		file:    file,
		written: size,
		synced:  size,
		closeCh: make(chan struct{}),
	}
	if policy == PolicyEverysec {
		writer.wg.Add(1)
		go writer.runEverysec(ctx)
	}

	return writer, nil
}

// Policy returns the configured appendfsync policy.
func (w *Writer) Policy() Policy {
	if w == nil {
		return PolicyEverysec
	}

	return w.policy
}

// SetRewriteGuard registers a function a rewrite calls before it snapshots the
// store, and whose result it calls when the snapshot is done. The snapshot and
// the switch to buffering new commands are atomic with respect to the store, but
// a command that had updated the store and not yet appended its frame would then
// be in the snapshot and in the rewrite buffer both, and replay twice. The owner
// of the append path passes a guard that waits for such commands to finish.
func (w *Writer) SetRewriteGuard(guard func() (release func())) {
	if w == nil {
		return
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	w.rewriteGuard = guard
}

func (w *Writer) acquireRewriteGuard() func() {
	w.mu.Lock()
	guard := w.rewriteGuard
	w.mu.Unlock()
	if guard == nil {
		return func() {}
	}
	return guard()
}

// LastWriteOK reports whether the file holds every command appended so far, as
// far as the writer can tell (INFO aof_last_write_status). It is false from a
// failed write until a later write succeeds, and from a failed fsync until a
// rewrite replaces the file. Under everysec and no, Append acknowledges a
// command before it is on disk, so a failure is otherwise visible only in the
// log.
func (w *Writer) LastWriteOK() bool {
	return w == nil || !w.writeFailed.Load()
}

// Append writes a payload without forcing an immediate fsync. A payload whose
// write fails is kept, and the next append, everysec fsync or Close writes it
// ahead of anything appended after it.
func (w *Writer) Append(payload []byte) error {
	return w.append(payload, false)
}

// AppendSync writes a payload and fsyncs it before returning, or returns an
// error and leaves the payload out of the file: it is not retried, and any part
// of it already written is cut back. The server answers such a command with an
// error and does not send it to replicas, so the file and the replicas agree.
// After a failed fsync AppendSync refuses every payload until a rewrite
// replaces the file; see LastWriteOK.
func (w *Writer) AppendSync(payload []byte) error {
	return w.append(payload, true)
}

// Close flushes, fsyncs, and closes the underlying AOF file.
func (w *Writer) Close() error {
	if w == nil {
		return nil
	}

	w.closeOnce.Do(func() {
		w.mu.Lock()
		w.closing = true
		w.mu.Unlock()
		close(w.closeCh)
	})
	w.wg.Wait()

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true

	flushErr := w.syncLocked()
	var closeErr error
	if w.file != nil {
		closeErr = w.file.Close()
	}
	if flushErr != nil && closeErr != nil {
		return errors.Join(flushErr, closeErr)
	}
	if flushErr != nil {
		return flushErr
	}
	if closeErr != nil {
		return fmt.Errorf("aof: close %q: %w", w.path, closeErr)
	}

	return nil
}

// BeginRewrite starts a background rewrite of the current AOF file.
func (w *Writer) BeginRewrite(ctx context.Context, store *storage.Store) error {
	if w == nil {
		return ErrClosed
	}
	if store == nil {
		return errors.New("aof: store unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	w.mu.Lock()
	if w.closing || w.closed {
		w.mu.Unlock()
		return ErrClosed
	}
	if w.rewritePending || w.rewriteActive {
		w.mu.Unlock()
		return ErrRewriteInProgress
	}
	w.rewritePending = true
	w.wg.Add(1)
	w.mu.Unlock()

	go w.runRewrite(ctx, store)
	return nil
}

func (w *Writer) append(payload []byte, syncNow bool) error {
	if w == nil || len(payload) == 0 {
		return nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	if syncNow && w.syncFailed {
		// A command taken back after the failed fsync may still be in the file
		// if cutting it failed; nothing else retries the cut while every
		// AppendSync is refused.
		w.cutTornTailLocked()
		return fmt.Errorf("aof: append to %q: %w", w.path, errEarlierSyncFailed)
	}
	// Append keeps the command whether or not the file takes it now: in pending
	// until a write of it succeeds, and in the rewrite buffer for a rewrite that
	// has taken its snapshot. AppendSync takes it back from both if it fails.
	rewriteLen := w.rewriteBuffer.Len()
	w.pending = append(w.pending, payload...)
	w.ends = append(w.ends, len(w.pending))
	if w.rewriteActive {
		if _, err := w.rewriteBuffer.Write(payload); err != nil {
			return fmt.Errorf("aof: buffer rewrite payload for %q: %w", w.path, err)
		}
	}
	switch {
	case syncNow:
		if err := w.syncLocked(); err != nil {
			w.withdrawLocked(len(payload), rewriteLen)
			return err
		}
		return nil
	case w.policy == PolicyNo:
		// Handing the bytes to the OS is the whole durability step for this policy.
		if err := w.flushLocked(); err != nil {
			return err
		}
		return w.settleLocked()
	case len(w.pending) >= pendingFlushSize:
		return w.flushLocked()
	}

	return nil
}

// withdrawLocked takes back the payload of size bytes that AppendSync added
// last, after it could not be written and synced, and cuts back any part of it
// already in the file. Nothing was appended after it, so it is the tail of
// pending if pending holds anything, and otherwise the last bytes written.
func (w *Writer) withdrawLocked(size int, rewriteLen int) {
	if len(w.pending) > 0 {
		w.pending = w.pending[:len(w.pending)-size]
		w.ends = w.ends[:len(w.ends)-1]
	} else {
		w.written -= int64(size)
		w.synced = min(w.synced, w.written)
		w.tornTail = true
	}
	w.rewriteBuffer.Truncate(rewriteLen)
	// If the cut fails here, the next AppendSync or flush tries again first.
	w.cutTornTailLocked()
}

// cutTornTailLocked cuts the file back to written if it may hold bytes after
// it. A failure leaves tornTail set for the next attempt.
func (w *Writer) cutTornTailLocked() {
	if w.tornTail && w.file != nil && cutFile(w.path, w.written) == nil {
		w.tornTail = false
	}
}

func (w *Writer) runEverysec(ctx context.Context) {
	defer w.wg.Done()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-w.closeCh:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.sync(); err != nil && !errors.Is(err, ErrClosed) {
				w.logWarn("failed to sync append-only file", "path", w.path, "error", err)
			}
		}
	}
}

func (w *Writer) sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}

	return w.syncLocked()
}

func (w *Writer) syncLocked() error {
	if err := w.flushLocked(); err != nil {
		// Commands an earlier write, or this one, got into the file in full
		// still need their fsync, or they would wait for it for as long as the
		// failure lasts.
		if w.file != nil && w.written > w.synced {
			if syncErr := w.file.Sync(); syncErr != nil {
				w.syncFailed = true
				return errors.Join(err, fmt.Errorf("aof: sync %q: %w", w.path, syncErr))
			}
			w.synced = w.written
		}
		return err
	}
	if err := w.file.Sync(); err != nil {
		w.syncFailed = true
		w.writeFailed.Store(true)
		return fmt.Errorf("aof: sync %q: %w", w.path, err)
	}
	w.synced = w.written

	return w.settleLocked()
}

// settleLocked runs once a flush, and the fsync the policy asks for, have
// succeeded. It retries the directory sync a rewrite swap could not finish, and
// then clears the failed status unless an fsync has failed since the file was
// last replaced.
func (w *Writer) settleLocked() error {
	if w.dirUnsynced {
		if err := syncDir(w.path); err != nil {
			w.writeFailed.Store(true)
			return fmt.Errorf("aof: sync the directory of %q after a rewrite: %w", w.path, err)
		}
		w.dirUnsynced = false
	}
	w.writeFailed.Store(w.syncFailed)

	return nil
}

// flushLocked writes every pending command to the file, reopening it first if a
// rewrite swap could not. If the file may hold part of a command after the last
// complete one, it first cuts the file back to that command, and fails without
// writing if it cannot: appending after the torn command would leave it in the
// middle of the file, which the loader refuses.
func (w *Writer) flushLocked() error {
	if w.file == nil {
		if err := w.reopenLocked(); err != nil {
			return err
		}
	}
	if w.tornTail {
		if err := cutFile(w.path, w.written); err != nil {
			w.writeFailed.Store(true)
			return fmt.Errorf("aof: cut %q back to its last complete command at byte %d: %w", w.path, w.written, err)
		}
		w.tornTail = false
	}
	if len(w.pending) == 0 {
		return nil
	}
	n, err := w.file.Write(w.pending)
	if err != nil {
		// Write reports every byte it wrote. The payloads it wrote in full stay
		// written; only a part of the next one is cut back, so a backlog larger
		// than one write can take still gets through over several.
		done := w.dropWrittenLocked(n)
		w.tornTail = n > done
		w.writeFailed.Store(true)
		return fmt.Errorf("aof: write %q: %w", w.path, err)
	}
	w.written += int64(n)
	if cap(w.pending) > maxIdlePendingCap {
		w.pending, w.ends = nil, nil
	} else {
		w.pending, w.ends = w.pending[:0], w.ends[:0]
	}

	return nil
}

// dropWrittenLocked counts as written the payloads at the front of pending that
// end within its first n bytes, removes them from pending, and returns their
// size.
func (w *Writer) dropWrittenLocked(n int) int {
	whole := sort.SearchInts(w.ends, n+1)
	if whole == 0 {
		return 0
	}
	done := w.ends[whole-1]
	w.written += int64(done)
	w.pending = w.pending[:copy(w.pending, w.pending[done:])]
	w.ends = w.ends[:copy(w.ends, w.ends[whole:])]
	for i := range w.ends {
		w.ends[i] -= done
	}

	return done
}

func (w *Writer) runRewrite(ctx context.Context, store *storage.Store) {
	defer w.wg.Done()

	select {
	case <-w.closeCh:
		w.clearRewriteState(false)
		return
	case <-ctx.Done():
		w.clearRewriteState(false)
		return
	default:
	}

	startedAt := time.Now()
	dir := filepath.Dir(w.path)
	if dir == "" {
		dir = "."
	}
	tempFile, err := os.CreateTemp(dir, filepath.Base(w.path)+".rewrite-*")
	if err != nil {
		w.clearRewriteState(false)
		w.logError("failed to create rewrite temp file", "path", w.path, "error", err)
		return
	}
	tempPath := tempFile.Name()
	cleanupTemp := true
	defer func() {
		if cleanupTemp {
			if removeErr := removeFile(tempPath); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
				w.logWarn("failed to remove rewrite temp file", "path", tempPath, "error", removeErr)
			}
		}
	}()

	activated := false
	releaseGuard := w.acquireRewriteGuard()
	entries, snapshotStats := store.SnapshotAllWithWriteBarrier(func() {
		w.mu.Lock()
		defer w.mu.Unlock()
		if w.closed {
			w.rewritePending = false
			return
		}

		w.rewriteActive = true
		w.rewritePending = false
		w.rewriteBuffer.Reset()
		activated = true
	})
	releaseGuard()
	if !activated {
		_ = tempFile.Close()
		return
	}

	select {
	case <-w.closeCh:
		w.clearRewriteState(true)
		_ = tempFile.Close()
		return
	case <-ctx.Done():
		w.clearRewriteState(true)
		_ = tempFile.Close()
		return
	default:
	}

	rewriteStats, err := GenerateRewrite(entries, tempFile)
	if err != nil {
		w.clearRewriteState(true)
		_ = tempFile.Close()
		w.logError("failed to generate rewritten append-only file", "path", w.path, "error", err)
		return
	}

	completed, err := w.finalizeRewrite(tempFile, tempPath)
	if err != nil {
		w.logError("failed to finalize append-only file rewrite", "path", w.path, "error", err)
		return
	}
	if !completed {
		return
	}

	cleanupTemp = false

	w.logInfo(
		"completed append-only file rewrite",
		"path", w.path,
		"snapshot_keys", snapshotStats.ExportedKeys,
		"rewrite_keys", rewriteStats.Keys,
		"rewrite_commands", rewriteStats.Commands,
		"duration", time.Since(startedAt),
	)
}

func (w *Writer) clearRewriteState(active bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.clearRewriteStateLocked(active)
}

func (w *Writer) clearRewriteStateLocked(active bool) {
	if active {
		w.rewriteActive = false
		w.rewriteBuffer.Reset()
	}
	w.rewritePending = false
}

func (w *Writer) finalizeRewrite(tempFile *os.File, tempPath string) (bool, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		w.clearRewriteStateLocked(true)
		_ = tempFile.Close()
		return false, nil
	}
	if err := w.appendBufferedRewriteLocked(tempFile); err != nil {
		w.clearRewriteStateLocked(true)
		_ = tempFile.Close()
		return false, fmt.Errorf("write buffered rewrite payload to %q: %w", tempPath, err)
	}
	if err := tempFile.Sync(); err != nil {
		w.clearRewriteStateLocked(true)
		_ = tempFile.Close()
		return false, fmt.Errorf("sync rewritten append-only file %q: %w", tempPath, err)
	}
	if err := tempFile.Close(); err != nil {
		w.clearRewriteStateLocked(true)
		return false, fmt.Errorf("close rewritten append-only temp file %q: %w", tempPath, err)
	}
	if err := w.swapRewriteFileLocked(tempPath); err != nil {
		w.clearRewriteStateLocked(true)
		return false, err
	}

	w.clearRewriteStateLocked(true)
	return true, nil
}

func (w *Writer) appendBufferedRewriteLocked(tempFile *os.File) error {
	_, err := tempFile.Write(w.rewriteBuffer.Bytes())
	return err
}

func (w *Writer) swapRewriteFileLocked(tempPath string) error {
	// The rewrite already holds every command still pending for the old file:
	// those appended before its snapshot are in the snapshot, and those appended
	// since are in the rewrite buffer, written to tempPath. So the old file is
	// not flushed first, and its failing cannot stop the swap. If the rename
	// fails, the old file is reopened and the next flush writes the pending
	// commands to it.
	var closeErr error
	if w.file != nil {
		closeErr = w.file.Close()
		w.file = nil
	}
	if closeErr != nil {
		w.logWarn("failed to close append-only file before rewrite swap", "path", w.path, "error", closeErr)
	}
	replaceErr := replaceFile(tempPath, w.path)
	// replaceFile renames before it syncs the directory, so unless the rename
	// itself failed, w.path now names the rewritten file.
	var unsynced *unsyncedReplaceError
	replaced := replaceErr == nil || errors.As(replaceErr, &unsynced)
	if replaced {
		// The rewrite was written and fsynced in full. Writing the pending
		// commands to it would repeat them.
		w.pending, w.ends = nil, nil
		w.tornTail = false
		w.adoptSize = true
		w.syncFailed = false
		w.dirUnsynced = replaceErr != nil
	} else if closeErr != nil {
		// A failed close reports a write the kernel could not finish, as a failed
		// fsync does: the old file may hold less than written says. Reopening
		// compares its size before anything is appended.
		w.syncFailed = true
	}

	// A failed reopen marks the write failed, and the next flush tries again.
	reopenErr := w.reopenLocked()
	if reopenErr == nil {
		if replaced {
			w.writeFailed.Store(w.dirUnsynced)
		} else if w.syncFailed {
			w.writeFailed.Store(true)
		}
	}
	switch {
	case replaceErr != nil && reopenErr != nil:
		return fmt.Errorf("replace append-only file with rewrite: %w; %w", replaceErr, reopenErr)
	case replaceErr != nil:
		return fmt.Errorf("replace append-only file with rewrite: %w", replaceErr)
	case reopenErr != nil:
		return fmt.Errorf("reopen append-only file after rewrite: %w", reopenErr)
	}

	return nil
}

// reopenLocked opens the file at path to append to. After a rewrite replaced
// the file, its size is where the next command goes. Otherwise it is the file
// the writer had, and if its size is not written, the next flush cuts it back
// to written, or fails if it is shorter.
func (w *Writer) reopenLocked() error {
	file, size, err := openAppendOnlyFile(w.path)
	if err != nil {
		w.writeFailed.Store(true)
		return fmt.Errorf("aof: reopen %q: %w", w.path, err)
	}
	w.file = file
	if w.adoptSize {
		// The rewrite was fsynced in full before it was renamed into place.
		w.written, w.synced = size, size
		w.adoptSize = false
	} else if size != w.written {
		w.tornTail = true
	}

	return nil
}

// cutFile cuts the file at path back to size bytes; tests replace it.
var cutFile = cutAppendOnlyFile

// cutAppendOnlyFile cuts the append-only file at path back to size bytes, the
// end of its last complete command, and syncs it, so a command AppendSync took
// back does not return after a crash. It opens a handle of its own, as
// TruncateTail does: Go opens an O_APPEND file on Windows without the
// write-data access truncating needs. It never extends the file. A file shorter
// than size has lost commands the writer counted as written, and padding it
// would put zeros where they were.
func cutAppendOnlyFile(path string, size int64) (err error) {
	file, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}()

	info, err := file.Stat()
	if err != nil {
		return err
	}
	if info.Size() < size {
		return fmt.Errorf("the file is %d bytes, shorter than the %d its complete commands took", info.Size(), size)
	}
	if err := file.Truncate(size); err != nil {
		return err
	}
	return file.Sync()
}

// openAppendOnlyFile opens path for appending, creating it if needed, and
// returns its size.
func openAppendOnlyFile(path string) (*os.File, int64, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, 0, err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, 0, err
	}

	return file, info.Size(), nil
}

func (w *Writer) logInfo(msg string, args ...any) {
	if w != nil && w.logger != nil {
		w.logger.Info(msg, args...)
	}
}

func (w *Writer) logWarn(msg string, args ...any) {
	if w != nil && w.logger != nil {
		w.logger.Warn(msg, args...)
	}
}

func (w *Writer) logError(msg string, args ...any) {
	if w != nil && w.logger != nil {
		w.logger.Error(msg, args...)
	}
}

// replaceFile moves tempPath over targetPath with a single rename. os.Rename
// replaces an existing file atomically on every supported platform, so there is
// no remove-then-rename fallback: it opened a window in which a failed second
// rename left no append-only file at all.
func replaceFile(tempPath string, targetPath string) error {
	if err := renameFile(tempPath, targetPath); err != nil {
		return fmt.Errorf("aof: replace %q: %w", targetPath, err)
	}

	// Persist the rename itself: the new directory entry is not crash-durable
	// until the containing directory is fsynced.
	if err := syncDir(targetPath); err != nil {
		return &unsyncedReplaceError{path: targetPath, err: err}
	}

	return nil
}

// unsyncedReplaceError is replaceFile's error when the rename succeeded and only
// the directory sync after it failed: targetPath already names the new file,
// though the rename may not survive a crash.
type unsyncedReplaceError struct {
	path string
	err  error
}

func (e *unsyncedReplaceError) Error() string {
	return fmt.Sprintf("aof: sync directory after replacing %q: %v", e.path, e.err)
}

func (e *unsyncedReplaceError) Unwrap() error { return e.err }
