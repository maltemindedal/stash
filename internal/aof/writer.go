package aof

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sync"
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

// Writer appends RESP payloads to an append-only file and can rewrite it in the background.
type Writer struct {
	path   string
	policy Policy
	logger *slog.Logger

	mu             sync.Mutex
	file           *os.File
	writer         *bufio.Writer
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
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("aof: create directory %q: %w", dir, err)
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("aof: open %q: %w", path, err)
	}

	writer := &Writer{
		path:    path,
		policy:  policy,
		logger:  logger,
		file:    file,
		writer:  bufio.NewWriter(file),
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

// Append writes a payload without forcing an immediate fsync.
func (w *Writer) Append(payload []byte) error {
	return w.append(payload, false)
}

// AppendSync writes a payload and fsyncs it before returning.
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
	closeErr := w.file.Close()
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
	if _, err := w.writer.Write(payload); err != nil {
		return fmt.Errorf("aof: write %q: %w", w.path, err)
	}
	if w.rewriteActive {
		if _, err := w.rewriteBuffer.Write(payload); err != nil {
			return fmt.Errorf("aof: buffer rewrite payload for %q: %w", w.path, err)
		}
	}
	if syncNow {
		return w.syncLocked()
	}
	if w.policy == PolicyNo {
		return w.flushLocked()
	}

	return nil
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
		return err
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("aof: sync %q: %w", w.path, err)
	}

	return nil
}

func (w *Writer) flushLocked() error {
	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("aof: flush %q: %w", w.path, err)
	}

	return nil
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
	oldFile := w.file
	if oldFile != nil {
		// Commands appended under everysec wait in the writer's buffer. If the swap
		// fails, the original file is reopened with a fresh buffer and the rewrite
		// buffer is discarded, so anything not flushed here would be lost.
		if err := w.flushLocked(); err != nil {
			return fmt.Errorf("flush append-only file before rewrite swap: %w", err)
		}
		if err := oldFile.Close(); err != nil {
			return fmt.Errorf("close current append-only file before rewrite swap: %w", err)
		}
	}
	if err := replaceFile(tempPath, w.path); err != nil {
		reopenErr := w.reopenAppendOnlyFileLocked()
		if reopenErr != nil {
			return fmt.Errorf("replace append-only file with rewrite: %w; reopen original file: %v", err, reopenErr)
		}
		return fmt.Errorf("replace append-only file with rewrite: %w", err)
	}
	if err := w.reopenAppendOnlyFileLocked(); err != nil {
		return fmt.Errorf("reopen append-only file after rewrite: %w", err)
	}

	return nil
}

func (w *Writer) reopenAppendOnlyFileLocked() error {
	file, err := os.OpenFile(w.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}

	w.file = file
	w.writer = bufio.NewWriter(file)
	return nil
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

func replaceFile(tempPath string, targetPath string) error {
	if err := renameFile(tempPath, targetPath); err != nil {
		if !isReplaceTargetExistsError(err) {
			return fmt.Errorf("aof: replace %q: %w", targetPath, err)
		}

		removeErr := removeFile(targetPath)
		if removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			return fmt.Errorf("aof: replace %q: rename error: %w; remove error: %v", targetPath, err, removeErr)
		}
		if retryErr := renameFile(tempPath, targetPath); retryErr != nil {
			return fmt.Errorf("aof: replace %q after removing existing target: %w", targetPath, retryErr)
		}
	}

	// Persist the rename itself: the new directory entry is not crash-durable
	// until the containing directory is fsynced.
	if err := syncDir(targetPath); err != nil {
		return fmt.Errorf("aof: sync directory after replacing %q: %w", targetPath, err)
	}

	return nil
}

func isReplaceTargetExistsError(err error) bool {
	if err == nil {
		return false
	}

	return errors.Is(err, fs.ErrExist) || os.IsExist(err)
}
