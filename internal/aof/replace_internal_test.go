package aof

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/maltemindedal/stash/internal/storage"
)

// TestReplaceFileDirectorySync covers the durability step of an AOF rewrite
// swap: a rename is not on disk until the containing directory is fsynced, so
// replaceFile must call syncDir for the target and must not report success when
// that sync fails.
func TestReplaceFileDirectorySync(t *testing.T) {
	newTempAndTarget := func(t *testing.T) (string, string) {
		t.Helper()

		dir := t.TempDir()
		target := filepath.Join(dir, "appendonly.aof")
		temp := filepath.Join(dir, "appendonly.aof.tmp")
		if err := os.WriteFile(temp, []byte("payload"), 0o600); err != nil {
			t.Fatalf("write temp: %v", err)
		}
		return temp, target
	}

	t.Run("syncs the target directory", func(t *testing.T) {
		temp, target := newTempAndTarget(t)

		var syncedDir string
		original := syncDir
		syncDir = func(path string) error {
			syncedDir = path
			return original(path)
		}
		defer func() { syncDir = original }()

		if err := replaceFile(temp, target); err != nil {
			t.Fatalf("replaceFile() error = %v", err)
		}
		if syncedDir != target {
			t.Fatalf("syncDir path = %q, want %q", syncedDir, target)
		}
		if _, err := os.Stat(target); err != nil {
			t.Fatalf("target missing after replace: %v", err)
		}
	})

	t.Run("propagates a sync failure", func(t *testing.T) {
		temp, target := newTempAndTarget(t)

		sentinel := errors.New("sync failed")
		original := syncDir
		syncDir = func(string) error { return sentinel }
		defer func() { syncDir = original }()

		if err := replaceFile(temp, target); !errors.Is(err, sentinel) {
			t.Fatalf("replaceFile() error = %v, want wrapped %v", err, sentinel)
		}
	})
}

// TestRewriteSwapFailureKeepsBufferedWrites covers a rewrite whose file swap
// fails. Under everysec, commands appended while the rewrite runs sit in the
// writer's buffer until the next sync; the swap closed the old file without
// flushing it and then reopened the original with a fresh buffer, so those
// acknowledged commands were dropped, and the rewrite's own copy of them
// (rewriteBuffer) was discarded on failure.
func TestRewriteSwapFailureKeepsBufferedWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	writer, err := OpenWriter(context.Background(), path, PolicyEverysec, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("OpenWriter() error = %v", err)
	}

	// The state the snapshot barrier leaves behind: a rewrite is active, so
	// appended commands are also copied into the rewrite buffer.
	writer.mu.Lock()
	writer.rewriteActive = true
	writer.mu.Unlock()

	payload := []byte("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n")
	if err := writer.Append(payload); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	tempFile, err := os.CreateTemp(filepath.Dir(path), "appendonly.aof.rewrite-*")
	if err != nil {
		t.Fatalf("CreateTemp() error = %v", err)
	}
	sentinel := errors.New("rename failed")
	originalRename := renameFile
	renameFile = func(string, string) error { return sentinel }
	defer func() { renameFile = originalRename }()

	completed, err := writer.finalizeRewrite(tempFile, tempFile.Name())
	if completed || !errors.Is(err, sentinel) {
		t.Fatalf("finalizeRewrite() = (%v, %v), want (false, an error wrapping the rename failure)", completed, err)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("append-only file after the failed swap = %q, want the acknowledged command %q", got, payload)
	}
}

// TestBeginRewriteConcurrentWithClose starts rewrites while the writer is being
// closed. BeginRewrite added to the writer's WaitGroup after releasing the lock
// that Close's Wait is ordered against, which the race detector reports as a
// WaitGroup Add racing with Wait, so this test is meaningful under -race.
func TestBeginRewriteConcurrentWithClose(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store := storage.NewStore()

	for i := 0; i < 500; i++ {
		writer, err := OpenWriter(context.Background(), filepath.Join(t.TempDir(), "appendonly.aof"), PolicyNo, logger)
		if err != nil {
			t.Fatalf("OpenWriter() error = %v", err)
		}

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			if err := writer.BeginRewrite(context.Background(), store); err != nil && !errors.Is(err, ErrClosed) && !errors.Is(err, ErrRewriteInProgress) {
				t.Errorf("BeginRewrite() error = %v", err)
			}
		}()
		go func() {
			defer wg.Done()
			if err := writer.Close(); err != nil {
				t.Errorf("Close() error = %v", err)
			}
		}()
		wg.Wait()

		// After Close, a further rewrite must be refused.
		if err := writer.BeginRewrite(context.Background(), store); !errors.Is(err, ErrClosed) {
			t.Fatalf("BeginRewrite() after Close error = %v, want ErrClosed", err)
		}
	}
}
