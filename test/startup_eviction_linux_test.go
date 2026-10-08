//go:build linux

package test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/maltemindedal/stash/internal/command"
	stashlogger "github.com/maltemindedal/stash/internal/logger"
	"github.com/maltemindedal/stash/internal/server"
	"github.com/maltemindedal/stash/internal/storage"
)

func TestServerRefusesToStartWhenItCannotLogStartupEvictions(t *testing.T) {
	// Keys evicted at startup are appended to the append-only file before the
	// first client is served. A server that cannot record them would come back
	// with them on the next start, so it refuses to start instead of serving a
	// keyspace the log does not describe.
	//
	// The append-only file is a FIFO in the test's own directory that nothing
	// reads. Opening it for writing needs a reader to be there, so one opens it
	// and closes it at once. A write after that close fails with EPIPE; a write
	// that wins the race with the close lands in the pipe, and the fsync after it
	// fails with EINVAL. Either way the eviction's append is the first thing that
	// cannot be logged, with no keys to seed from a snapshot first and no device
	// in /dev involved.
	aofPath := filepath.Join(t.TempDir(), "appendonly.aof")
	if err := syscall.Mkfifo(aofPath, 0o600); err != nil {
		t.Fatalf("Mkfifo(%q) error = %v", aofPath, err)
	}
	go func() {
		if reader, err := os.OpenFile(aofPath, os.O_RDONLY, 0); err == nil {
			_ = reader.Close()
		}
	}()

	cfg := defaultTestConfig()
	cfg.AOFPath = aofPath
	cfg.MaxMemory = 2000
	logger := stashlogger.New(cfg.LogLevel)
	store := storage.NewStore()
	for i := 0; i < 50; i++ {
		if _, err := store.Set(startupEvictionKey(i), []byte(strings.Repeat("0", 100)), 0); err != nil {
			t.Fatalf("Set(%s) error = %v", startupEvictionKey(i), err)
		}
	}
	srv := server.New(cfg, logger, store, command.NewExecutor(store, logger))

	// A server that wrongly starts would serve until the context ends.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err := srv.ListenAndServe(ctx)
	if err == nil {
		t.Fatal("ListenAndServe() error = nil, want a refusal to start when the evictions cannot be logged")
	}
	if !strings.HasPrefix(err.Error(), "server: ") {
		t.Fatalf("ListenAndServe() error = %q, want it to start with %q", err, "server: ")
	}
	if want := `append startup evictions to aof "` + aofPath + `"`; !strings.Contains(err.Error(), want) {
		t.Fatalf("ListenAndServe() error = %q, want it to say %q", err, want)
	}
	if !errors.Is(err, syscall.EPIPE) && !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("ListenAndServe() error = %q, want it to wrap the failed write (%v) or fsync (%v)", err, syscall.EPIPE, syscall.EINVAL)
	}
}
