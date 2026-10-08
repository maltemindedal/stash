//go:build linux || darwin

package test

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/maltemindedal/stash/internal/command"
	stashlogger "github.com/maltemindedal/stash/internal/logger"
	"github.com/maltemindedal/stash/internal/server"
	"github.com/maltemindedal/stash/internal/storage"
)

func TestServerRefusesToSeedAnAOFPathThatIsNotARegularFile(t *testing.T) {
	// The keys an RDB snapshot loads go into the append-only file by a rename
	// over its path. Over a device such as /dev/full, which reports a size of
	// zero, that would put a regular file in the device's place, so the server
	// refuses to start instead. A FIFO in a temp directory stands in for the
	// device, so that a server that wrongly renames over it harms nothing.
	dir := t.TempDir()
	aofPath := filepath.Join(dir, "appendonly.aof")
	if err := syscall.Mkfifo(aofPath, 0o600); err != nil {
		t.Fatalf("Mkfifo(%q) error = %v", aofPath, err)
	}
	// Opening a FIFO to write blocks until it has a reader. With this one, a
	// server that wrongly opens it to append starts, and the test fails instead
	// of hanging.
	reader, err := os.OpenFile(aofPath, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		t.Fatalf("OpenFile(%q) error = %v", aofPath, err)
	}
	defer closeTestResource(t, reader)

	cfg := testAOFConfig(aofPath)
	cfg.RDBPath = writeTempRDBFile(t, buildTestRDB(
		selectTestDB(0),
		testStringEntry([]byte("fromrdb"), []byte("hello")),
	))
	logger := stashlogger.New(cfg.LogLevel)
	store := storage.NewStore()
	srv := server.New(cfg, logger, store, command.NewExecutor(store, logger))

	// A server that wrongly starts would serve until the context ends.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err = srv.ListenAndServe(ctx)
	if err == nil {
		t.Fatal("ListenAndServe() error = nil, want a refusal to start")
	}
	if msg := err.Error(); !strings.HasPrefix(msg, "server: ") || !strings.Contains(msg, strconv.Quote(aofPath)) || !strings.Contains(msg, "not a regular file") {
		t.Fatalf("ListenAndServe() error = %q, want a server: error naming %q and saying it is not a regular file", msg, aofPath)
	}
	if info, err := os.Lstat(aofPath); err != nil || info.Mode()&os.ModeNamedPipe == 0 {
		t.Fatalf("Lstat(%q) = (%v, %v), want the FIFO left in place", aofPath, info, err)
	}
	if files, err := os.ReadDir(dir); err != nil || len(files) != 1 {
		t.Fatalf("directory = (%v, %v), want only the FIFO", files, err)
	}
}
