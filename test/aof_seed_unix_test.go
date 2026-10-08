//go:build linux || darwin

package test

import (
	"context"
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

func TestServerRefusesToStartWhenItCannotSeedTheAOFFromTheRDB(t *testing.T) {
	// A FIFO reports size 0, as a new file does, so startup takes the branch that
	// loads the snapshot and then writes its keys into the append-only file. That
	// write replaces the file, which would destroy anything that is not a regular
	// file (a device such as /dev/null is the likely real case), so the server
	// refuses to start instead. The FIFO is in the test's own directory.
	rdbPath := writeTempRDBFile(t, buildTestRDB(
		selectTestDB(0),
		testStringEntry([]byte("fromrdb"), []byte("hello")),
	))
	aofPath := filepath.Join(t.TempDir(), "appendonly.aof")
	if err := syscall.Mkfifo(aofPath, 0o600); err != nil {
		t.Fatalf("Mkfifo(%q) error = %v", aofPath, err)
	}

	cfg := testAOFConfig(aofPath)
	cfg.RDBPath = rdbPath
	logger := stashlogger.New(cfg.LogLevel)
	store := storage.NewStore()
	srv := server.New(cfg, logger, store, command.NewExecutor(store, logger))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe(ctx) }()

	// A server that wrongly goes on opens the FIFO for writing, which blocks
	// until something reads it, so it neither returns nor serves.
	var err error
	select {
	case err = <-errCh:
	case <-time.After(3 * time.Second):
		t.Fatal("ListenAndServe() did not return, want a refusal to start")
	}
	if err == nil {
		t.Fatal("ListenAndServe() error = nil, want a refusal to start")
	}
	if !strings.HasPrefix(err.Error(), "server: ") || !strings.Contains(err.Error(), aofPath) {
		t.Fatalf("ListenAndServe() error = %q, want a server: error naming %q", err, aofPath)
	}
	info, statErr := os.Lstat(aofPath)
	if statErr != nil || info.Mode()&os.ModeNamedPipe == 0 {
		t.Fatalf("Lstat(%q) = (%v, %v), want the FIFO still in place", aofPath, info, statErr)
	}
}
