//go:build linux

package test

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/maltemindedal/stash/internal/command"
	stashlogger "github.com/maltemindedal/stash/internal/logger"
	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/server"
	"github.com/maltemindedal/stash/internal/storage"
)

func TestServerRefusesToStartWhenItCannotLogStartupEvictions(t *testing.T) {
	// Keys evicted at startup are appended to the append-only file before the
	// first client is served. A server that cannot record them would come back
	// with them on the next start, so it refuses to start instead of serving a
	// keyspace the log does not describe.
	dumpPath := filepath.Join(t.TempDir(), "dump.rdb")
	seedCfg := defaultTestConfig()
	seedCfg.DumpPath = dumpPath
	addr, stop, errCh := startTestServer(t, seedCfg)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) error = %v", addr, err)
	}
	parser := protocol.NewParser(conn)
	for i := 0; i < 50; i++ {
		assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "OK"}, "SET", startupEvictionKey(i), strings.Repeat("0", 100))
	}
	closeTestResource(t, conn)
	stop()
	waitForServerStop(t, errCh)

	// /dev/full opens for appending and fails every write with ENOSPC, so the
	// startup eviction is the first thing that cannot be logged.
	const aofPath = "/dev/full"
	cfg := defaultTestConfig()
	cfg.RDBPath = dumpPath
	cfg.AOFPath = aofPath
	cfg.MaxMemory = 2000
	logger := stashlogger.New(cfg.LogLevel)
	store := storage.NewStore()
	srv := server.New(cfg, logger, store, command.NewExecutor(store, logger))

	// A server that wrongly starts would serve until the context ends.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err = srv.ListenAndServe(ctx)
	if err == nil {
		t.Fatal("ListenAndServe() error = nil, want a refusal to start when the evictions cannot be logged")
	}
	if !strings.HasPrefix(err.Error(), "server: ") {
		t.Fatalf("ListenAndServe() error = %q, want it to start with %q", err, "server: ")
	}
	if want := `append startup evictions to aof "` + aofPath + `"`; !strings.Contains(err.Error(), want) {
		t.Fatalf("ListenAndServe() error = %q, want it to say %q", err, want)
	}
	if !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("ListenAndServe() error = %q, want it to wrap the write failure %v", err, syscall.ENOSPC)
	}
}
