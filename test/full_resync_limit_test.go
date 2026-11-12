package test

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"testing"

	"github.com/maltemindedal/stash/internal/command"
	"github.com/maltemindedal/stash/internal/config"
	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/rdb"
	"github.com/maltemindedal/stash/internal/server"
	"github.com/maltemindedal/stash/internal/storage"
)

// fullResyncTestLimit stands in for the 512 MiB bulk string limit, so that a
// test can fill a master past it.
const fullResyncTestLimit = 4096

// TestAMasterRefusesAFullResyncWhoseSnapshotExceedsTheBulkStringLimit runs PSYNC
// by hand against a master whose snapshot limit is lowered. Over the limit the
// master answers with an error that names the snapshot's size and the limit,
// logs the same, and registers no replica: the connection stays an ordinary
// client that still answers PING. Under the limit the master serves the full
// resync as before.
func TestAMasterRefusesAFullResyncWhoseSnapshotExceedsTheBulkStringLimit(t *testing.T) {
	tests := []struct {
		name      string
		valueSize int
		refused   bool
	}{
		{name: "a keyspace over the limit is refused", valueSize: 2 * fullResyncTestLimit, refused: true},
		{name: "a keyspace under the limit is served", valueSize: fullResyncTestLimit / 4},
	}

	forEachNetworkingMode(t, defaultTestConfig(), func(t *testing.T, cfg config.Config) {
		for _, tt := range tests {
			tt := tt
			t.Run(tt.name, func(t *testing.T) {
				store := storage.NewStore()
				if err := store.Set(nil, "big", bytes.Repeat([]byte("v"), tt.valueSize), 0); err != nil {
					t.Fatalf("Set() error = %v", err)
				}
				entries, _ := store.SnapshotStrings()
				size := rdb.SnapshotSize(entries)

				logs := &syncBuffer{}
				addr := startMasterWithSnapshotLimit(t, cfg, store, slog.New(slog.NewTextHandler(logs, nil)))
				conn, err := net.Dial("tcp", addr)
				if err != nil {
					t.Fatalf("Dial(%q) error = %v", addr, err)
				}
				defer closeTestResource(t, conn)
				parser := protocol.NewParser(conn)

				assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "OK"}, "REPLCONF", "listening-port", "6380")
				if !tt.refused {
					assertFullResyncServed(t, conn, parser, size)
					return
				}

				refusal := protocol.ErrorValue{Message: fmt.Sprintf(
					"ERR full resync snapshot of %d bytes exceeds the %d byte limit of a bulk string", size, fullResyncTestLimit,
				)}
				assertCommandResponse(t, conn, parser, refusal, "PSYNC", "?", "-1")
				assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "PONG"}, "PING")
				if got := infoFields(t, conn, parser, "replication")["connected_slaves"]; got != "0" {
					t.Fatalf("connected_slaves = %q after a refused PSYNC, want 0", got)
				}
				for _, want := range []string{
					"refusing full resync",
					fmt.Sprintf("snapshot_size_bytes=%d", size),
					fmt.Sprintf("limit_bytes=%d", fullResyncTestLimit),
				} {
					if !strings.Contains(logs.String(), want) {
						t.Fatalf("the master logged %q, want it to contain %q", logs.String(), want)
					}
				}
			})
		}
	})
}

// startMasterWithSnapshotLimit serves store on cfg from a master that refuses a
// snapshot over fullResyncTestLimit, and stops it when the test ends.
func startMasterWithSnapshotLimit(t *testing.T, cfg config.Config, store *storage.Store, logger *slog.Logger) string {
	t.Helper()

	newExecutor := func(services server.Services) (server.CommandExecutor, error) {
		services.Master.SetSnapshotLimit(fullResyncTestLimit)
		return command.New(services)
	}
	srv, err := server.New(cfg, logger, store, newExecutor)
	if err != nil {
		t.Fatalf("server.New() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.ListenAndServe(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		waitForServerStop(t, errCh)
	})
	return waitForAddr(t, srv)
}

// assertFullResyncServed sends PSYNC and requires FULLRESYNC followed by a
// snapshot of size bytes.
func assertFullResyncServed(t *testing.T, conn net.Conn, parser *protocol.Parser, size int) {
	t.Helper()

	if err := protocol.WriteValue(conn, request("PSYNC", "?", "-1")); err != nil {
		t.Fatalf("WriteValue(PSYNC) error = %v", err)
	}
	line, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse(FULLRESYNC) error = %v", err)
	}
	if text, ok := line.(protocol.SimpleString); !ok || !strings.HasPrefix(text.Value, "FULLRESYNC ") {
		t.Fatalf("PSYNC reply = %#v, want FULLRESYNC", line)
	}
	snapshot, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse(snapshot) error = %v", err)
	}
	if bulk, ok := snapshot.(protocol.BulkString); !ok || len(bulk.Data) != size {
		t.Fatalf("snapshot = %T, want a bulk string of %d bytes", snapshot, size)
	}
}
