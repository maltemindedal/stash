package test

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	stashlogger "github.com/maltemindedal/stash/internal/logger"
	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/rdb"
	"github.com/maltemindedal/stash/internal/storage"
)

func TestServerLoadsRDBBeforeServingCommands(t *testing.T) {
	rdbPath := writeTempRDBFile(t, buildTestRDB(
		selectTestDB(0),
		testStringEntry([]byte("name"), []byte("Stash")),
		testExpiringMillisEntry(uint64(time.Now().Add(-time.Second).UnixMilli()), []byte("stale"), []byte("gone")),
	))

	cfg := defaultTestConfig()
	cfg.RDBPath = rdbPath

	logger := stashlogger.New(cfg.LogLevel)
	store := storage.NewStore()
	srv := newServer(t, cfg, logger, store)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.ListenAndServe(ctx)
	}()

	addr := waitForAddr(t, srv)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) error = %v", addr, err)
	}
	t.Cleanup(func() {
		if closeErr := conn.Close(); closeErr != nil {
			t.Logf("failed to close test connection: %v", closeErr)
		}
	})

	parser := protocol.NewParser(conn)
	assertCommandResponse(t, conn, parser, protocol.BulkString{Data: []byte("Stash")}, "GET", "name")
	assertCommandResponse(t, conn, parser, protocol.BulkString{Null: true}, "GET", "stale")

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("ListenAndServe() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not stop within timeout")
	}
}

func TestServerFailsFastOnCorruptRDB(t *testing.T) {
	dir := t.TempDir()
	rdbPath := filepath.Join(dir, "bad.rdb")
	if err := os.WriteFile(rdbPath, []byte("not-a-valid-rdb"), 0o600); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", rdbPath, err)
	}

	cfg := defaultTestConfig()
	cfg.RDBPath = rdbPath

	logger := stashlogger.New(cfg.LogLevel)
	store := storage.NewStore()
	srv := newServer(t, cfg, logger, store)

	err := srv.ListenAndServe(context.Background())
	if err == nil {
		t.Fatal("ListenAndServe() error = nil, want startup failure")
	}
	if !errors.Is(err, rdb.ErrInvalidHeader) {
		t.Fatalf("ListenAndServe() error = %v, want wrapped ErrInvalidHeader", err)
	}
	if srv.Addr() != "" {
		t.Fatalf("Addr() = %q, want empty string on startup failure", srv.Addr())
	}
}

func writeTempRDBFile(t *testing.T, payload []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "dump.rdb")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", path, err)
	}
	return path
}

func buildTestRDB(parts ...[]byte) []byte {
	payload := append([]byte{}, []byte("REDIS0011")...)
	for _, part := range parts {
		payload = append(payload, part...)
	}
	payload = append(payload, 0xFF)
	payload = append(payload, make([]byte, 8)...)
	return payload
}

func selectTestDB(index uint64) []byte {
	return append([]byte{0xFE}, encodeTestLength(index)...)
}

func testStringEntry(key, value []byte) []byte {
	payload := []byte{0x00}
	payload = append(payload, encodeTestString(key)...)
	payload = append(payload, encodeTestString(value)...)
	return payload
}

func testExpiringMillisEntry(expiresAt uint64, key, value []byte) []byte {
	payload := []byte{0xFC}
	raw := make([]byte, 8)
	binary.LittleEndian.PutUint64(raw, expiresAt)
	payload = append(payload, raw...)
	payload = append(payload, testStringEntry(key, value)...)
	return payload
}

func encodeTestString(value []byte) []byte {
	payload := encodeTestLength(uint64(len(value)))
	payload = append(payload, value...)
	return payload
}

func encodeTestLength(length uint64) []byte {
	if length < 1<<6 {
		return []byte{byte(length)}
	}
	if length < 1<<14 {
		return []byte{byte((length>>8)&0x3F) | 0x40, byte(length)}
	}

	raw := make([]byte, 5)
	raw[0] = 0x80
	binary.BigEndian.PutUint32(raw[1:], uint32(length))
	return raw
}
