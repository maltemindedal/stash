package test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/maltemindedal/stash/internal/aof"
	"github.com/maltemindedal/stash/internal/command"
	"github.com/maltemindedal/stash/internal/config"
	stashlogger "github.com/maltemindedal/stash/internal/logger"
	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/server"
	"github.com/maltemindedal/stash/internal/storage"
)

func TestServerPersistsAndReplaysAOF(t *testing.T) {
	aofPath := filepath.Join(t.TempDir(), "appendonly.aof")
	cfg := testAOFConfig(aofPath)

	addr, stop, errCh := startTestServer(t, cfg)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) error = %v", addr, err)
	}
	parser := protocol.NewParser(conn)

	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "OK"}, "SET", "name", "Stash")
	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "OK"}, "SET", "expiring", "soon", "PX", "60000")
	assertCommandResponse(t, conn, parser, protocol.Integer{Value: 1}, "HSET", "profile", "lang", "go")
	assertCommandResponse(t, conn, parser, protocol.Integer{Value: 2}, "RPUSH", "letters", "a", "b")
	assertCommandResponse(t, conn, parser, protocol.Integer{Value: 2}, "SADD", "tags", "fast", "durable")
	assertCommandResponse(t, conn, parser, protocol.Integer{Value: 2}, "ZADD", "leaders", "1", "alpha", "2", "beta")
	assertCommandResponse(t, conn, parser, protocol.BulkString{Data: []byte("1-0")}, "XADD", "events", "1-0", "type", "start")
	assertCommandResponse(t, conn, parser, protocol.Integer{Value: 1}, "INCR", "counter")
	assertCommandResponse(t, conn, parser, protocol.Integer{Value: 2}, "INCR", "counter")
	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "OK"}, "MULTI")
	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "QUEUED"}, "SET", "tx-bad", "hello")
	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "QUEUED"}, "INCR", "tx-bad")
	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "QUEUED"}, "SET", "tx-good", "1")
	assertCommandResponse(t, conn, parser, protocol.Array{Elements: []protocol.Value{
		protocol.SimpleString{Value: "OK"},
		protocol.ErrorValue{Message: "ERR value is not an integer or out of range"},
		protocol.SimpleString{Value: "OK"},
	}}, "EXEC")
	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "OK"}, "MULTI")
	assertCommandResponse(t, conn, parser, protocol.ErrorValue{Message: "ERR unknown command \"NOPE\""}, "NOPE")
	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "QUEUED"}, "SET", "tx-abort", "1")
	assertCommandResponse(t, conn, parser, protocol.ErrorValue{Message: "EXECABORT Transaction discarded because of previous errors."}, "EXEC")

	watcherConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) watcher error = %v", addr, err)
	}
	defer closeTestResource(t, watcherConn)
	watcherParser := protocol.NewParser(watcherConn)

	assertCommandResponse(t, watcherConn, watcherParser, protocol.SimpleString{Value: "OK"}, "WATCH", "tx-watch")
	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "OK"}, "SET", "tx-watch", "1")
	assertCommandResponse(t, watcherConn, watcherParser, protocol.SimpleString{Value: "OK"}, "MULTI")
	assertCommandResponse(t, watcherConn, watcherParser, protocol.SimpleString{Value: "QUEUED"}, "SET", "tx-watch", "2")
	assertCommandResponse(t, watcherConn, watcherParser, protocol.Array{Null: true}, "EXEC")
	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "OK"}, "MULTI")
	assertCommandResponse(t, conn, parser, protocol.Array{Elements: []protocol.Value{}}, "EXEC")

	if err := conn.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	stop()
	waitForServerStop(t, errCh)

	restartCfg := testAOFConfig(aofPath)
	restartAddr, restartStop, restartErrCh := startTestServer(t, restartCfg)
	restartConn, err := net.Dial("tcp", restartAddr)
	if err != nil {
		t.Fatalf("Dial(%q) restart error = %v", restartAddr, err)
	}
	defer closeTestResource(t, restartConn)
	restartParser := protocol.NewParser(restartConn)

	assertCommandResponse(t, restartConn, restartParser, protocol.BulkString{Data: []byte("Stash")}, "GET", "name")
	assertCommandResponse(t, restartConn, restartParser, protocol.BulkString{Data: []byte("soon")}, "GET", "expiring")
	assertCommandResponse(t, restartConn, restartParser, protocol.BulkString{Data: []byte("go")}, "HGET", "profile", "lang")
	assertCommandResponse(t, restartConn, restartParser, protocol.Array{Elements: []protocol.Value{
		protocol.BulkString{Data: []byte("a")},
		protocol.BulkString{Data: []byte("b")},
	}}, "LRANGE", "letters", "0", "-1")
	assertCommandResponse(t, restartConn, restartParser, protocol.Integer{Value: 1}, "SISMEMBER", "tags", "durable")
	assertCommandResponse(t, restartConn, restartParser, protocol.Array{Elements: []protocol.Value{
		protocol.BulkString{Data: []byte("alpha")},
		protocol.BulkString{Data: []byte("beta")},
	}}, "ZRANGE", "leaders", "0", "-1")
	assertCommandResponse(t, restartConn, restartParser, protocol.Array{Elements: []protocol.Value{
		protocol.Array{Elements: []protocol.Value{
			protocol.BulkString{Data: []byte("events")},
			protocol.Array{Elements: []protocol.Value{
				protocol.Array{Elements: []protocol.Value{
					protocol.BulkString{Data: []byte("1-0")},
					protocol.Array{Elements: []protocol.Value{
						protocol.BulkString{Data: []byte("type")},
						protocol.BulkString{Data: []byte("start")},
					}},
				}},
			}},
		}},
	}}, "XREAD", "STREAMS", "events", "0-0")
	assertCommandResponse(t, restartConn, restartParser, protocol.BulkString{Data: []byte("2")}, "GET", "counter")
	assertCommandResponse(t, restartConn, restartParser, protocol.BulkString{Data: []byte("hello")}, "GET", "tx-bad")
	assertCommandResponse(t, restartConn, restartParser, protocol.BulkString{Data: []byte("1")}, "GET", "tx-good")
	assertCommandResponse(t, restartConn, restartParser, protocol.BulkString{Null: true}, "GET", "tx-abort")
	assertCommandResponse(t, restartConn, restartParser, protocol.BulkString{Data: []byte("1")}, "GET", "tx-watch")

	restartStop()
	waitForServerStop(t, restartErrCh)
}

func TestServerPrefersAOFOverRDB(t *testing.T) {
	dir := t.TempDir()
	aofPath := filepath.Join(dir, "appendonly.aof")
	rdbPath := writeTempRDBFile(t, buildTestRDB(
		selectTestDB(0),
		testStringEntry([]byte("name"), []byte("rdb")),
	))
	payload, err := protocol.Encode(request("SET", "name", "aof"))
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	if err := os.WriteFile(aofPath, payload, 0o600); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", aofPath, err)
	}

	cfg := testAOFConfig(aofPath)
	cfg.RDBPath = rdbPath
	addr, stop, errCh := startTestServer(t, cfg)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) error = %v", addr, err)
	}
	defer closeTestResource(t, conn)
	parser := protocol.NewParser(conn)

	assertCommandResponse(t, conn, parser, protocol.BulkString{Data: []byte("aof")}, "GET", "name")

	stop()
	waitForServerStop(t, errCh)
}

func TestServerKeepsRDBKeysAfterTheAOFTakesOver(t *testing.T) {
	// With --rdb and an AOF that is missing or empty, the snapshot's keys are
	// loaded and nothing wrote them into the AOF. The first write made the file
	// non-empty, so the next start took the AOF branch, skipped the snapshot, and
	// those keys were gone.
	tests := []struct {
		name    string
		prepare func(t *testing.T, aofPath string)
	}{
		{name: "the append-only file does not exist", prepare: func(*testing.T, string) {}},
		{name: "the append-only file is empty", prepare: func(t *testing.T, aofPath string) {
			if err := os.WriteFile(aofPath, nil, 0o600); err != nil {
				t.Fatalf("WriteFile(%q) error = %v", aofPath, err)
			}
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rdbPath := writeTempRDBFile(t, buildTestRDB(
				selectTestDB(0),
				testStringEntry([]byte("fromrdb"), []byte("hello")),
			))
			aofPath := filepath.Join(t.TempDir(), "appendonly.aof")
			tt.prepare(t, aofPath)
			cfg := testAOFConfig(aofPath)
			cfg.RDBPath = rdbPath

			addr, stop, errCh := startTestServer(t, cfg)
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				t.Fatalf("Dial(%q) error = %v", addr, err)
			}
			parser := protocol.NewParser(conn)
			assertCommandResponse(t, conn, parser, protocol.BulkString{Data: []byte("hello")}, "GET", "fromrdb")
			assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "OK"}, "SET", "other", "1")
			closeTestResource(t, conn)
			stop()
			waitForServerStop(t, errCh)

			// The snapshot is skipped from here on, so take it away to show the
			// append-only file alone carries its keys.
			if err := os.Remove(rdbPath); err != nil {
				t.Fatalf("Remove(%q) error = %v", rdbPath, err)
			}

			restartAddr, restartStop, restartErrCh := startTestServer(t, cfg)
			restartConn, err := net.Dial("tcp", restartAddr)
			if err != nil {
				t.Fatalf("Dial(%q) restart error = %v", restartAddr, err)
			}
			defer closeTestResource(t, restartConn)
			restartParser := protocol.NewParser(restartConn)
			assertCommandResponse(t, restartConn, restartParser, protocol.BulkString{Data: []byte("hello")}, "GET", "fromrdb")
			assertCommandResponse(t, restartConn, restartParser, protocol.BulkString{Data: []byte("1")}, "GET", "other")

			restartStop()
			waitForServerStop(t, restartErrCh)
		})
	}
}

func TestServerKeepsTheDeadlineOfAnRDBKeyAfterTheAOFTakesOver(t *testing.T) {
	// The seeded append-only file has to carry the snapshot's absolute deadline.
	// A relative one would be read against the clock at each restart, so the key
	// would get a fresh lease every time it was loaded.
	deadline := time.Now().Add(2 * time.Second).UnixMilli()
	rdbPath := writeTempRDBFile(t, buildTestRDB(
		selectTestDB(0),
		testExpiringMillisEntry(uint64(deadline), []byte("session"), []byte("alice")),
	))
	aofPath := filepath.Join(t.TempDir(), "appendonly.aof")
	cfg := testAOFConfig(aofPath)
	cfg.RDBPath = rdbPath

	addr, stop, errCh := startTestServer(t, cfg)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) error = %v", addr, err)
	}
	parser := protocol.NewParser(conn)
	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "OK"}, "SET", "other", "1")
	closeTestResource(t, conn)
	stop()
	waitForServerStop(t, errCh)

	wantFrame, err := protocol.Encode(request("SET", "session", "alice", "PXAT", strconv.FormatInt(deadline, 10)))
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	logged, err := os.ReadFile(aofPath)
	if err != nil {
		t.Fatalf("ReadFile(%q) error = %v", aofPath, err)
	}
	if !bytes.Contains(logged, wantFrame) {
		t.Fatalf("append-only file = %q, want it to hold %q", logged, wantFrame)
	}

	for time.Now().UnixMilli() <= deadline {
		time.Sleep(10 * time.Millisecond)
	}

	restartAddr, restartStop, restartErrCh := startTestServer(t, cfg)
	restartConn, err := net.Dial("tcp", restartAddr)
	if err != nil {
		t.Fatalf("Dial(%q) restart error = %v", restartAddr, err)
	}
	defer closeTestResource(t, restartConn)
	restartParser := protocol.NewParser(restartConn)
	assertCommandResponse(t, restartConn, restartParser, protocol.BulkString{Null: true}, "GET", "session")
	assertCommandResponse(t, restartConn, restartParser, protocol.BulkString{Data: []byte("1")}, "GET", "other")

	restartStop()
	waitForServerStop(t, restartErrCh)
}

func TestServerLogsStartupEvictionsOfKeysItLoadedFromTheRDB(t *testing.T) {
	// The snapshot's keys are written into the append-only file before startup
	// enforces --maxmemory, so the deletions of the keys it evicts land after
	// their SETs and a restart without the limit finds the same keys.
	const keys = 50
	parts := [][]byte{selectTestDB(0)}
	for i := 0; i < keys; i++ {
		parts = append(parts, testStringEntry([]byte(startupEvictionKey(i)), []byte(strings.Repeat("0", 100))))
	}
	rdbPath := writeTempRDBFile(t, buildTestRDB(parts...))
	aofPath := filepath.Join(t.TempDir(), "appendonly.aof")
	cfg := testAOFConfig(aofPath)
	cfg.RDBPath = rdbPath
	cfg.MaxMemory = 2000

	addr, stop, errCh := startTestServer(t, cfg)
	live := liveStartupEvictionKeys(t, addr, keys)
	stop()
	waitForServerStop(t, errCh)
	if len(live) == 0 || len(live) == keys {
		t.Fatalf("%d of %d keys stayed live under the limit, want some evicted and some live", len(live), keys)
	}

	if err := os.Remove(rdbPath); err != nil {
		t.Fatalf("Remove(%q) error = %v", rdbPath, err)
	}
	cfg.MaxMemory = 0
	restartAddr, restartStop, restartErrCh := startTestServer(t, cfg)
	restarted := liveStartupEvictionKeys(t, restartAddr, keys)
	restartStop()
	waitForServerStop(t, restartErrCh)

	if len(restarted) != len(live) {
		t.Fatalf("%d keys are live after restarting without the limit, want the %d that were live with it", len(restarted), len(live))
	}
	for key := range live {
		if !restarted[key] {
			t.Fatalf("key %q was live under the limit and is gone after a restart", key)
		}
	}
}

func TestServerDoesNotPersistPublishToAOF(t *testing.T) {
	aofPath := filepath.Join(t.TempDir(), "appendonly.aof")
	cfg := testAOFConfig(aofPath)
	addr, stop, errCh := startTestServer(t, cfg)

	subscriberConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) subscriber error = %v", addr, err)
	}
	defer closeTestResource(t, subscriberConn)
	subscriberParser := protocol.NewParser(subscriberConn)
	if err := protocol.WriteValue(subscriberConn, request("SUBSCRIBE", "updates")); err != nil {
		t.Fatalf("WriteValue(SUBSCRIBE) error = %v", err)
	}
	if _, err := subscriberParser.Parse(); err != nil {
		t.Fatalf("Parse() SUBSCRIBE error = %v", err)
	}

	publisherConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) publisher error = %v", addr, err)
	}
	defer closeTestResource(t, publisherConn)
	publisherParser := protocol.NewParser(publisherConn)
	assertCommandResponse(t, publisherConn, publisherParser, protocol.Integer{Value: 1}, "PUBLISH", "updates", "hello")
	if _, err := subscriberParser.Parse(); err != nil {
		t.Fatalf("Parse() pushed pubsub message error = %v", err)
	}

	info, err := os.Stat(aofPath)
	if err != nil {
		t.Fatalf("Stat(%q) error = %v", aofPath, err)
	}
	if info.Size() != 0 {
		t.Fatalf("AOF size after PUBLISH = %d, want 0", info.Size())
	}

	stop()
	waitForServerStop(t, errCh)
}

func TestServerRejectsInvalidReplicaConfigBeforeOpeningAOF(t *testing.T) {
	aofPath := filepath.Join(t.TempDir(), "appendonly.aof")
	cfg := testAOFConfig(aofPath)
	cfg.ReplicaOf = "not-a-host-port"

	logger := stashlogger.New(cfg.LogLevel)
	store := storage.NewStore()
	executor := command.NewExecutor(store, logger)
	srv := server.New(cfg, logger, store, executor)

	err := srv.ListenAndServe(context.Background())
	if err == nil {
		t.Fatal("ListenAndServe() error = nil, want invalid replica configuration failure")
	}
	if _, statErr := os.Stat(aofPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("Stat(%q) error = %v, want file not created", aofPath, statErr)
	}
}

func TestServerBGRewriteAOFCompactsAndReloads(t *testing.T) {
	aofPath := filepath.Join(t.TempDir(), "appendonly.aof")
	cfg := testAOFConfig(aofPath)
	addr, stop, errCh := startTestServer(t, cfg)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) error = %v", addr, err)
	}
	parser := protocol.NewParser(conn)

	for i := 0; i < 20; i++ {
		assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "OK"}, "SET", "name", "value"+time.Date(2000, 1, 1, 0, 0, i, 0, time.UTC).Format("05"))
	}
	before, err := os.Stat(aofPath)
	if err != nil {
		t.Fatalf("Stat(%q) before rewrite error = %v", aofPath, err)
	}
	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "Background append only file rewriting started"}, "BGREWRITEAOF")

	deadline := time.Now().Add(3 * time.Second)
	compacted := false
	for time.Now().Before(deadline) {
		info, statErr := os.Stat(aofPath)
		if statErr != nil {
			t.Fatalf("Stat(%q) during rewrite error = %v", aofPath, statErr)
		}
		if info.Size() < before.Size() {
			compacted = true
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if !compacted {
		t.Fatalf("BGREWRITEAOF did not compact file within timeout (before=%d)", before.Size())
	}

	if err := conn.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	stop()
	waitForServerStop(t, errCh)

	restartCfg := testAOFConfig(aofPath)
	restartAddr, restartStop, restartErrCh := startTestServer(t, restartCfg)
	restartConn, err := net.Dial("tcp", restartAddr)
	if err != nil {
		t.Fatalf("Dial(%q) restart error = %v", restartAddr, err)
	}
	defer closeTestResource(t, restartConn)
	restartParser := protocol.NewParser(restartConn)
	assertCommandResponse(t, restartConn, restartParser, protocol.BulkString{Data: []byte("value19")}, "GET", "name")

	restartStop()
	waitForServerStop(t, restartErrCh)
}

func TestAOFRewriteKeepsTTLDeadlinesAcrossRestart(t *testing.T) {
	// A rewrite used to write a key's remaining time as `PX <ms left>`. Replay
	// reads that against the clock at load, so every restart after a rewrite gave
	// the key a fresh lease and brought back keys that had expired in between.
	aofPath := filepath.Join(t.TempDir(), "appendonly.aof")
	addr, stop, errCh := startTestServer(t, testAOFConfig(aofPath))
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) error = %v", addr, err)
	}
	parser := protocol.NewParser(conn)

	// Repeated writes to one key give the rewrite something to compact, so the
	// file shrinking is the signal that it has been swapped in.
	for i := 0; i < 20; i++ {
		assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "OK"}, "SET", "filler", "v"+strconv.Itoa(i))
	}
	deadline := strconv.FormatInt(time.Now().Add(2*time.Second).UnixMilli(), 10)
	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "OK"}, "SET", "session:42", "alice", "PXAT", deadline)
	before, err := os.Stat(aofPath)
	if err != nil {
		t.Fatalf("Stat(%q) before rewrite error = %v", aofPath, err)
	}
	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "Background append only file rewriting started"}, "BGREWRITEAOF")

	waitUntil := time.Now().Add(3 * time.Second)
	compacted := false
	for time.Now().Before(waitUntil) {
		info, statErr := os.Stat(aofPath)
		if statErr != nil {
			t.Fatalf("Stat(%q) during rewrite error = %v", aofPath, statErr)
		}
		if info.Size() < before.Size() {
			compacted = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !compacted {
		t.Fatalf("BGREWRITEAOF did not compact file within timeout (before=%d)", before.Size())
	}

	// The live path logged this frame too, but the rewrite replaced the file, so
	// finding it here means the rewrite kept the absolute deadline.
	wantFrame, err := protocol.Encode(request("SET", "session:42", "alice", "PXAT", deadline))
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	rewritten, err := os.ReadFile(aofPath)
	if err != nil {
		t.Fatalf("ReadFile(%q) error = %v", aofPath, err)
	}
	if !bytes.Contains(rewritten, wantFrame) {
		t.Fatalf("rewritten append-only file = %q, want it to hold %q", rewritten, wantFrame)
	}

	closeTestResource(t, conn)
	stop()
	waitForServerStop(t, errCh)

	deadlineMillis, err := strconv.ParseInt(deadline, 10, 64)
	if err != nil {
		t.Fatalf("ParseInt(%q) error = %v", deadline, err)
	}
	for time.Now().UnixMilli() <= deadlineMillis {
		time.Sleep(10 * time.Millisecond)
	}

	restartAddr, restartStop, restartErrCh := startTestServer(t, testAOFConfig(aofPath))
	restartConn, err := net.Dial("tcp", restartAddr)
	if err != nil {
		t.Fatalf("Dial(%q) restart error = %v", restartAddr, err)
	}
	defer closeTestResource(t, restartConn)
	restartParser := protocol.NewParser(restartConn)

	assertCommandResponse(t, restartConn, restartParser, protocol.BulkString{Null: true}, "GET", "session:42")
	assertCommandResponse(t, restartConn, restartParser, protocol.BulkString{Data: []byte("v19")}, "GET", "filler")

	restartStop()
	waitForServerStop(t, restartErrCh)
}

func TestServerDiscardsTornAOFTailBeforeAppending(t *testing.T) {
	// A crash mid-append leaves a partial command at the end of the file. Startup
	// used to replay everything before it and then append new commands straight
	// after the torn bytes, so the next restart read them as part of the torn
	// command and silently dropped every write acknowledged in between.
	aofPath := filepath.Join(t.TempDir(), "appendonly.aof")
	complete, err := protocol.EncodeValues([]protocol.Value{request("SET", "a", "1"), request("SET", "b", "2")})
	if err != nil {
		t.Fatalf("EncodeValues() error = %v", err)
	}
	torn := []byte("*3\r\n$3\r\nSET\r\n$1\r\nc\r\n$3\r\nva")
	if err := os.WriteFile(aofPath, append(append([]byte{}, complete...), torn...), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	addr, stop, errCh := startTestServer(t, testAOFConfig(aofPath))
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) error = %v", addr, err)
	}
	parser := protocol.NewParser(conn)
	assertCommandResponse(t, conn, parser, protocol.BulkString{Data: []byte("2")}, "GET", "b")
	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "OK"}, "SET", "d", "4")
	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "OK"}, "SET", "e", "5")
	closeTestResource(t, conn)
	stop()
	waitForServerStop(t, errCh)

	restartAddr, restartStop, restartErrCh := startTestServer(t, testAOFConfig(aofPath))
	restartConn, err := net.Dial("tcp", restartAddr)
	if err != nil {
		t.Fatalf("Dial(%q) restart error = %v", restartAddr, err)
	}
	defer closeTestResource(t, restartConn)
	restartParser := protocol.NewParser(restartConn)

	assertCommandResponse(t, restartConn, restartParser, protocol.BulkString{Data: []byte("1")}, "GET", "a")
	assertCommandResponse(t, restartConn, restartParser, protocol.BulkString{Data: []byte("4")}, "GET", "d")
	assertCommandResponse(t, restartConn, restartParser, protocol.BulkString{Data: []byte("5")}, "GET", "e")
	assertCommandResponse(t, restartConn, restartParser, protocol.BulkString{Null: true}, "GET", "c")

	restartStop()
	waitForServerStop(t, restartErrCh)
}

func TestServerRefusesToStartOnACorruptAOF(t *testing.T) {
	// Damage before the end of the file is not a crash artefact. Starting anyway
	// would drop every command after it and append new ones behind the damage, so
	// the server refuses, names the byte where the damage starts, and leaves the
	// file exactly as it found it.
	aofPath := filepath.Join(t.TempDir(), "appendonly.aof")
	good, err := protocol.EncodeValues([]protocol.Value{request("SET", "a", "1"), request("SET", "b", "2")})
	if err != nil {
		t.Fatalf("EncodeValues() error = %v", err)
	}
	after, err := protocol.EncodeValues([]protocol.Value{request("SET", "c", "3")})
	if err != nil {
		t.Fatalf("EncodeValues() error = %v", err)
	}
	damaged := []byte("*3\r\n$3\r\nSET\r\n$1\r\nx\r\n$3\r\nabXY")
	content := append(append(append([]byte{}, good...), damaged...), after...)
	if err := os.WriteFile(aofPath, content, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	cfg := testAOFConfig(aofPath)
	logger := stashlogger.New(cfg.LogLevel)
	store := storage.NewStore()
	srv := server.New(cfg, logger, store, command.NewExecutor(store, logger))

	// A server that wrongly starts would serve until the context ends.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err = srv.ListenAndServe(ctx)
	if err == nil {
		t.Fatal("ListenAndServe() error = nil, want a refusal to start on a corrupt append-only file")
	}
	if want := "byte " + strconv.Itoa(len(good)); !strings.Contains(err.Error(), want) {
		t.Fatalf("ListenAndServe() error = %q, want it to name %q", err, want)
	}
	if !strings.Contains(err.Error(), "persistence.md") {
		t.Fatalf("ListenAndServe() error = %q, want it to point at the repair instructions", err)
	}
	got, readErr := os.ReadFile(aofPath)
	if readErr != nil || !bytes.Equal(got, content) {
		t.Fatalf("the append-only file changed (read error %v)", readErr)
	}
}

func TestServerKeepsAutoGeneratedStreamIDsAcrossRestart(t *testing.T) {
	aofPath := filepath.Join(t.TempDir(), "appendonly.aof")

	addr, stop, errCh := startTestServer(t, testAOFConfig(aofPath))
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) error = %v", addr, err)
	}
	parser := protocol.NewParser(conn)
	if err := protocol.WriteValue(conn, request("XADD", "events", "*", "type", "start")); err != nil {
		t.Fatalf("WriteValue(XADD) error = %v", err)
	}
	reply, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse(XADD reply) error = %v", err)
	}
	id, isNull, ok := integrationBulkStringContent(reply)
	if !ok || isNull || id == "" {
		t.Fatalf("XADD * reply = %#v, want the generated ID", reply)
	}
	closeTestResource(t, conn)
	stop()
	waitForServerStop(t, errCh)

	// Replay must not depend on the clock at restart, so let it move on.
	time.Sleep(20 * time.Millisecond)

	restartAddr, restartStop, restartErrCh := startTestServer(t, testAOFConfig(aofPath))
	restartConn, err := net.Dial("tcp", restartAddr)
	if err != nil {
		t.Fatalf("Dial(%q) restart error = %v", restartAddr, err)
	}
	defer closeTestResource(t, restartConn)
	restartParser := protocol.NewParser(restartConn)

	assertCommandResponse(t, restartConn, restartParser, protocol.Array{Elements: []protocol.Value{
		protocol.Array{Elements: []protocol.Value{
			protocol.BulkString{Data: []byte("events")},
			protocol.Array{Elements: []protocol.Value{
				protocol.Array{Elements: []protocol.Value{
					protocol.BulkString{Data: []byte(id)},
					protocol.Array{Elements: []protocol.Value{
						protocol.BulkString{Data: []byte("type")},
						protocol.BulkString{Data: []byte("start")},
					}},
				}},
			}},
		}},
	}}, "XREAD", "STREAMS", "events", "0-0")

	restartStop()
	waitForServerStop(t, restartErrCh)
}

// startupEvictionKey names key i of the keyspace the startup-eviction tests load.
func startupEvictionKey(i int) string {
	return fmt.Sprintf("k%04d", i)
}

// writeStartupEvictionAOF writes an append-only file that sets keys 100-byte
// values, as a server that ran without a memory limit would have left it.
func writeStartupEvictionAOF(t *testing.T, path string, keys int) {
	t.Helper()

	value := strings.Repeat("0", 100)
	frames := make([]protocol.Value, 0, keys)
	for i := 0; i < keys; i++ {
		frames = append(frames, request("SET", startupEvictionKey(i), value))
	}
	payload, err := protocol.EncodeValues(frames)
	if err != nil {
		t.Fatalf("EncodeValues() error = %v", err)
	}
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", path, err)
	}
}

// liveStartupEvictionKeys reports which of the first keys startup-eviction keys
// the server at addr still holds.
func liveStartupEvictionKeys(t *testing.T, addr string, keys int) map[string]bool {
	t.Helper()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) error = %v", addr, err)
	}
	defer closeTestResource(t, conn)
	parser := protocol.NewParser(conn)

	live := make(map[string]bool)
	const batch = 100
	for start := 0; start < keys; start += batch {
		end := start + batch
		if end > keys {
			end = keys
		}
		for i := start; i < end; i++ {
			if err := protocol.WriteValue(conn, request("GET", startupEvictionKey(i))); err != nil {
				t.Fatalf("WriteValue(GET) error = %v", err)
			}
		}
		for i := start; i < end; i++ {
			reply, err := parser.Parse()
			if err != nil {
				t.Fatalf("Parse(GET %s) error = %v", startupEvictionKey(i), err)
			}
			_, isNull, ok := integrationBulkStringContent(reply)
			if !ok {
				t.Fatalf("GET %s reply = %#v, want a bulk string", startupEvictionKey(i), reply)
			}
			if !isNull {
				live[startupEvictionKey(i)] = true
			}
		}
	}
	return live
}

// delCommandsInAOF returns the keys named by each DEL command in the file, in
// the order the commands appear.
func delCommandsInAOF(t *testing.T, path string) [][]string {
	t.Helper()

	var dels [][]string
	_, err := aof.LoadFile(context.Background(), path, func(_ context.Context, value protocol.Value) error {
		array, ok := value.(protocol.Array)
		if !ok || len(array.Elements) == 0 {
			return errors.New("replayed frame is not a command array")
		}
		name, ok := array.Elements[0].(protocol.BulkString)
		if !ok || !strings.EqualFold(string(name.Data), "DEL") {
			return nil
		}
		keys := make([]string, 0, len(array.Elements)-1)
		for _, element := range array.Elements[1:] {
			key, ok := element.(protocol.BulkString)
			if !ok {
				return errors.New("DEL argument is not a bulk string")
			}
			keys = append(keys, string(key.Data))
		}
		dels = append(dels, keys)
		return nil
	})
	if err != nil {
		t.Fatalf("LoadFile(%q) error = %v", path, err)
	}
	return dels
}

func TestKeysEvictedAtStartupStayEvictedAfterARestart(t *testing.T) {
	// Startup enforces --maxmemory after the append-only file is loaded. The keys
	// it evicted were logged nowhere, so the next start without the limit loaded
	// every one of them again.
	const maxKeysPerDel = 1024
	tests := []struct {
		name        string
		keys        int
		maxMemory   int64
		appendFsync string
		minEvicted  int
	}{
		{name: "50 keys under a 2000 byte limit", keys: 50, maxMemory: 2000, appendFsync: "always", minEvicted: 1},
		// More evictions than one DEL may name, so the log needs several.
		{name: "3000 keys under a 20000 byte limit", keys: 3000, maxMemory: 20000, appendFsync: "always", minEvicted: maxKeysPerDel + 1},
		// everysec only buffers an ordinary append for up to a second.
		{name: "50 keys under everysec", keys: 50, maxMemory: 2000, appendFsync: "everysec", minEvicted: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			aofPath := filepath.Join(t.TempDir(), "appendonly.aof")
			writeStartupEvictionAOF(t, aofPath, tt.keys)

			limited := testAOFConfig(aofPath)
			limited.AppendFsync = tt.appendFsync
			limited.MaxMemory = tt.maxMemory
			addr, stop, errCh := startTestServer(t, limited)

			// The server is already listening, so the deletions must be on disk
			// now, not at the next fsync or at shutdown.
			var logged []string
			for _, keys := range delCommandsInAOF(t, aofPath) {
				if len(keys) > maxKeysPerDel {
					t.Fatalf("a DEL in the append-only file names %d keys, want at most %d", len(keys), maxKeysPerDel)
				}
				logged = append(logged, keys...)
			}
			live := liveStartupEvictionKeys(t, addr, tt.keys)
			stop()
			waitForServerStop(t, errCh)

			evicted := tt.keys - len(live)
			if evicted < tt.minEvicted || len(live) == 0 {
				t.Fatalf("%d of %d keys stayed live under the limit, want at least %d evicted and some live", len(live), tt.keys, tt.minEvicted)
			}
			if len(logged) != evicted {
				t.Fatalf("the append-only file deletes %d keys, want the %d the server evicted", len(logged), evicted)
			}

			restartAddr, restartStop, restartErrCh := startTestServer(t, testAOFConfig(aofPath))
			restarted := liveStartupEvictionKeys(t, restartAddr, tt.keys)
			restartStop()
			waitForServerStop(t, restartErrCh)

			if len(restarted) != len(live) {
				t.Fatalf("%d keys are live after restarting without the limit, want the %d that were live with it", len(restarted), len(live))
			}
			for key := range live {
				if !restarted[key] {
					t.Fatalf("key %q was live under the limit and is gone after a restart", key)
				}
			}
		})
	}
}

func testAOFConfig(aofPath string) config.Config {
	cfg := defaultTestConfig()
	cfg.AOFPath = aofPath
	cfg.AppendFsync = "always"
	return cfg
}

// TestInfoPersistenceReportsTheAOF covers INFO persistence over a real
// connection: whether an append-only file is being written and whether its last
// write succeeded.
func TestInfoPersistenceReportsTheAOF(t *testing.T) {
	infoPersistence := func(t *testing.T, cfg config.Config) string {
		t.Helper()

		addr, stop, errCh := startTestServer(t, cfg)
		defer func() {
			stop()
			waitForServerStop(t, errCh)
		}()
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatalf("Dial(%q) error = %v", addr, err)
		}
		defer closeTestResource(t, conn)
		parser := protocol.NewParser(conn)

		assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "OK"}, "SET", "k", "v")
		if err := protocol.WriteValue(conn, request("INFO", "persistence")); err != nil {
			t.Fatalf("WriteValue(INFO) error = %v", err)
		}
		got, err := parser.Parse()
		if err != nil {
			t.Fatalf("Parse() error = %v", err)
		}
		text, _, ok := integrationBulkStringContent(got)
		if !ok {
			t.Fatalf("INFO persistence response type = %T, want a bulk string", got)
		}
		return text
	}

	t.Run("with an append-only file", func(t *testing.T) {
		text := infoPersistence(t, testAOFConfig(filepath.Join(t.TempDir(), "appendonly.aof")))
		for _, want := range []string{"# Persistence", "aof_enabled:1", "aof_last_write_status:ok"} {
			if !strings.Contains(text, want) {
				t.Fatalf("INFO persistence = %q, missing %q", text, want)
			}
		}
	})

	t.Run("without one", func(t *testing.T) {
		text := infoPersistence(t, defaultTestConfig())
		for _, want := range []string{"# Persistence", "aof_enabled:0", "aof_last_write_status:ok"} {
			if !strings.Contains(text, want) {
				t.Fatalf("INFO persistence = %q, missing %q", text, want)
			}
		}
	})
}
