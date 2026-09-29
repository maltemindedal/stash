package test

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

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
