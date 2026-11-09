package test

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/maltemindedal/stash/internal/config"
	stashlogger "github.com/maltemindedal/stash/internal/logger"
	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/storage"
)

// TestAReplicaRestartedWithoutItsMasterKeepsTheResyncedKeyspace gives a Replica
// an append-only file that holds a key from before it attached, lets it take a
// full resync and a command of the stream, and starts a server on that file
// alone. It must hold the Master's keyspace: the full resync replaced the
// Replica's keyspace, so its file is rewritten from the new one. The file used
// to keep its old history, and a restart replayed the stale key back beside
// the stream's commands.
func TestAReplicaRestartedWithoutItsMasterKeepsTheResyncedKeyspace(t *testing.T) {
	aofPath := filepath.Join(t.TempDir(), "replica.aof")
	// What the Replica's file holds from before it attached to this Master.
	if err := os.WriteFile(aofPath, []byte("*3\r\n$3\r\nSET\r\n$5\r\nstale\r\n$1\r\n1\r\n"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	logger := stashlogger.New("error")

	start := func(cfg config.Config) (addr string, stop func()) {
		t.Helper()
		srv := newServer(t, cfg, logger, storage.NewStore())
		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error, 1)
		go func() { errCh <- srv.ListenAndServe(ctx) }()
		stopped := false
		stop = func() {
			if !stopped {
				stopped = true
				cancel()
				waitForServerStop(t, errCh)
			}
		}
		t.Cleanup(stop)
		return waitForAddr(t, srv), stop
	}
	dial := func(addr string) (net.Conn, *protocol.Parser) {
		t.Helper()
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatalf("Dial(%q) error = %v", addr, err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		return conn, protocol.NewParser(conn)
	}
	null := protocol.BulkString{Null: true}
	bulk := func(value string) protocol.BulkString { return protocol.BulkString{Data: []byte(value)} }

	masterAddr, stopMaster := start(defaultTestConfig())
	masterConn, masterParser := dial(masterAddr)
	assertCommandResponse(t, masterConn, masterParser, protocol.SimpleString{Value: "OK"}, "SET", "fresh", "from the snapshot")

	replicaCfg := defaultTestConfig()
	replicaCfg.AOFPath = aofPath
	replicaCfg.ReplicaOf = masterAddr
	replicaAddr, stopReplica := start(replicaCfg)
	replicaConn, replicaParser := dial(replicaAddr)
	assertEventuallyCommandResponse(t, replicaConn, replicaParser, bulk("from the snapshot"), 5*time.Second, "GET", "fresh")
	assertCommandResponse(t, replicaConn, replicaParser, null, "GET", "stale")

	assertCommandResponse(t, masterConn, masterParser, protocol.SimpleString{Value: "OK"}, "SET", "later", "from the stream")
	assertEventuallyCommandResponse(t, replicaConn, replicaParser, bulk("from the stream"), 5*time.Second, "GET", "later")

	// The rewrite runs in the background; the file has followed the full resync
	// once the stale key's history is out of it.
	deadline := time.Now().Add(10 * time.Second)
	for {
		contents, err := os.ReadFile(aofPath)
		if err != nil {
			t.Fatalf("ReadFile() error = %v", err)
		}
		if !strings.Contains(string(contents), "stale") && strings.Contains(string(contents), "fresh") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the Replica's append-only file still holds the history from before its full resync after 10s:\n%q", contents)
		}
		time.Sleep(10 * time.Millisecond)
	}

	stopReplica()
	stopMaster()

	restartedCfg := defaultTestConfig()
	restartedCfg.AOFPath = aofPath
	restartedAddr, _ := start(restartedCfg)
	conn, parser := dial(restartedAddr)
	assertCommandResponse(t, conn, parser, null, "GET", "stale")
	assertCommandResponse(t, conn, parser, bulk("from the snapshot"), "GET", "fresh")
	assertCommandResponse(t, conn, parser, bulk("from the stream"), "GET", "later")
}
