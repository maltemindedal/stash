package test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	stashlogger "github.com/maltemindedal/stash/internal/logger"
	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/storage"
)

func TestBLPopIsLoggedAndReplicatedAsAPop(t *testing.T) {
	// BLPOP removes an element, but it was neither logged nor replicated, so a
	// restart brought the element back from the RPUSH that had added it, and a
	// replica kept it.
	aofPath := filepath.Join(t.TempDir(), "appendonly.aof")
	cfg := testAOFConfig(aofPath)

	logger := stashlogger.New(cfg.LogLevel)
	store := storage.NewStore()
	srv := newServer(t, cfg, logger, store)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe(ctx) }()
	addr := waitForAddr(t, srv)

	replicaConn, replicaParser := attachTestReplica(t, srv, addr)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) error = %v", addr, err)
	}
	defer closeTestResource(t, conn)
	parser := protocol.NewParser(conn)

	// One BLPOP that finds an element at once, and one that waits for it.
	assertCommandResponse(t, conn, parser, protocol.Integer{Value: 1}, "RPUSH", "jobs", "immediate")
	assertCommandResponse(t, conn, parser, protocol.Array{Elements: []protocol.Value{
		protocol.BulkString{Data: []byte("jobs")}, protocol.BulkString{Data: []byte("immediate")},
	}}, "BLPOP", "jobs")

	waiting, waitingParser := dialClient(t, addr)
	if err := protocol.WriteValue(waiting, request("BLPOP", "jobs")); err != nil {
		t.Fatalf("WriteValue(BLPOP) error = %v", err)
	}
	waitForClients(t, conn, parser, 3, 2*time.Second, "the waiting client should be connected")
	assertCommandResponse(t, conn, parser, protocol.Integer{Value: 1}, "RPUSH", "jobs", "delayed")
	if err := waiting.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline() error = %v", err)
	}
	if _, err := waitingParser.Parse(); err != nil {
		t.Fatalf("Parse(BLPOP reply) error = %v", err)
	}

	// The replica sees the pushes and both pops, in that order.
	for _, want := range [][]string{
		{"RPUSH", "jobs", "immediate"},
		{"LPOP", "jobs"},
		{"RPUSH", "jobs", "delayed"},
		{"LPOP", "jobs"},
	} {
		if err := assertReplicaRequest(replicaParser, want[0], want[1:]...); err != nil {
			t.Fatalf("replica stream: %v (want %v)", err, want)
		}
	}
	_ = replicaConn

	// Both elements are gone after a restart.
	cancel()
	waitForServerStop(t, errCh)
	restartAddr, restartStop, restartErrCh := startTestServer(t, cfg)
	defer func() {
		restartStop()
		waitForServerStop(t, restartErrCh)
	}()
	restarted, restartedParser := dialClient(t, restartAddr)
	assertValuesEqual(t, roundTrip(t, restarted, restartedParser, "LRANGE", "jobs", "0", "-1"), protocol.Array{Elements: []protocol.Value{}})
}

func TestAPushThatServesSeveralBlockedClientsIsLoggedWithAPopForEach(t *testing.T) {
	// Each blocked client the push serves pops an element, and each pop is logged
	// as LPOP, so the log replays to the list the clients left behind.
	tests := []struct {
		name string
		push []string
	}{
		{name: "two elements for two clients", push: []string{"RPUSH", "q", "a", "b"}},
		{name: "three elements for two clients", push: []string{"RPUSH", "q", "a", "b", "c"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			aofPath := filepath.Join(t.TempDir(), "appendonly.aof")
			addr, stop, errCh := startTestServer(t, testAOFConfig(aofPath))
			admin, adminParser := dialClient(t, addr)

			first, firstParser := blockOn(t, addr, "q")
			second, secondParser := blockOn(t, addr, "q")
			assertCommandResponse(t, admin, adminParser, protocol.Integer{Value: int64(len(tt.push) - 2)}, tt.push...)
			readReplyWithin(t, first, firstParser, 2*time.Second, "BLPOP reply to the first client")
			readReplyWithin(t, second, secondParser, 2*time.Second, "BLPOP reply to the second client")
			stop()
			waitForServerStop(t, errCh)

			assertAOFHolds(t, aofPath, tt.push, []string{"LPOP", "q"}, []string{"LPOP", "q"})
		})
	}
}

// assertAOFHolds reads the append-only file at path and checks that it holds the
// commands in want, in order, and nothing after them.
func assertAOFHolds(t *testing.T, path string, want ...[]string) {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%q) error = %v", path, err)
	}
	logged := protocol.NewParser(bytes.NewReader(data))
	for _, command := range want {
		if err := assertReplicaRequest(logged, command[0], command[1:]...); err != nil {
			t.Fatalf("AOF %q: %v (want %v)", data, err, command)
		}
	}
	if _, err := logged.Parse(); !errors.Is(err, io.EOF) {
		t.Fatalf("AOF %q: Parse() after %d commands error = %v, want io.EOF", data, len(want), err)
	}
}
