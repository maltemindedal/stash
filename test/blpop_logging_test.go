package test

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/maltemindedal/stash/internal/command"
	stashlogger "github.com/maltemindedal/stash/internal/logger"
	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/server"
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
	srv := server.New(cfg, logger, store, command.NewExecutor(store, logger))
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
