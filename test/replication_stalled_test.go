package test

import (
	"context"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/maltemindedal/stash/internal/command"
	stashlogger "github.com/maltemindedal/stash/internal/logger"
	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/rdb"
	"github.com/maltemindedal/stash/internal/server"
	"github.com/maltemindedal/stash/internal/storage"
)

func TestAStalledReplicaDoesNotBlockWritersOnTheMaster(t *testing.T) {
	// A replica that stops reading fills its socket buffers. Writers used to write
	// to every replica's socket themselves, so the first one to hit the full
	// buffer, and then every other, stopped until the replica read again.
	cfg := defaultTestConfig()
	logger := stashlogger.New(cfg.LogLevel)
	store := storage.NewStore()
	srv := server.New(cfg, logger, store, command.NewExecutor(store, logger))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe(ctx) }()
	addr := waitForAddr(t, srv)

	attachTestReplica(t, srv, addr) // never reads what the master sends it

	client, clientParser := dialClient(t, addr)
	value := strings.Repeat("v", 512<<10)
	started := time.Now()
	const writes = 80 // 40 MiB, far past what the socket buffers hold
	for i := 0; i < writes; i++ {
		if err := client.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatalf("SetDeadline() error = %v", err)
		}
		if err := protocol.WriteValue(client, request("SET", "big", value)); err != nil {
			t.Fatalf("write %d error = %v (a stalled replica is blocking the master's writers)", i, err)
		}
		if _, err := clientParser.Parse(); err != nil {
			t.Fatalf("reply %d error = %v (a stalled replica is blocking the master's writers)", i, err)
		}
	}
	if elapsed := time.Since(started); elapsed > 10*time.Second {
		t.Fatalf("%d writes took %v behind a stalled replica", writes, elapsed)
	}
	if srv.ReplicaCount() != 1 {
		t.Fatalf("ReplicaCount() = %d, want the stalled replica still registered (it is 40 MiB behind, under the limit)", srv.ReplicaCount())
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("ListenAndServe() error = %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the master did not shut down with a stalled replica attached")
	}
}

func TestReplicaReconnectsAndResynchronisesWhenItsMasterDrops(t *testing.T) {
	// A replica used to attach once. When its master went away it stayed
	// disconnected, serving stale data, until it was restarted. Now it retries and
	// starts from a full resynchronisation, so what it held from the old
	// connection is replaced by what the master has now.
	masterListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	defer closeTestResource(t, masterListener)

	handshakes := make(chan int, 4)
	stopMaster := make(chan struct{})
	go func() {
		for generation := 1; ; generation++ {
			conn, err := masterListener.Accept()
			if err != nil {
				return
			}
			go func(generation int, conn net.Conn) {
				defer func() { _ = conn.Close() }()
				parser := protocol.NewParser(conn)
				reply := func(value protocol.Value) bool { return protocol.WriteValue(conn, value) == nil }

				if assertReplicaRequest(parser, "PING") != nil || !reply(protocol.SimpleString{Value: "PONG"}) {
					return
				}
				if _, err := decodeReplicaRequest(parser); err != nil || !reply(protocol.SimpleString{Value: "OK"}) { // REPLCONF
					return
				}
				if assertReplicaRequest(parser, "PSYNC", "?", "-1") != nil {
					return
				}
				snapshot, _ := rdb.BuildSnapshot([]storage.StringSnapshotEntry{{Key: "generation", Value: []byte(strconv.Itoa(generation))}})
				if !reply(protocol.SimpleString{Value: "FULLRESYNC test-replid 0"}) || !reply(protocol.BulkString{Data: snapshot}) {
					return
				}
				if generation == 1 {
					// One write after the snapshot, then the master goes away.
					if !reply(request("SET", "only-on-the-first", "yes")) {
						return
					}
					time.Sleep(100 * time.Millisecond)
					handshakes <- generation
					return
				}
				handshakes <- generation
				<-stopMaster
			}(generation, conn)
		}
	}()
	defer close(stopMaster)

	cfg := defaultTestConfig()
	cfg.ReplicaOf = masterListener.Addr().String()
	logger := stashlogger.New(cfg.LogLevel)
	store := storage.NewStore()
	srv := server.New(cfg, logger, store, command.NewExecutor(store, logger))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe(ctx) }()
	replicaAddr := waitForAddr(t, srv)

	for want := 1; want <= 2; want++ {
		select {
		case got := <-handshakes:
			if got != want {
				t.Fatalf("handshake %d completed, want %d", got, want)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("the replica did not complete handshake %d (it does not reconnect)", want)
		}
	}

	conn, parser := dialClient(t, replicaAddr)
	assertEventuallyCommandResponse(t, conn, parser, protocol.BulkString{Data: []byte("2")}, 3*time.Second, "GET", "generation")
	assertCommandResponse(t, conn, parser, protocol.BulkString{Null: true}, "GET", "only-on-the-first")

	cancel()
	waitForServerStop(t, errCh)
}
