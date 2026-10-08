package test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maltemindedal/stash/internal/command"
	"github.com/maltemindedal/stash/internal/config"
	stashlogger "github.com/maltemindedal/stash/internal/logger"
	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/rdb"
	"github.com/maltemindedal/stash/internal/server"
	"github.com/maltemindedal/stash/internal/storage"
)

func TestServerHandlesMasterReplicaHandshake(t *testing.T) {
	cfg := defaultTestConfig()
	cfg.RequirePass = "secret"

	logger := stashlogger.New(cfg.LogLevel)
	store := storage.NewStore()
	executor := command.NewExecutor(store, logger)
	srv := server.New(cfg, logger, store, executor)

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
	defer closeTestResource(t, conn)

	parser := protocol.NewParser(conn)
	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "PONG"}, "PING")
	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "OK"}, "AUTH", "secret")
	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "OK"}, "REPLCONF", "listening-port", "6380")

	if err := protocol.WriteValue(conn, request("PSYNC", "?", "-1")); err != nil {
		t.Fatalf("WriteValue(PSYNC) error = %v", err)
	}

	fullResyncRaw, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse() FULLRESYNC error = %v", err)
	}
	fullResync, ok := fullResyncRaw.(protocol.SimpleString)
	if !ok {
		t.Fatalf("FULLRESYNC type = %T, want protocol.SimpleString", fullResyncRaw)
	}
	parts := strings.Fields(fullResync.Value)
	if len(parts) != 3 || parts[0] != "FULLRESYNC" || parts[1] == "" || parts[2] != "0" {
		t.Fatalf("FULLRESYNC payload = %q, want 'FULLRESYNC <replid> 0'", fullResync.Value)
	}

	snapshotRaw, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse() snapshot error = %v", err)
	}
	snapshot, ok := snapshotRaw.(protocol.BulkString)
	if !ok {
		t.Fatalf("snapshot type = %T, want protocol.BulkString", snapshotRaw)
	}
	if snapshot.Null {
		t.Fatal("snapshot bulk string unexpectedly null")
	}
	if string(snapshot.Data) != string(rdb.EmptySnapshot()) {
		t.Fatalf("snapshot payload = %q, want %q", string(snapshot.Data), string(rdb.EmptySnapshot()))
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if srv.ReplicaCount() == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if srv.ReplicaCount() != 1 {
		t.Fatalf("ReplicaCount() = %d, want 1", srv.ReplicaCount())
	}

	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "PONG"}, "PING")

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

func TestServerReplicaModeInitiatesHandshake(t *testing.T) {
	t.Run("unprotected master", func(t *testing.T) {
		masterListener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("Listen() fake master error = %v", err)
		}
		defer closeTestResource(t, masterListener)

		listeningPortCh := make(chan string, 1)
		handshakeDone := make(chan struct{})
		masterStop := make(chan struct{})
		masterErrCh := make(chan error, 1)

		go func() {
			conn, err := masterListener.Accept()
			if err != nil {
				masterErrCh <- err
				return
			}
			defer closeTestResource(t, conn)

			parser := protocol.NewParser(conn)
			writer := conn

			if err := assertReplicaRequest(parser, "PING"); err != nil {
				masterErrCh <- err
				return
			}
			if err := protocol.WriteValue(writer, protocol.SimpleString{Value: "PONG"}); err != nil {
				masterErrCh <- err
				return
			}

			replconf, err := decodeReplicaRequest(parser)
			if err != nil {
				masterErrCh <- err
				return
			}
			if replconf.Name != "REPLCONF" {
				masterErrCh <- fmt.Errorf("second request = %q, want REPLCONF", replconf.Name)
				return
			}
			if len(replconf.Args) != 2 || !strings.EqualFold(string(replconf.Args[0]), "listening-port") {
				masterErrCh <- fmt.Errorf("REPLCONF args = %q, want listening-port <port>", replconf.Args)
				return
			}
			listeningPortCh <- string(replconf.Args[1])
			if err := protocol.WriteValue(writer, protocol.SimpleString{Value: "OK"}); err != nil {
				masterErrCh <- err
				return
			}

			if err := assertReplicaRequest(parser, "PSYNC", "?", "-1"); err != nil {
				masterErrCh <- err
				return
			}
			if err := protocol.WriteValue(writer, protocol.SimpleString{Value: "FULLRESYNC test-replid 0"}); err != nil {
				masterErrCh <- err
				return
			}
			if err := protocol.WriteValue(writer, protocol.BulkString{Data: rdb.EmptySnapshot()}); err != nil {
				masterErrCh <- err
				return
			}

			close(handshakeDone)
			<-masterStop
			masterErrCh <- nil
		}()

		cfg := defaultTestConfig()
		cfg.ReplicaOf = masterListener.Addr().String()

		logger := stashlogger.New(cfg.LogLevel)
		store := storage.NewStore()
		executor := command.NewExecutor(store, logger)
		srv := server.New(cfg, logger, store, executor)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		serverErrCh := make(chan error, 1)
		go func() {
			serverErrCh <- srv.ListenAndServe(ctx)
		}()

		replicaAddr := waitForAddr(t, srv)
		_, replicaPortText, err := net.SplitHostPort(replicaAddr)
		if err != nil {
			t.Fatalf("SplitHostPort(%q) error = %v", replicaAddr, err)
		}

		select {
		case announcedPort := <-listeningPortCh:
			if announcedPort != replicaPortText {
				t.Fatalf("REPLCONF listening-port = %q, want %q", announcedPort, replicaPortText)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("replica did not send REPLCONF in time")
		}

		select {
		case <-handshakeDone:
		case <-time.After(2 * time.Second):
			t.Fatal("replica did not complete handshake in time")
		}

		clientConn, err := net.Dial("tcp", replicaAddr)
		if err != nil {
			t.Fatalf("Dial(%q) client error = %v", replicaAddr, err)
		}
		defer closeTestResource(t, clientConn)
		clientParser := protocol.NewParser(clientConn)
		assertCommandResponse(t, clientConn, clientParser, protocol.SimpleString{Value: "PONG"}, "PING")

		close(masterStop)

		select {
		case err := <-masterErrCh:
			if err != nil {
				t.Fatalf("fake master error = %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("fake master did not stop within timeout")
		}

		cancel()
		select {
		case err := <-serverErrCh:
			if err != nil {
				t.Fatalf("ListenAndServe() error = %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("replica server did not stop within timeout")
		}
	})

	t.Run("protected master with masterauth", func(t *testing.T) {
		masterListener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("Listen() fake master error = %v", err)
		}
		defer closeTestResource(t, masterListener)

		listeningPortCh := make(chan string, 1)
		handshakeDone := make(chan struct{})
		masterStop := make(chan struct{})
		masterErrCh := make(chan error, 1)

		go func() {
			conn, err := masterListener.Accept()
			if err != nil {
				masterErrCh <- err
				return
			}
			defer closeTestResource(t, conn)

			parser := protocol.NewParser(conn)
			writer := conn

			if err := assertReplicaRequest(parser, "PING"); err != nil {
				masterErrCh <- err
				return
			}
			if err := protocol.WriteValue(writer, protocol.SimpleString{Value: "PONG"}); err != nil {
				masterErrCh <- err
				return
			}

			if err := assertReplicaRequest(parser, "AUTH", "secret"); err != nil {
				masterErrCh <- err
				return
			}
			if err := protocol.WriteValue(writer, protocol.SimpleString{Value: "OK"}); err != nil {
				masterErrCh <- err
				return
			}

			replconf, err := decodeReplicaRequest(parser)
			if err != nil {
				masterErrCh <- err
				return
			}
			if replconf.Name != "REPLCONF" {
				masterErrCh <- fmt.Errorf("third request = %q, want REPLCONF", replconf.Name)
				return
			}
			if len(replconf.Args) != 2 || !strings.EqualFold(string(replconf.Args[0]), "listening-port") {
				masterErrCh <- fmt.Errorf("REPLCONF args = %q, want listening-port <port>", replconf.Args)
				return
			}
			listeningPortCh <- string(replconf.Args[1])
			if err := protocol.WriteValue(writer, protocol.SimpleString{Value: "OK"}); err != nil {
				masterErrCh <- err
				return
			}

			if err := assertReplicaRequest(parser, "PSYNC", "?", "-1"); err != nil {
				masterErrCh <- err
				return
			}
			if err := protocol.WriteValue(writer, protocol.SimpleString{Value: "FULLRESYNC test-replid 0"}); err != nil {
				masterErrCh <- err
				return
			}
			if err := protocol.WriteValue(writer, protocol.BulkString{Data: rdb.EmptySnapshot()}); err != nil {
				masterErrCh <- err
				return
			}

			close(handshakeDone)
			<-masterStop
			masterErrCh <- nil
		}()

		cfg := config.Default()
		cfg.Host = "127.0.0.1"
		cfg.Port = 0
		cfg.LogLevel = "error"
		cfg.EvictionInterval = 5 * time.Millisecond
		cfg.EvictionSampleSize = 10
		cfg.DumpPath = ""
		cfg.ReplicaOf = masterListener.Addr().String()
		cfg.MasterAuth = "secret"

		logger := stashlogger.New(cfg.LogLevel)
		store := storage.NewStore()
		executor := command.NewExecutor(store, logger)
		srv := server.New(cfg, logger, store, executor)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		serverErrCh := make(chan error, 1)
		go func() {
			serverErrCh <- srv.ListenAndServe(ctx)
		}()

		replicaAddr := waitForAddr(t, srv)
		_, replicaPortText, err := net.SplitHostPort(replicaAddr)
		if err != nil {
			t.Fatalf("SplitHostPort(%q) error = %v", replicaAddr, err)
		}

		select {
		case announcedPort := <-listeningPortCh:
			if announcedPort != replicaPortText {
				t.Fatalf("REPLCONF listening-port = %q, want %q", announcedPort, replicaPortText)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("replica did not send authenticated REPLCONF in time")
		}

		select {
		case <-handshakeDone:
		case <-time.After(2 * time.Second):
			t.Fatal("replica did not complete authenticated handshake in time")
		}

		clientConn, err := net.Dial("tcp", replicaAddr)
		if err != nil {
			t.Fatalf("Dial(%q) client error = %v", replicaAddr, err)
		}
		defer closeTestResource(t, clientConn)
		clientParser := protocol.NewParser(clientConn)
		assertCommandResponse(t, clientConn, clientParser, protocol.SimpleString{Value: "PONG"}, "PING")

		close(masterStop)

		select {
		case err := <-masterErrCh:
			if err != nil {
				t.Fatalf("fake master error = %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("fake master did not stop within timeout")
		}

		cancel()
		select {
		case err := <-serverErrCh:
			if err != nil {
				t.Fatalf("ListenAndServe() error = %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("replica server did not stop within timeout")
		}
	})

	t.Run("protected master without masterauth fails before PSYNC and keeps replica server alive", func(t *testing.T) {
		masterListener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("Listen() fake master error = %v", err)
		}
		defer closeTestResource(t, masterListener)

		handshakeFailed := make(chan struct{})
		masterErrCh := make(chan error, 1)

		go func() {
			conn, err := masterListener.Accept()
			if err != nil {
				masterErrCh <- err
				return
			}
			defer closeTestResource(t, conn)

			parser := protocol.NewParser(conn)
			writer := conn

			if err := assertReplicaRequest(parser, "PING"); err != nil {
				masterErrCh <- err
				return
			}
			if err := protocol.WriteValue(writer, protocol.SimpleString{Value: "PONG"}); err != nil {
				masterErrCh <- err
				return
			}

			replconf, err := decodeReplicaRequest(parser)
			if err != nil {
				masterErrCh <- err
				return
			}
			if replconf.Name != "REPLCONF" {
				masterErrCh <- fmt.Errorf("second request = %q, want REPLCONF", replconf.Name)
				return
			}
			if len(replconf.Args) != 2 || !strings.EqualFold(string(replconf.Args[0]), "listening-port") {
				masterErrCh <- fmt.Errorf("REPLCONF args = %q, want listening-port <port>", replconf.Args)
				return
			}
			if err := protocol.WriteValue(writer, protocol.ErrorValue{Message: "NOAUTH Authentication required."}); err != nil {
				masterErrCh <- err
				return
			}

			if err := assertNoFurtherReplicaRequests(conn, parser, 300*time.Millisecond); err != nil {
				masterErrCh <- err
				return
			}

			close(handshakeFailed)
			masterErrCh <- nil
		}()

		var replicaLogs synchronizedBuffer
		replicaLogger := slog.New(slog.NewTextHandler(&replicaLogs, &slog.HandlerOptions{Level: slog.LevelDebug}))

		cfg := config.Default()
		cfg.Host = "127.0.0.1"
		cfg.Port = 0
		cfg.LogLevel = "debug"
		cfg.EvictionInterval = 5 * time.Millisecond
		cfg.EvictionSampleSize = 10
		cfg.DumpPath = ""
		cfg.ReplicaOf = masterListener.Addr().String()

		store := storage.NewStore()
		executor := command.NewExecutor(store, replicaLogger)
		srv := server.New(cfg, replicaLogger, store, executor)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		serverErrCh := make(chan error, 1)
		go func() {
			serverErrCh <- srv.ListenAndServe(ctx)
		}()

		replicaAddr := waitForAddr(t, srv)

		select {
		case <-handshakeFailed:
		case <-time.After(2 * time.Second):
			t.Fatal("replica did not fail unauthenticated protected-master handshake in time")
		}

		clientConn, err := net.Dial("tcp", replicaAddr)
		if err != nil {
			t.Fatalf("Dial(%q) client error = %v", replicaAddr, err)
		}
		defer closeTestResource(t, clientConn)
		clientParser := protocol.NewParser(clientConn)
		assertCommandResponse(t, clientConn, clientParser, protocol.SimpleString{Value: "PONG"}, "PING")

		waitForLogFragments(t, &replicaLogs, 2*time.Second,
			"replica REPLCONF failed",
			"protected master requires replica authentication (--masterauth)",
			"NOAUTH Authentication required.",
		)
		if strings.Contains(replicaLogs.String(), "replica handshake completed") {
			t.Fatalf("replica logs unexpectedly contain completed handshake:\n%s", replicaLogs.String())
		}

		select {
		case err := <-masterErrCh:
			if err != nil {
				t.Fatalf("fake master error = %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("fake master did not stop within timeout")
		}

		cancel()
		select {
		case err := <-serverErrCh:
			if err != nil {
				t.Fatalf("ListenAndServe() error = %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("replica server did not stop within timeout")
		}
	})

	t.Run("protected master with wrong masterauth fails after AUTH and keeps replica server alive", func(t *testing.T) {
		masterListener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("Listen() fake master error = %v", err)
		}
		defer closeTestResource(t, masterListener)

		handshakeFailed := make(chan struct{})
		masterErrCh := make(chan error, 1)

		go func() {
			conn, err := masterListener.Accept()
			if err != nil {
				masterErrCh <- err
				return
			}
			defer closeTestResource(t, conn)

			parser := protocol.NewParser(conn)
			writer := conn

			if err := assertReplicaRequest(parser, "PING"); err != nil {
				masterErrCh <- err
				return
			}
			if err := protocol.WriteValue(writer, protocol.SimpleString{Value: "PONG"}); err != nil {
				masterErrCh <- err
				return
			}

			if err := assertReplicaRequest(parser, "AUTH", "wrong"); err != nil {
				masterErrCh <- err
				return
			}
			if err := protocol.WriteValue(writer, protocol.ErrorValue{Message: "WRONGPASS invalid username-password pair or user is disabled."}); err != nil {
				masterErrCh <- err
				return
			}

			if err := assertNoFurtherReplicaRequests(conn, parser, 300*time.Millisecond); err != nil {
				masterErrCh <- err
				return
			}

			close(handshakeFailed)
			masterErrCh <- nil
		}()

		var replicaLogs synchronizedBuffer
		replicaLogger := slog.New(slog.NewTextHandler(&replicaLogs, &slog.HandlerOptions{Level: slog.LevelDebug}))

		cfg := config.Default()
		cfg.Host = "127.0.0.1"
		cfg.Port = 0
		cfg.LogLevel = "debug"
		cfg.EvictionInterval = 5 * time.Millisecond
		cfg.EvictionSampleSize = 10
		cfg.DumpPath = ""
		cfg.ReplicaOf = masterListener.Addr().String()
		cfg.MasterAuth = "wrong"

		store := storage.NewStore()
		executor := command.NewExecutor(store, replicaLogger)
		srv := server.New(cfg, replicaLogger, store, executor)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		serverErrCh := make(chan error, 1)
		go func() {
			serverErrCh <- srv.ListenAndServe(ctx)
		}()

		replicaAddr := waitForAddr(t, srv)

		select {
		case <-handshakeFailed:
		case <-time.After(2 * time.Second):
			t.Fatal("replica did not fail protected-master AUTH in time")
		}

		clientConn, err := net.Dial("tcp", replicaAddr)
		if err != nil {
			t.Fatalf("Dial(%q) client error = %v", replicaAddr, err)
		}
		defer closeTestResource(t, clientConn)
		clientParser := protocol.NewParser(clientConn)
		assertCommandResponse(t, clientConn, clientParser, protocol.SimpleString{Value: "PONG"}, "PING")

		waitForLogFragments(t, &replicaLogs, 2*time.Second,
			"replica AUTH failed",
			"replica AUTH rejected by master",
			"WRONGPASS",
		)
		if strings.Contains(replicaLogs.String(), "replica handshake completed") {
			t.Fatalf("replica logs unexpectedly contain completed handshake:\n%s", replicaLogs.String())
		}

		select {
		case err := <-masterErrCh:
			if err != nil {
				t.Fatalf("fake master error = %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("fake master did not stop within timeout")
		}

		cancel()
		select {
		case err := <-serverErrCh:
			if err != nil {
				t.Fatalf("ListenAndServe() error = %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("replica server did not stop within timeout")
		}
	})
}

func TestServerReplicaFullResyncReplacesExistingData(t *testing.T) {
	masterListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() fake master error = %v", err)
	}
	defer closeTestResource(t, masterListener)

	handshakeDone := make(chan struct{})
	masterStop := make(chan struct{})
	masterErrCh := make(chan error, 1)
	snapshot := buildTestRDB(
		selectTestDB(0),
		testStringEntry([]byte("fresh"), []byte("from-master")),
	)

	go func() {
		conn, err := masterListener.Accept()
		if err != nil {
			masterErrCh <- err
			return
		}
		defer closeTestResource(t, conn)

		parser := protocol.NewParser(conn)
		writer := conn

		if err := assertReplicaRequest(parser, "PING"); err != nil {
			masterErrCh <- err
			return
		}
		if err := protocol.WriteValue(writer, protocol.SimpleString{Value: "PONG"}); err != nil {
			masterErrCh <- err
			return
		}

		replconf, err := decodeReplicaRequest(parser)
		if err != nil {
			masterErrCh <- err
			return
		}
		if replconf.Name != "REPLCONF" || len(replconf.Args) != 2 || !strings.EqualFold(string(replconf.Args[0]), "listening-port") {
			masterErrCh <- fmt.Errorf("REPLCONF args = %q, want listening-port <port>", replconf.Args)
			return
		}
		if string(replconf.Args[1]) == "0" {
			masterErrCh <- fmt.Errorf("REPLCONF listening-port = %q, want an ephemeral non-zero port", replconf.Args[1])
			return
		}
		if err := protocol.WriteValue(writer, protocol.SimpleString{Value: "OK"}); err != nil {
			masterErrCh <- err
			return
		}

		if err := assertReplicaRequest(parser, "PSYNC", "?", "-1"); err != nil {
			masterErrCh <- err
			return
		}
		if err := protocol.WriteValue(writer, protocol.SimpleString{Value: "FULLRESYNC test-replid 0"}); err != nil {
			masterErrCh <- err
			return
		}
		if err := protocol.WriteValue(writer, protocol.BulkString{Data: snapshot}); err != nil {
			masterErrCh <- err
			return
		}

		close(handshakeDone)
		<-masterStop
		masterErrCh <- nil
	}()

	rdbPath := writeTempRDBFile(t, buildTestRDB(
		selectTestDB(0),
		testStringEntry([]byte("stale"), []byte("local")),
	))

	cfg := config.Default()
	cfg.Host = "127.0.0.1"
	cfg.Port = 0
	cfg.LogLevel = "error"
	cfg.EvictionInterval = 5 * time.Millisecond
	cfg.EvictionSampleSize = 10
	cfg.DumpPath = ""
	cfg.RDBPath = rdbPath
	cfg.ReplicaOf = masterListener.Addr().String()

	logger := stashlogger.New(cfg.LogLevel)
	store := storage.NewStore()
	executor := command.NewExecutor(store, logger)
	srv := server.New(cfg, logger, store, executor)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serverErrCh := make(chan error, 1)
	go func() {
		serverErrCh <- srv.ListenAndServe(ctx)
	}()

	replicaAddr := waitForAddr(t, srv)

	select {
	case <-handshakeDone:
	case <-time.After(2 * time.Second):
		t.Fatal("replica did not complete FULLRESYNC in time")
	}

	clientConn, err := net.Dial("tcp", replicaAddr)
	if err != nil {
		t.Fatalf("Dial(%q) client error = %v", replicaAddr, err)
	}
	defer closeTestResource(t, clientConn)
	clientParser := protocol.NewParser(clientConn)

	// The master finishing its write does not mean the replica has applied the
	// snapshot yet, so wait for the master's data to appear. The keyspace is
	// replaced as a whole, so once it has, the old local key must be gone.
	assertEventuallyCommandResponse(t, clientConn, clientParser, protocol.BulkString{Data: []byte("from-master")}, 2*time.Second, "GET", "fresh")
	assertCommandResponse(t, clientConn, clientParser, protocol.BulkString{Null: true}, "GET", "stale")

	close(masterStop)

	select {
	case err := <-masterErrCh:
		if err != nil {
			t.Fatalf("fake master error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("fake master did not stop within timeout")
	}

	cancel()
	select {
	case err := <-serverErrCh:
		if err != nil {
			t.Fatalf("ListenAndServe() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("replica server did not stop within timeout")
	}
}

func assertReplicaRequest(parser *protocol.Parser, wantName string, wantArgs ...string) error {
	request, err := decodeReplicaRequest(parser)
	if err != nil {
		return err
	}
	if request.Name != wantName {
		return fmt.Errorf("request name = %q, want %q", request.Name, wantName)
	}
	if len(request.Args) != len(wantArgs) {
		return fmt.Errorf("request args len = %d, want %d", len(request.Args), len(wantArgs))
	}
	for i := range wantArgs {
		if string(request.Args[i]) != wantArgs[i] {
			return fmt.Errorf("request arg[%d] = %q, want %q", i, string(request.Args[i]), wantArgs[i])
		}
	}
	return nil
}

func decodeReplicaRequest(parser *protocol.Parser) (*command.Request, error) {
	value, err := parser.Parse()
	if err != nil {
		return nil, err
	}
	return command.DecodeRequest(value)
}

func assertNoFurtherReplicaRequests(conn net.Conn, parser *protocol.Parser, timeout time.Duration) error {
	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return err
	}
	defer func() { _ = conn.SetReadDeadline(time.Time{}) }()

	_, err := decodeReplicaRequest(parser)
	if err == nil {
		return fmt.Errorf("unexpected additional replica request")
	}

	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return nil
	}
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return nil
	}

	return fmt.Errorf("decode additional replica request: %w", err)
}

func waitForLogFragments(t *testing.T, logs *synchronizedBuffer, timeout time.Duration, fragments ...string) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		output := logs.String()
		matched := true
		for _, fragment := range fragments {
			if !strings.Contains(output, fragment) {
				matched = false
				break
			}
		}
		if matched {
			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("logs did not contain expected fragments %q within %v\nlogs:\n%s", fragments, timeout, logs.String())
}

func TestMasterPropagatesMutationsToReplica(t *testing.T) {
	masterCfg := config.Default()
	masterCfg.Host = "127.0.0.1"
	masterCfg.Port = 0
	masterCfg.LogLevel = "error"
	masterCfg.EvictionInterval = 5 * time.Millisecond
	masterCfg.EvictionSampleSize = 10
	masterCfg.DumpPath = ""

	logger := stashlogger.New(masterCfg.LogLevel)

	masterStore := storage.NewStore()
	masterExecutor := command.NewExecutor(masterStore, logger)
	master := server.New(masterCfg, logger, masterStore, masterExecutor)

	masterCtx, cancelMaster := context.WithCancel(context.Background())
	defer cancelMaster()

	masterErrCh := make(chan error, 1)
	go func() {
		masterErrCh <- master.ListenAndServe(masterCtx)
	}()

	masterAddr := waitForAddr(t, master)

	replicaCfg := config.Default()
	replicaCfg.Host = "127.0.0.1"
	replicaCfg.Port = 0
	replicaCfg.LogLevel = "error"
	replicaCfg.EvictionInterval = 5 * time.Millisecond
	replicaCfg.EvictionSampleSize = 10
	replicaCfg.DumpPath = ""
	replicaCfg.ReplicaOf = masterAddr

	replicaStore := storage.NewStore()
	replicaExecutor := command.NewExecutor(replicaStore, logger)
	replica := server.New(replicaCfg, logger, replicaStore, replicaExecutor)

	replicaCtx, cancelReplica := context.WithCancel(context.Background())
	defer cancelReplica()

	replicaErrCh := make(chan error, 1)
	go func() {
		replicaErrCh <- replica.ListenAndServe(replicaCtx)
	}()

	replicaAddr := waitForAddr(t, replica)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if master.ReplicaCount() == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if master.ReplicaCount() != 1 {
		t.Fatalf("ReplicaCount() = %d, want 1", master.ReplicaCount())
	}

	masterConn, err := net.Dial("tcp", masterAddr)
	if err != nil {
		t.Fatalf("Dial(%q) master error = %v", masterAddr, err)
	}
	defer closeTestResource(t, masterConn)
	masterParser := protocol.NewParser(masterConn)

	replicaConn, err := net.Dial("tcp", replicaAddr)
	if err != nil {
		t.Fatalf("Dial(%q) replica error = %v", replicaAddr, err)
	}
	defer closeTestResource(t, replicaConn)
	replicaParser := protocol.NewParser(replicaConn)

	assertCommandResponse(t, masterConn, masterParser, protocol.SimpleString{Value: "OK"}, "SET", "name", "Stash")
	assertEventuallyCommandResponse(t, replicaConn, replicaParser, protocol.BulkString{Data: []byte("Stash")}, 2*time.Second, "GET", "name")

	assertCommandResponse(t, masterConn, masterParser, protocol.Integer{Value: 1}, "DEL", "name")
	assertEventuallyCommandResponse(t, replicaConn, replicaParser, protocol.BulkString{Null: true}, 2*time.Second, "GET", "name")

	assertCommandResponse(t, masterConn, masterParser, protocol.SimpleString{Value: "OK"}, "MULTI")
	assertCommandResponse(t, masterConn, masterParser, protocol.SimpleString{Value: "QUEUED"}, "SET", "tx-key", "1")
	assertCommandResponse(t, masterConn, masterParser, protocol.SimpleString{Value: "QUEUED"}, "DEL", "missing")
	assertCommandResponse(t, masterConn, masterParser, protocol.Array{Elements: []protocol.Value{
		protocol.SimpleString{Value: "OK"},
		protocol.Integer{Value: 0},
	}}, "EXEC")
	assertEventuallyCommandResponse(t, replicaConn, replicaParser, protocol.BulkString{Data: []byte("1")}, 2*time.Second, "GET", "tx-key")

	assertCommandResponse(t, masterConn, masterParser, protocol.SimpleString{Value: "OK"}, "MULTI")
	assertCommandResponse(t, masterConn, masterParser, protocol.SimpleString{Value: "QUEUED"}, "SET", "tx-bad", "hello")
	assertCommandResponse(t, masterConn, masterParser, protocol.SimpleString{Value: "QUEUED"}, "INCR", "tx-bad")
	assertCommandResponse(t, masterConn, masterParser, protocol.SimpleString{Value: "QUEUED"}, "SET", "tx-good", "1")
	assertCommandResponse(t, masterConn, masterParser, protocol.Array{Elements: []protocol.Value{
		protocol.SimpleString{Value: "OK"},
		protocol.ErrorValue{Message: "ERR value is not an integer or out of range"},
		protocol.SimpleString{Value: "OK"},
	}}, "EXEC")
	assertEventuallyCommandResponse(t, replicaConn, replicaParser, protocol.BulkString{Data: []byte("hello")}, 2*time.Second, "GET", "tx-bad")
	assertEventuallyCommandResponse(t, replicaConn, replicaParser, protocol.BulkString{Data: []byte("1")}, 2*time.Second, "GET", "tx-good")

	assertCommandResponse(t, masterConn, masterParser, protocol.SimpleString{Value: "OK"}, "MULTI")
	assertCommandResponse(t, masterConn, masterParser, protocol.ErrorValue{Message: "ERR unknown command \"NOPE\""}, "NOPE")
	assertCommandResponse(t, masterConn, masterParser, protocol.SimpleString{Value: "QUEUED"}, "SET", "tx-abort", "1")
	assertCommandResponse(t, masterConn, masterParser, protocol.ErrorValue{Message: "EXECABORT Transaction discarded because of previous errors."}, "EXEC")
	assertEventuallyCommandResponse(t, replicaConn, replicaParser, protocol.BulkString{Null: true}, 2*time.Second, "GET", "tx-abort")

	watcherConn, err := net.Dial("tcp", masterAddr)
	if err != nil {
		t.Fatalf("Dial(%q) watcher error = %v", masterAddr, err)
	}
	defer closeTestResource(t, watcherConn)
	watcherParser := protocol.NewParser(watcherConn)

	assertCommandResponse(t, watcherConn, watcherParser, protocol.SimpleString{Value: "OK"}, "WATCH", "tx-watch")
	assertCommandResponse(t, masterConn, masterParser, protocol.SimpleString{Value: "OK"}, "SET", "tx-watch", "1")
	assertCommandResponse(t, watcherConn, watcherParser, protocol.SimpleString{Value: "OK"}, "MULTI")
	assertCommandResponse(t, watcherConn, watcherParser, protocol.SimpleString{Value: "QUEUED"}, "SET", "tx-watch", "2")
	assertCommandResponse(t, watcherConn, watcherParser, protocol.Array{Null: true}, "EXEC")
	assertEventuallyCommandResponse(t, replicaConn, replicaParser, protocol.BulkString{Data: []byte("1")}, 2*time.Second, "GET", "tx-watch")

	cancelReplica()
	select {
	case err := <-replicaErrCh:
		if err != nil {
			t.Fatalf("replica ListenAndServe() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("replica did not stop within timeout")
	}

	cancelMaster()
	select {
	case err := <-masterErrCh:
		if err != nil {
			t.Fatalf("master ListenAndServe() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("master did not stop within timeout")
	}
}

// TestMasterPropagatesActiveEvictionOfExpiredKey pins that a key the active TTL
// eviction loop removes is treated as the keyspace mutation it is: the master
// sends a DEL naming it to its replicas, records that DEL in its AOF, and
// invalidates any WATCH guarding the key. Without that, a replica keeps serving
// a key the master has already dropped, and replaying the AOF resurrects it.
func TestMasterPropagatesActiveEvictionOfExpiredKey(t *testing.T) {
	aofPath := filepath.Join(t.TempDir(), "appendonly.aof")
	cfg := testAOFConfig(aofPath)

	logger := stashlogger.New(cfg.LogLevel)
	store := storage.NewStore()
	executor := command.NewExecutor(store, logger)
	srv := server.New(cfg, logger, store, executor)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.ListenAndServe(ctx)
	}()

	addr := waitForAddr(t, srv)
	replicaConn, replicaParser := attachTestReplica(t, srv, addr)

	clientConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) client error = %v", addr, err)
	}
	defer closeTestResource(t, clientConn)
	clientParser := protocol.NewParser(clientConn)

	// The TTL must outlive the WATCH below, so that the transaction is aborted
	// by the eviction rather than by the SET that armed it.
	assertCommandResponse(t, clientConn, clientParser, protocol.SimpleString{Value: "OK"}, "SET", "ephemeral", "value", "PX", "300")

	watcherConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) watcher error = %v", addr, err)
	}
	defer closeTestResource(t, watcherConn)
	watcherParser := protocol.NewParser(watcherConn)
	assertCommandResponse(t, watcherConn, watcherParser, protocol.SimpleString{Value: "OK"}, "WATCH", "ephemeral")

	waitForPropagatedDelete(t, replicaConn, replicaParser, "ephemeral")

	assertCommandResponse(t, watcherConn, watcherParser, protocol.SimpleString{Value: "OK"}, "MULTI")
	assertCommandResponse(t, watcherConn, watcherParser, protocol.SimpleString{Value: "QUEUED"}, "SET", "ephemeral", "again")
	assertCommandResponse(t, watcherConn, watcherParser, protocol.Array{Null: true}, "EXEC")

	cancel()
	waitForServerStop(t, errCh)

	recorded, err := os.ReadFile(aofPath)
	if err != nil {
		t.Fatalf("ReadFile(%q) error = %v", aofPath, err)
	}
	wantFrame, err := protocol.EncodeValues([]protocol.Value{request("DEL", "ephemeral")})
	if err != nil {
		t.Fatalf("EncodeValues() error = %v", err)
	}
	if !bytes.Contains(recorded, wantFrame) {
		t.Fatalf("AOF = %q, want it to contain the eviction frame %q", recorded, wantFrame)
	}
}

// attachTestReplica completes the replica handshake on a raw connection and
// returns it positioned at the start of the propagated command stream.
func attachTestReplica(t *testing.T, srv *server.Server, addr string) (net.Conn, *protocol.Parser) {
	t.Helper()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) replica error = %v", addr, err)
	}
	t.Cleanup(func() { closeTestResource(t, conn) })

	parser := protocol.NewParser(conn)
	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "OK"}, "REPLCONF", "listening-port", "6380")

	if err := protocol.WriteValue(conn, request("PSYNC", "?", "-1")); err != nil {
		t.Fatalf("WriteValue(PSYNC) error = %v", err)
	}
	if _, err := parser.Parse(); err != nil {
		t.Fatalf("Parse() FULLRESYNC error = %v", err)
	}
	if _, err := parser.Parse(); err != nil {
		t.Fatalf("Parse() snapshot error = %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if srv.ReplicaCount() == 1 {
			return conn, parser
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("ReplicaCount() = %d, want 1", srv.ReplicaCount())
	return nil, nil
}

// waitForPropagatedDelete consumes the propagated command stream until the
// deletion of key arrives, tolerating the writes that armed it.
func waitForPropagatedDelete(t *testing.T, conn net.Conn, parser *protocol.Parser, key string) {
	t.Helper()

	if err := conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline() error = %v", err)
	}
	defer func() { _ = conn.SetReadDeadline(time.Time{}) }()

	for {
		propagated, err := decodeReplicaRequest(parser)
		if err != nil {
			t.Fatalf("waiting for propagated DEL %q: %v", key, err)
		}
		if propagated.Name != "DEL" {
			continue
		}
		if len(propagated.Args) != 1 || string(propagated.Args[0]) != key {
			t.Fatalf("propagated DEL args = %q, want [%s]", propagated.Args, key)
		}
		return
	}
}

func TestMasterFullResyncTransfersExistingKeyspace(t *testing.T) {
	masterCfg := config.Default()
	masterCfg.Host = "127.0.0.1"
	masterCfg.Port = 0
	masterCfg.LogLevel = "error"
	masterCfg.EvictionInterval = 5 * time.Millisecond
	masterCfg.EvictionSampleSize = 10
	masterCfg.DumpPath = ""

	logger := stashlogger.New(masterCfg.LogLevel)

	masterStore := storage.NewStore()
	masterExecutor := command.NewExecutor(masterStore, logger)
	master := server.New(masterCfg, logger, masterStore, masterExecutor)

	masterCtx, cancelMaster := context.WithCancel(context.Background())
	defer cancelMaster()

	masterErrCh := make(chan error, 1)
	go func() {
		masterErrCh <- master.ListenAndServe(masterCtx)
	}()

	masterAddr := waitForAddr(t, master)

	masterConn, err := net.Dial("tcp", masterAddr)
	if err != nil {
		t.Fatalf("Dial(%q) master error = %v", masterAddr, err)
	}
	defer closeTestResource(t, masterConn)
	masterParser := protocol.NewParser(masterConn)

	// Populate the master before any replica exists, so these keys can only
	// reach the replica through the full resync snapshot.
	assertCommandResponse(t, masterConn, masterParser, protocol.SimpleString{Value: "OK"}, "SET", "preexisting", "value")
	assertCommandResponse(t, masterConn, masterParser, protocol.SimpleString{Value: "OK"}, "SET", "second", "another")
	// RDB support is scoped to string keys; a collection on the master must not
	// break the handshake, it is simply absent from the snapshot.
	assertCommandResponse(t, masterConn, masterParser, protocol.Integer{Value: 1}, "HSET", "collection", "field", "value")

	replicaCfg := config.Default()
	replicaCfg.Host = "127.0.0.1"
	replicaCfg.Port = 0
	replicaCfg.LogLevel = "error"
	replicaCfg.EvictionInterval = 5 * time.Millisecond
	replicaCfg.EvictionSampleSize = 10
	replicaCfg.DumpPath = ""
	replicaCfg.ReplicaOf = masterAddr

	replicaStore := storage.NewStore()
	replicaExecutor := command.NewExecutor(replicaStore, logger)
	replica := server.New(replicaCfg, logger, replicaStore, replicaExecutor)

	replicaCtx, cancelReplica := context.WithCancel(context.Background())
	defer cancelReplica()

	replicaErrCh := make(chan error, 1)
	go func() {
		replicaErrCh <- replica.ListenAndServe(replicaCtx)
	}()

	replicaAddr := waitForAddr(t, replica)

	replicaConn, err := net.Dial("tcp", replicaAddr)
	if err != nil {
		t.Fatalf("Dial(%q) replica error = %v", replicaAddr, err)
	}
	defer closeTestResource(t, replicaConn)
	replicaParser := protocol.NewParser(replicaConn)

	assertEventuallyCommandResponse(t, replicaConn, replicaParser, protocol.BulkString{Data: []byte("value")}, 2*time.Second, "GET", "preexisting")
	assertCommandResponse(t, replicaConn, replicaParser, protocol.BulkString{Data: []byte("another")}, "GET", "second")
	assertCommandResponse(t, replicaConn, replicaParser, protocol.BulkString{Null: true}, "HGET", "collection", "field")

	cancelReplica()
	select {
	case err := <-replicaErrCh:
		if err != nil {
			t.Fatalf("replica ListenAndServe() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("replica did not stop within timeout")
	}

	cancelMaster()
	select {
	case err := <-masterErrCh:
		if err != nil {
			t.Fatalf("master ListenAndServe() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("master did not stop within timeout")
	}
}

func TestPublishPropagatesToReplicaSubscribers(t *testing.T) {
	masterCfg := config.Default()
	masterCfg.Host = "127.0.0.1"
	masterCfg.Port = 0
	masterCfg.LogLevel = "error"
	masterCfg.EvictionInterval = 5 * time.Millisecond
	masterCfg.EvictionSampleSize = 10
	masterCfg.DumpPath = ""

	logger := stashlogger.New(masterCfg.LogLevel)

	masterStore := storage.NewStore()
	masterExecutor := command.NewExecutor(masterStore, logger)
	master := server.New(masterCfg, logger, masterStore, masterExecutor)

	masterCtx, cancelMaster := context.WithCancel(context.Background())
	defer cancelMaster()

	masterErrCh := make(chan error, 1)
	go func() {
		masterErrCh <- master.ListenAndServe(masterCtx)
	}()

	masterAddr := waitForAddr(t, master)

	replicaCfg := config.Default()
	replicaCfg.Host = "127.0.0.1"
	replicaCfg.Port = 0
	replicaCfg.LogLevel = "error"
	replicaCfg.EvictionInterval = 5 * time.Millisecond
	replicaCfg.EvictionSampleSize = 10
	replicaCfg.DumpPath = ""
	replicaCfg.ReplicaOf = masterAddr

	replicaStore := storage.NewStore()
	replicaExecutor := command.NewExecutor(replicaStore, logger)
	replica := server.New(replicaCfg, logger, replicaStore, replicaExecutor)

	replicaCtx, cancelReplica := context.WithCancel(context.Background())
	defer cancelReplica()

	replicaErrCh := make(chan error, 1)
	go func() {
		replicaErrCh <- replica.ListenAndServe(replicaCtx)
	}()

	replicaAddr := waitForAddr(t, replica)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if master.ReplicaCount() == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if master.ReplicaCount() != 1 {
		t.Fatalf("ReplicaCount() = %d, want 1", master.ReplicaCount())
	}

	publisherConn, err := net.Dial("tcp", masterAddr)
	if err != nil {
		t.Fatalf("Dial(%q) publisher error = %v", masterAddr, err)
	}
	defer closeTestResource(t, publisherConn)
	publisherParser := protocol.NewParser(publisherConn)

	masterSubscriberConn, err := net.Dial("tcp", masterAddr)
	if err != nil {
		t.Fatalf("Dial(%q) master subscriber error = %v", masterAddr, err)
	}
	defer closeTestResource(t, masterSubscriberConn)
	masterSubscriberParser := protocol.NewParser(masterSubscriberConn)

	replicaSubscriberConn, err := net.Dial("tcp", replicaAddr)
	if err != nil {
		t.Fatalf("Dial(%q) replica subscriber error = %v", replicaAddr, err)
	}
	defer closeTestResource(t, replicaSubscriberConn)
	replicaSubscriberParser := protocol.NewParser(replicaSubscriberConn)

	assertCommandResponse(t, masterSubscriberConn, masterSubscriberParser, protocol.Array{Elements: []protocol.Value{
		protocol.TextBulkString{Value: "subscribe"},
		protocol.BulkString{Data: []byte("updates")},
		protocol.Integer{Value: 1},
	}}, "SUBSCRIBE", "updates")
	assertCommandResponse(t, replicaSubscriberConn, replicaSubscriberParser, protocol.Array{Elements: []protocol.Value{
		protocol.TextBulkString{Value: "subscribe"},
		protocol.BulkString{Data: []byte("updates")},
		protocol.Integer{Value: 1},
	}}, "SUBSCRIBE", "updates")

	if err := protocol.WriteValue(publisherConn, request("PUBLISH", "updates", "hello")); err != nil {
		t.Fatalf("WriteValue(PUBLISH) error = %v", err)
	}
	publishResponse, err := publisherParser.Parse()
	if err != nil {
		t.Fatalf("Parse() PUBLISH response error = %v", err)
	}
	publishedCount, ok := publishResponse.(protocol.Integer)
	if !ok {
		t.Fatalf("PUBLISH response type = %T, want protocol.Integer", publishResponse)
	}
	if publishedCount.Value < 1 {
		t.Fatalf("PUBLISH subscriber count = %d, want at least 1 local subscriber", publishedCount.Value)
	}

	masterMessage, err := masterSubscriberParser.Parse()
	if err != nil {
		t.Fatalf("Parse() master subscriber message error = %v", err)
	}
	assertValuesEqual(t, masterMessage, protocol.Array{Elements: []protocol.Value{
		protocol.TextBulkString{Value: "message"},
		protocol.BulkString{Data: []byte("updates")},
		protocol.BulkString{Data: []byte("hello")},
	}})

	if err := replicaSubscriberConn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline() replica subscriber error = %v", err)
	}
	replicaMessage, err := replicaSubscriberParser.Parse()
	if err != nil {
		t.Fatalf("Parse() replica subscriber message error = %v", err)
	}
	if err := replicaSubscriberConn.SetReadDeadline(time.Time{}); err != nil {
		t.Fatalf("reset SetReadDeadline() replica subscriber error = %v", err)
	}
	assertValuesEqual(t, replicaMessage, protocol.Array{Elements: []protocol.Value{
		protocol.TextBulkString{Value: "message"},
		protocol.BulkString{Data: []byte("updates")},
		protocol.BulkString{Data: []byte("hello")},
	}})

	cancelReplica()
	select {
	case err := <-replicaErrCh:
		if err != nil {
			t.Fatalf("replica ListenAndServe() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("replica did not stop within timeout")
	}

	cancelMaster()
	select {
	case err := <-masterErrCh:
		if err != nil {
			t.Fatalf("master ListenAndServe() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("master did not stop within timeout")
	}
}

func TestWaitReturnsReplicaAcknowledgements(t *testing.T) {
	masterCfg := config.Default()
	masterCfg.Host = "127.0.0.1"
	masterCfg.Port = 0
	masterCfg.LogLevel = "error"
	masterCfg.EvictionInterval = 5 * time.Millisecond
	masterCfg.EvictionSampleSize = 10
	masterCfg.DumpPath = ""

	logger := stashlogger.New(masterCfg.LogLevel)

	masterStore := storage.NewStore()
	masterExecutor := command.NewExecutor(masterStore, logger)
	master := server.New(masterCfg, logger, masterStore, masterExecutor)

	masterCtx, cancelMaster := context.WithCancel(context.Background())
	defer cancelMaster()

	masterErrCh := make(chan error, 1)
	go func() {
		masterErrCh <- master.ListenAndServe(masterCtx)
	}()

	masterAddr := waitForAddr(t, master)

	replicaCfg := config.Default()
	replicaCfg.Host = "127.0.0.1"
	replicaCfg.Port = 0
	replicaCfg.LogLevel = "error"
	replicaCfg.EvictionInterval = 5 * time.Millisecond
	replicaCfg.EvictionSampleSize = 10
	replicaCfg.DumpPath = ""
	replicaCfg.ReplicaOf = masterAddr

	replicaStore := storage.NewStore()
	replicaExecutor := command.NewExecutor(replicaStore, logger)
	replica := server.New(replicaCfg, logger, replicaStore, replicaExecutor)

	replicaCtx, cancelReplica := context.WithCancel(context.Background())
	defer cancelReplica()

	replicaErrCh := make(chan error, 1)
	go func() {
		replicaErrCh <- replica.ListenAndServe(replicaCtx)
	}()

	_ = waitForAddr(t, replica)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if master.ReplicaCount() == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if master.ReplicaCount() != 1 {
		t.Fatalf("ReplicaCount() = %d, want 1", master.ReplicaCount())
	}

	masterConn, err := net.Dial("tcp", masterAddr)
	if err != nil {
		t.Fatalf("Dial(%q) master error = %v", masterAddr, err)
	}
	defer closeTestResource(t, masterConn)
	masterParser := protocol.NewParser(masterConn)

	assertCommandResponse(t, masterConn, masterParser, protocol.Integer{Value: 1}, "WAIT", "1", "1000")
	assertCommandResponse(t, masterConn, masterParser, protocol.SimpleString{Value: "OK"}, "SET", "wait-key", "ready")
	assertCommandResponse(t, masterConn, masterParser, protocol.Integer{Value: 1}, "WAIT", "1", "1000")

	cancelReplica()
	select {
	case err := <-replicaErrCh:
		if err != nil {
			t.Fatalf("replica ListenAndServe() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("replica did not stop within timeout")
	}

	cancelMaster()
	select {
	case err := <-masterErrCh:
		if err != nil {
			t.Fatalf("master ListenAndServe() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("master did not stop within timeout")
	}
}

// TestWaitCountsAReplicaThatAttachedAfterEarlierWrites writes to a master before a
// replica attaches, then waits on a write made after. The replica counts its
// offset from zero when it attaches, while the master's includes the earlier
// writes, so WAIT has to count the replica from the master's offset at attach.
// It compared the two raw: with three writes before the attach, the replica's
// acknowledgements stayed 81 bytes short of every later write, more than the
// GETACK it counts into each acknowledgement makes up. A WAIT that has to wait is
// an error under --event-loop by design, so this runs in the default mode only.
func TestWaitCountsAReplicaThatAttachedAfterEarlierWrites(t *testing.T) {
	tests := []struct {
		name         string
		writesBefore int
	}{
		{name: "no writes before the replica attached", writesBefore: 0},
		{name: "three writes before the replica attached", writesBefore: 3},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			logger := stashlogger.New("error")
			masterStore := storage.NewStore()
			master := server.New(defaultTestConfig(), logger, masterStore, command.NewExecutor(masterStore, logger))
			masterCtx, cancelMaster := context.WithCancel(context.Background())
			defer cancelMaster()
			masterErrCh := make(chan error, 1)
			go func() {
				masterErrCh <- master.ListenAndServe(masterCtx)
			}()
			masterAddr := waitForAddr(t, master)

			writerConn, err := net.Dial("tcp", masterAddr)
			if err != nil {
				t.Fatalf("Dial(%q) master error = %v", masterAddr, err)
			}
			defer closeTestResource(t, writerConn)
			writerParser := protocol.NewParser(writerConn)

			for i := 0; i < tt.writesBefore; i++ {
				key, value := string(rune('a'+i)), fmt.Sprint(i+1)
				assertCommandResponse(t, writerConn, writerParser, protocol.SimpleString{Value: "OK"}, "SET", key, value)
			}

			replicaCfg := defaultTestConfig()
			replicaCfg.ReplicaOf = masterAddr
			replicaAddr, cancelReplica, replicaErrCh := startTestServer(t, replicaCfg)
			defer cancelReplica()

			// Write only once the master has registered the replica, so that the
			// write is sent to it rather than lost to the full resync.
			deadline := time.Now().Add(2 * time.Second)
			for master.ReplicaCount() != 1 && time.Now().Before(deadline) {
				time.Sleep(10 * time.Millisecond)
			}
			if got := master.ReplicaCount(); got != 1 {
				t.Fatalf("ReplicaCount() = %d, want 1", got)
			}

			assertCommandResponse(t, writerConn, writerParser, protocol.SimpleString{Value: "OK"}, "SET", "greeting", "hello")

			replicaConn, err := net.Dial("tcp", replicaAddr)
			if err != nil {
				t.Fatalf("Dial(%q) replica error = %v", replicaAddr, err)
			}
			defer closeTestResource(t, replicaConn)
			replicaParser := protocol.NewParser(replicaConn)
			assertEventuallyCommandResponse(t, replicaConn, replicaParser, protocol.BulkString{Data: []byte("hello")}, 2*time.Second, "GET", "greeting")

			assertCommandResponse(t, writerConn, writerParser, protocol.Integer{Value: 1}, "WAIT", "1", "1000")

			cancelReplica()
			waitForServerStop(t, replicaErrCh)
			cancelMaster()
			waitForServerStop(t, masterErrCh)
		})
	}
}

// TestInfoReportsAReplicasOffsetInTheMastersOffsets has a replica attach after
// earlier writes and acknowledge a later one, then reads INFO replication on the
// master. As in Redis, a slaveN line's offset is how far the replica has
// acknowledged the stream in the master's offsets, so a replica that holds
// everything sent to it shows master_repl_offset. It used to show the replica's
// own count, which leaves out the writes made before it attached.
func TestInfoReportsAReplicasOffsetInTheMastersOffsets(t *testing.T) {
	tests := []struct {
		name         string
		writesBefore int
	}{
		{name: "no writes before the replica attached", writesBefore: 0},
		{name: "three writes before the replica attached", writesBefore: 3},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			logger := stashlogger.New("error")
			masterStore := storage.NewStore()
			master := server.New(defaultTestConfig(), logger, masterStore, command.NewExecutor(masterStore, logger))
			masterCtx, cancelMaster := context.WithCancel(context.Background())
			defer cancelMaster()
			masterErrCh := make(chan error, 1)
			go func() {
				masterErrCh <- master.ListenAndServe(masterCtx)
			}()
			masterAddr := waitForAddr(t, master)

			masterConn, err := net.Dial("tcp", masterAddr)
			if err != nil {
				t.Fatalf("Dial(%q) master error = %v", masterAddr, err)
			}
			defer closeTestResource(t, masterConn)
			masterParser := protocol.NewParser(masterConn)

			for i := 0; i < tt.writesBefore; i++ {
				key, value := string(rune('a'+i)), fmt.Sprint(i+1)
				assertCommandResponse(t, masterConn, masterParser, protocol.SimpleString{Value: "OK"}, "SET", key, value)
			}

			replicaCfg := defaultTestConfig()
			replicaCfg.ReplicaOf = masterAddr
			replicaAddr, cancelReplica, replicaErrCh := startTestServer(t, replicaCfg)
			defer cancelReplica()

			deadline := time.Now().Add(2 * time.Second)
			for master.ReplicaCount() != 1 && time.Now().Before(deadline) {
				time.Sleep(10 * time.Millisecond)
			}
			if got := master.ReplicaCount(); got != 1 {
				t.Fatalf("ReplicaCount() = %d, want 1", got)
			}

			assertCommandResponse(t, masterConn, masterParser, protocol.SimpleString{Value: "OK"}, "SET", "greeting", "hello")
			replicaConn, err := net.Dial("tcp", replicaAddr)
			if err != nil {
				t.Fatalf("Dial(%q) replica error = %v", replicaAddr, err)
			}
			defer closeTestResource(t, replicaConn)
			replicaParser := protocol.NewParser(replicaConn)
			assertEventuallyCommandResponse(t, replicaConn, replicaParser, protocol.BulkString{Data: []byte("hello")}, 2*time.Second, "GET", "greeting")
			// WAIT has the replica acknowledge everything it was sent, including
			// the GETACK, after which the master sends nothing more.
			assertCommandResponse(t, masterConn, masterParser, protocol.Integer{Value: 1}, "WAIT", "1", "1000")

			fields := infoFields(t, masterConn, masterParser, "replication")
			slave, ok := fields["slave0"]
			if !ok {
				t.Fatalf("INFO replication = %v, missing slave0", fields)
			}
			_, slaveOffset, found := strings.Cut(slave, ",offset=")
			if !found {
				t.Fatalf("slave0 = %q, missing an offset", slave)
			}
			if masterOffset := fields["master_repl_offset"]; slaveOffset != masterOffset {
				t.Fatalf("slave0 offset = %s, want master_repl_offset %s (slave0:%s)", slaveOffset, masterOffset, slave)
			}

			cancelReplica()
			waitForServerStop(t, replicaErrCh)
			cancelMaster()
			waitForServerStop(t, masterErrCh)
		})
	}
}

// infoFields sends INFO section on conn and returns its name:value fields.
func infoFields(t *testing.T, conn net.Conn, parser *protocol.Parser, section string) map[string]string {
	t.Helper()

	if err := protocol.WriteValue(conn, request("INFO", section)); err != nil {
		t.Fatalf("WriteValue(INFO %s) error = %v", section, err)
	}
	reply, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse(INFO %s) error = %v", section, err)
	}
	text, _, ok := integrationBulkStringContent(reply)
	if !ok {
		t.Fatalf("INFO %s reply type = %T, want a bulk string", section, reply)
	}

	fields := make(map[string]string)
	for _, line := range strings.Split(text, "\r\n") {
		if name, value, found := strings.Cut(line, ":"); found && !strings.HasPrefix(line, "#") {
			fields[name] = value
		}
	}
	return fields
}

func TestReplicationStructuredLogs(t *testing.T) {
	var masterLogs synchronizedBuffer
	masterLogger := slog.New(slog.NewTextHandler(&masterLogs, &slog.HandlerOptions{Level: slog.LevelDebug}))

	masterCfg := config.Default()
	masterCfg.Host = "127.0.0.1"
	masterCfg.Port = 0
	masterCfg.LogLevel = "debug"
	masterCfg.EvictionInterval = 5 * time.Millisecond
	masterCfg.EvictionSampleSize = 10
	masterCfg.DumpPath = ""

	masterStore := storage.NewStore()
	masterExecutor := command.NewExecutor(masterStore, masterLogger)
	master := server.New(masterCfg, masterLogger, masterStore, masterExecutor)

	masterCtx, cancelMaster := context.WithCancel(context.Background())
	defer cancelMaster()

	masterErrCh := make(chan error, 1)
	go func() {
		masterErrCh <- master.ListenAndServe(masterCtx)
	}()

	masterAddr := waitForAddr(t, master)

	var replicaLogs synchronizedBuffer
	replicaLogger := slog.New(slog.NewTextHandler(&replicaLogs, &slog.HandlerOptions{Level: slog.LevelDebug}))

	replicaCfg := config.Default()
	replicaCfg.Host = "127.0.0.1"
	replicaCfg.Port = 0
	replicaCfg.LogLevel = "debug"
	replicaCfg.EvictionInterval = 5 * time.Millisecond
	replicaCfg.EvictionSampleSize = 10
	replicaCfg.DumpPath = ""
	replicaCfg.ReplicaOf = masterAddr

	replicaStore := storage.NewStore()
	replicaExecutor := command.NewExecutor(replicaStore, replicaLogger)
	replica := server.New(replicaCfg, replicaLogger, replicaStore, replicaExecutor)

	replicaCtx, cancelReplica := context.WithCancel(context.Background())
	defer cancelReplica()

	replicaErrCh := make(chan error, 1)
	go func() {
		replicaErrCh <- replica.ListenAndServe(replicaCtx)
	}()

	replicaAddr := waitForAddr(t, replica)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if master.ReplicaCount() == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if master.ReplicaCount() != 1 {
		t.Fatalf("ReplicaCount() = %d, want 1", master.ReplicaCount())
	}

	masterConn, err := net.Dial("tcp", masterAddr)
	if err != nil {
		t.Fatalf("Dial(%q) master error = %v", masterAddr, err)
	}
	defer closeTestResource(t, masterConn)
	masterParser := protocol.NewParser(masterConn)

	replicaConn, err := net.Dial("tcp", replicaAddr)
	if err != nil {
		t.Fatalf("Dial(%q) replica error = %v", replicaAddr, err)
	}
	defer closeTestResource(t, replicaConn)
	replicaParser := protocol.NewParser(replicaConn)

	assertCommandResponse(t, masterConn, masterParser, protocol.SimpleString{Value: "OK"}, "SET", "name", "Stash")
	assertCommandResponse(t, masterConn, masterParser, protocol.Integer{Value: 1}, "WAIT", "1", "1000")
	assertEventuallyCommandResponse(t, replicaConn, replicaParser, protocol.BulkString{Data: []byte("Stash")}, 2*time.Second, "GET", "name")

	cancelReplica()
	select {
	case err := <-replicaErrCh:
		if err != nil {
			t.Fatalf("replica ListenAndServe() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("replica did not stop within timeout")
	}

	cancelMaster()
	select {
	case err := <-masterErrCh:
		if err != nil {
			t.Fatalf("master ListenAndServe() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("master did not stop within timeout")
	}

	masterOutput := masterLogs.String()
	for _, fragment := range []string{"replica registered", "requested replica acknowledgements", "replica acknowledged offset", "WAIT completed", "replica disconnected"} {
		if !strings.Contains(masterOutput, fragment) {
			t.Fatalf("master logs missing %q\nlogs:\n%s", fragment, masterOutput)
		}
	}

	replicaOutput := replicaLogs.String()
	for _, fragment := range []string{"applied full resync snapshot", "replication stream command received"} {
		if !strings.Contains(replicaOutput, fragment) {
			t.Fatalf("replica logs missing %q\nlogs:\n%s", fragment, replicaOutput)
		}
	}
}

func TestWaitTimesOutWithoutReplicas(t *testing.T) {
	cfg := config.Default()
	cfg.Host = "127.0.0.1"
	cfg.Port = 0
	cfg.LogLevel = "error"
	cfg.EvictionInterval = 5 * time.Millisecond
	cfg.EvictionSampleSize = 10
	cfg.DumpPath = ""

	logger := stashlogger.New(cfg.LogLevel)
	store := storage.NewStore()
	executor := command.NewExecutor(store, logger)
	srv := server.New(cfg, logger, store, executor)

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
	defer closeTestResource(t, conn)
	parser := protocol.NewParser(conn)

	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "OK"}, "SET", "solo", "1")
	assertCommandResponse(t, conn, parser, protocol.Integer{Value: 0}, "WAIT", "1", "50")

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

func TestWaitUsesPerClientReplicationOffset(t *testing.T) {
	masterCfg := config.Default()
	masterCfg.Host = "127.0.0.1"
	masterCfg.Port = 0
	masterCfg.LogLevel = "error"
	masterCfg.EvictionInterval = 5 * time.Millisecond
	masterCfg.EvictionSampleSize = 10
	masterCfg.DumpPath = ""

	logger := stashlogger.New(masterCfg.LogLevel)

	masterStore := storage.NewStore()
	masterExecutor := command.NewExecutor(masterStore, logger)
	master := server.New(masterCfg, logger, masterStore, masterExecutor)

	masterCtx, cancelMaster := context.WithCancel(context.Background())
	defer cancelMaster()

	masterErrCh := make(chan error, 1)
	go func() {
		masterErrCh <- master.ListenAndServe(masterCtx)
	}()

	masterAddr := waitForAddr(t, master)

	replicaCfg := config.Default()
	replicaCfg.Host = "127.0.0.1"
	replicaCfg.Port = 0
	replicaCfg.LogLevel = "error"
	replicaCfg.EvictionInterval = 5 * time.Millisecond
	replicaCfg.EvictionSampleSize = 10
	replicaCfg.DumpPath = ""
	replicaCfg.ReplicaOf = masterAddr

	replicaStore := storage.NewStore()
	replicaExecutor := command.NewExecutor(replicaStore, logger)
	replica := server.New(replicaCfg, logger, replicaStore, replicaExecutor)

	replicaCtx, cancelReplica := context.WithCancel(context.Background())
	defer cancelReplica()

	replicaErrCh := make(chan error, 1)
	go func() {
		replicaErrCh <- replica.ListenAndServe(replicaCtx)
	}()

	_ = waitForAddr(t, replica)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if master.ReplicaCount() == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if master.ReplicaCount() != 1 {
		t.Fatalf("ReplicaCount() = %d, want 1", master.ReplicaCount())
	}

	writerConn, err := net.Dial("tcp", masterAddr)
	if err != nil {
		t.Fatalf("Dial(%q) writer error = %v", masterAddr, err)
	}
	defer closeTestResource(t, writerConn)
	writerParser := protocol.NewParser(writerConn)

	waiterConn, err := net.Dial("tcp", masterAddr)
	if err != nil {
		t.Fatalf("Dial(%q) waiter error = %v", masterAddr, err)
	}
	defer closeTestResource(t, waiterConn)
	waiterParser := protocol.NewParser(waiterConn)

	assertCommandResponse(t, writerConn, writerParser, protocol.SimpleString{Value: "OK"}, "SET", "shared", "value")
	assertCommandResponse(t, waiterConn, waiterParser, protocol.Integer{Value: 1}, "WAIT", "1", "1000")

	cancelReplica()
	select {
	case err := <-replicaErrCh:
		if err != nil {
			t.Fatalf("replica ListenAndServe() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("replica did not stop within timeout")
	}

	cancelMaster()
	select {
	case err := <-masterErrCh:
		if err != nil {
			t.Fatalf("master ListenAndServe() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("master did not stop within timeout")
	}
}

func assertEventuallyCommandResponse(t *testing.T, conn net.Conn, parser *protocol.Parser, want protocol.Value, timeout time.Duration, parts ...string) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var last protocol.Value
	var lastErr error

	for time.Now().Before(deadline) {
		if err := protocol.WriteValue(conn, request(parts...)); err != nil {
			t.Fatalf("WriteValue(%v) error = %v", parts, err)
		}

		got, err := parser.Parse()
		if err == nil && valuesEquivalent(got, want) {
			return
		}

		last = got
		lastErr = err
		time.Sleep(10 * time.Millisecond)
	}

	if lastErr != nil {
		t.Fatalf("eventual Parse(%v) last error = %v", parts, lastErr)
	}
	t.Fatalf("eventual response for %v = %#v, want %#v", parts, last, want)
}

func valuesEquivalent(got protocol.Value, want protocol.Value) bool {
	gotEncoded, gotErr := protocol.Encode(got)
	wantEncoded, wantErr := protocol.Encode(want)
	if gotErr != nil || wantErr != nil {
		return false
	}

	return bytes.Equal(gotEncoded, wantEncoded)
}

type synchronizedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *synchronizedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *synchronizedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
