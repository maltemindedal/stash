//go:build linux || darwin

package test

import (
	"bytes"
	"errors"
	"io"
	"net"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/maltemindedal/stash/internal/config"
	"github.com/maltemindedal/stash/internal/protocol"
)

func eventLoopTestConfig() config.Config {
	cfg := defaultTestConfig()
	cfg.EventLoop = true
	return cfg
}

func TestEventLoopServesRepresentativeCommands(t *testing.T) {
	addr, stop, errCh := startTestServer(t, eventLoopTestConfig())
	defer stop()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) error = %v", addr, err)
	}
	defer closeTestResource(t, conn)
	parser := protocol.NewParser(conn)

	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "PONG"}, "PING")
	assertCommandResponse(t, conn, parser, protocol.BulkString{Data: []byte("hello")}, "ECHO", "hello")
	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "OK"}, "SET", "greeting", "world")
	assertCommandResponse(t, conn, parser, protocol.BulkString{Data: []byte("world")}, "GET", "greeting")
	assertCommandResponse(t, conn, parser, protocol.BulkString{Null: true}, "GET", "missing")
	assertCommandResponse(t, conn, parser, protocol.Integer{Value: 1}, "INCR", "counter")

	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "OK"}, "MULTI")
	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "QUEUED"}, "SET", "tx-key", "tx-value")
	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "QUEUED"}, "INCR", "counter")
	assertCommandResponse(t, conn, parser, protocol.Array{Elements: []protocol.Value{
		protocol.SimpleString{Value: "OK"},
		protocol.Integer{Value: 2},
	}}, "EXEC")
	assertCommandResponse(t, conn, parser, protocol.BulkString{Data: []byte("tx-value")}, "GET", "tx-key")

	stop()
	waitForServerStop(t, errCh)
}

func TestEventLoopClosesConnectionAfterProtocolError(t *testing.T) {
	addr, stop, errCh := startTestServer(t, eventLoopTestConfig())
	defer stop()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) error = %v", addr, err)
	}
	defer closeTestResource(t, conn)
	parser := protocol.NewParser(conn)

	if _, err := conn.Write([]byte("!!!bogus\r\n")); err != nil {
		t.Fatalf("Write(bogus frame) error = %v", err)
	}

	reply, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse() protocol error reply error = %v", err)
	}
	if _, ok := reply.(protocol.ErrorValue); !ok {
		t.Fatalf("reply = %#v, want protocol.ErrorValue", reply)
	}

	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline() error = %v", err)
	}
	if _, err := parser.Parse(); !errors.Is(err, io.EOF) {
		t.Fatalf("Parse() after protocol error = %v, want io.EOF", err)
	}

	stop()
	waitForServerStop(t, errCh)
}

func TestEventLoopServesManyIdleConnectionsWithoutPerConnectionGoroutines(t *testing.T) {
	addr, stop, errCh := startTestServer(t, eventLoopTestConfig())
	defer stop()

	const idleConns = 200

	baseline := runtime.NumGoroutine()

	conns := make([]net.Conn, 0, idleConns)
	defer func() {
		for _, conn := range conns {
			_ = conn.Close()
		}
	}()
	for i := 0; i < idleConns; i++ {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatalf("Dial(%q) idle connection %d error = %v", addr, i, err)
		}
		conns = append(conns, conn)
	}

	// Prove every connection is accepted and served, not just queued.
	first := protocol.NewParser(conns[0])
	assertCommandResponse(t, conns[0], first, protocol.SimpleString{Value: "PONG"}, "PING")
	last := protocol.NewParser(conns[idleConns-1])
	assertCommandResponse(t, conns[idleConns-1], last, protocol.SimpleString{Value: "PONG"}, "PING")

	grown := runtime.NumGoroutine() - baseline
	if grown >= idleConns/2 {
		t.Fatalf("goroutine count grew by %d for %d idle connections, want event-loop scaling far below one goroutine per connection", grown, idleConns)
	}

	stop()
	waitForServerStop(t, errCh)
}

func TestEventLoopRejectsBlockingCommandsInsteadOfStallingTheLoop(t *testing.T) {
	addr, stop, errCh := startTestServer(t, eventLoopTestConfig())
	defer stop()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) error = %v", addr, err)
	}
	defer closeTestResource(t, conn)
	parser := protocol.NewParser(conn)

	// BLPOP on an empty list would park the loop goroutine forever; it must
	// fail fast instead. A satisfiable BLPOP still succeeds.
	assertCommandResponse(t, conn, parser, protocol.ErrorValue{
		Message: "ERR BLPOP would block; blocking commands are not supported with event-loop networking",
	}, "BLPOP", "jobs")
	assertCommandResponse(t, conn, parser, protocol.Integer{Value: 1}, "RPUSH", "jobs", "task-1")
	assertCommandResponse(t, conn, parser, protocol.Array{Elements: []protocol.Value{
		protocol.BulkString{Data: []byte("jobs")},
		protocol.BulkString{Data: []byte("task-1")},
	}}, "BLPOP", "jobs")

	// WAIT that cannot be satisfied immediately would self-deadlock its
	// GETACK round-trip; it must fail fast. The immediate forms still work.
	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "OK"}, "SET", "k", "v")
	assertCommandResponse(t, conn, parser, protocol.Integer{Value: 0}, "WAIT", "0", "100")
	assertCommandResponse(t, conn, parser, protocol.Integer{Value: 0}, "WAIT", "1", "0")
	assertCommandResponse(t, conn, parser, protocol.ErrorValue{
		Message: "ERR WAIT would block; blocking commands are not supported with event-loop networking",
	}, "WAIT", "1", "100")

	// The connection stays serviceable after the rejected commands, and other
	// clients were never stalled.
	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "PONG"}, "PING")

	stop()
	waitForServerStop(t, errCh)
}

func TestEventLoopDrainsPipelinedRepliesAfterClientCloseWrite(t *testing.T) {
	addr, stop, errCh := startTestServer(t, eventLoopTestConfig())
	defer stop()

	setupConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) setup error = %v", addr, err)
	}
	defer closeTestResource(t, setupConn)
	setupParser := protocol.NewParser(setupConn)

	// A value large enough that the pipelined replies exceed typical kernel
	// send buffers, forcing the flush path through EAGAIN.
	payload := strings.Repeat("v", 1<<20)
	assertCommandResponse(t, setupConn, setupParser, protocol.SimpleString{Value: "OK"}, "SET", "big", payload)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) error = %v", addr, err)
	}
	defer closeTestResource(t, conn)

	const pipelined = 8
	var pipeline bytes.Buffer
	for i := 0; i < pipelined; i++ {
		if err := protocol.WriteValue(&pipeline, request("GET", "big")); err != nil {
			t.Fatalf("WriteValue(GET) error = %v", err)
		}
	}
	if _, err := conn.Write(pipeline.Bytes()); err != nil {
		t.Fatalf("Write(pipeline) error = %v", err)
	}

	// Half-close the write side before reading a single reply: the server must
	// still serve the parsed pipeline tail and drain every buffered response.
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		t.Fatalf("connection type = %T, want *net.TCPConn", conn)
	}
	if err := tcpConn.CloseWrite(); err != nil {
		t.Fatalf("CloseWrite() error = %v", err)
	}

	parser := protocol.NewParser(conn)
	for i := 0; i < pipelined; i++ {
		reply, err := parser.Parse()
		if err != nil {
			t.Fatalf("Parse() reply %d error = %v", i, err)
		}
		bulk, ok := reply.(protocol.BulkString)
		if !ok || len(bulk.Data) != len(payload) {
			t.Fatalf("reply %d = %T with %d bytes, want full %d-byte bulk string", i, reply, len(bulk.Data), len(payload))
		}
	}
	if _, err := parser.Parse(); !errors.Is(err, io.EOF) {
		t.Fatalf("Parse() after pipeline = %v, want io.EOF", err)
	}

	stop()
	waitForServerStop(t, errCh)
}
