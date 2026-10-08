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

func TestEventLoopServesPipelinedRequests(t *testing.T) {
	addr, stop, errCh := startTestServer(t, eventLoopTestConfig())
	defer stop()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) error = %v", addr, err)
	}
	defer closeTestResource(t, conn)
	parser := protocol.NewParser(conn)

	// Send a pipeline of frames in one burst, including a trailing frame split
	// across two writes, and expect ordered replies.
	pipeline := "*1\r\n$4\r\nPING\r\n*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n*2\r\n$3\r\nGET\r\n$3\r\nfo"
	if _, err := conn.Write([]byte(pipeline)); err != nil {
		t.Fatalf("Write(pipeline) error = %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	if _, err := conn.Write([]byte("o\r\n")); err != nil {
		t.Fatalf("Write(pipeline tail) error = %v", err)
	}

	assertParsedValue(t, parser, protocol.SimpleString{Value: "PONG"})
	assertParsedValue(t, parser, protocol.SimpleString{Value: "OK"})
	assertParsedValue(t, parser, protocol.BulkString{Data: []byte("bar")})

	stop()
	waitForServerStop(t, errCh)
}

func TestEventLoopDeliversPubSubMessages(t *testing.T) {
	addr, stop, errCh := startTestServer(t, eventLoopTestConfig())
	defer stop()

	subscriberConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) subscriber error = %v", addr, err)
	}
	defer closeTestResource(t, subscriberConn)
	subscriberParser := protocol.NewParser(subscriberConn)

	publisherConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) publisher error = %v", addr, err)
	}
	defer closeTestResource(t, publisherConn)
	publisherParser := protocol.NewParser(publisherConn)

	if err := protocol.WriteValue(subscriberConn, request("SUBSCRIBE", "updates")); err != nil {
		t.Fatalf("WriteValue(SUBSCRIBE) error = %v", err)
	}
	assertParsedValue(t, subscriberParser, protocol.Array{Elements: []protocol.Value{
		protocol.TextBulkString{Value: "subscribe"},
		protocol.BulkString{Data: []byte("updates")},
		protocol.Integer{Value: 1},
	}})

	assertCommandResponse(t, publisherConn, publisherParser, protocol.Integer{Value: 1}, "PUBLISH", "updates", "hello")
	assertParsedValue(t, subscriberParser, protocol.Array{Elements: []protocol.Value{
		protocol.TextBulkString{Value: "message"},
		protocol.BulkString{Data: []byte("updates")},
		protocol.BulkString{Data: []byte("hello")},
	}})

	if err := protocol.WriteValue(subscriberConn, request("UNSUBSCRIBE")); err != nil {
		t.Fatalf("WriteValue(UNSUBSCRIBE) error = %v", err)
	}
	assertParsedValue(t, subscriberParser, protocol.Array{Elements: []protocol.Value{
		protocol.TextBulkString{Value: "unsubscribe"},
		protocol.BulkString{Data: []byte("updates")},
		protocol.Integer{Value: 0},
	}})
	assertCommandResponse(t, publisherConn, publisherParser, protocol.Integer{Value: 0}, "PUBLISH", "updates", "bye")

	stop()
	waitForServerStop(t, errCh)
}

// TestEventLoopDeliversAMessagePublishedBeforeUnsubscribeAheadOfTheUnsubscribeReply
// races a PUBLISH from one connection against the subscriber's own UNSUBSCRIBE.
// Whichever runs first, nothing may reach the subscriber after the UNSUBSCRIBE
// reply, since the subscriber reads its next command's reply from there. The
// loop can serve both requests in one batch of readiness events, so repeating
// the race is how the test reaches that order; it never waits for a time.
func TestEventLoopDeliversAMessagePublishedBeforeUnsubscribeAheadOfTheUnsubscribeReply(t *testing.T) {
	addr, stop, errCh := startTestServer(t, eventLoopTestConfig())
	defer stop()

	subscriberConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) subscriber error = %v", addr, err)
	}
	defer closeTestResource(t, subscriberConn)
	subscriberParser := protocol.NewParser(subscriberConn)

	publisherConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) publisher error = %v", addr, err)
	}
	defer closeTestResource(t, publisherConn)
	publisherParser := protocol.NewParser(publisherConn)

	const trials = 200
	for trial := 0; trial < trials; trial++ {
		assertCommandResponse(t, subscriberConn, subscriberParser, protocol.Array{Elements: []protocol.Value{
			protocol.TextBulkString{Value: "subscribe"},
			protocol.BulkString{Data: []byte("news")},
			protocol.Integer{Value: 1},
		}}, "SUBSCRIBE", "news")

		if err := protocol.WriteValue(publisherConn, request("PUBLISH", "news", "hello")); err != nil {
			t.Fatalf("trial %d: WriteValue(PUBLISH) error = %v", trial, err)
		}
		if err := protocol.WriteValue(subscriberConn, request("UNSUBSCRIBE", "news")); err != nil {
			t.Fatalf("trial %d: WriteValue(UNSUBSCRIBE) error = %v", trial, err)
		}

		// Read up to the UNSUBSCRIBE reply. The message is there only if the
		// PUBLISH ran first, and then it comes ahead of the reply.
		for kind := ""; kind != "unsubscribe"; {
			got, err := subscriberParser.Parse()
			if err != nil {
				t.Fatalf("trial %d: Parse() error = %v", trial, err)
			}
			if kind = pubSubFrameKind(got); kind != "message" && kind != "unsubscribe" {
				t.Fatalf("trial %d: subscriber received %#v, want a message or the UNSUBSCRIBE reply", trial, got)
			}
		}

		publishReply, err := publisherParser.Parse()
		if err != nil {
			t.Fatalf("trial %d: Parse() PUBLISH reply error = %v", trial, err)
		}
		if _, ok := publishReply.(protocol.Integer); !ok {
			t.Fatalf("trial %d: PUBLISH reply = %#v, want an integer", trial, publishReply)
		}

		// A message still on its way would be read here as PING's reply.
		assertCommandResponse(t, subscriberConn, subscriberParser, protocol.SimpleString{Value: "PONG"}, "PING")
	}

	stop()
	waitForServerStop(t, errCh)
}

// pubSubFrameKind returns the first element of a pub/sub frame, such as
// "message" or "unsubscribe", or "" for any other value.
func pubSubFrameKind(value protocol.Value) string {
	array, ok := value.(protocol.Array)
	if !ok || len(array.Elements) == 0 {
		return ""
	}
	kind, _, _ := integrationBulkStringContent(array.Elements[0])
	return kind
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

func assertParsedValue(t *testing.T, parser *protocol.Parser, want protocol.Value) {
	t.Helper()

	got, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	assertValuesEqual(t, got, want)
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
