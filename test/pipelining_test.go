package test

import (
	"bytes"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/maltemindedal/stash/internal/protocol"
)

// encodeCommands renders commands as one buffer of RESP requests, the way a
// pipelining client sends them in a single write.
func encodeCommands(t *testing.T, commands ...[]string) []byte {
	t.Helper()

	var buf bytes.Buffer
	for _, parts := range commands {
		if err := protocol.WriteValue(&buf, request(parts...)); err != nil {
			t.Fatalf("WriteValue(%v) error = %v", parts, err)
		}
	}
	return buf.Bytes()
}

// readReplyWithin reads one reply, failing if none arrives within limit.
func readReplyWithin(t *testing.T, conn net.Conn, parser *protocol.Parser, limit time.Duration, what string) protocol.Value {
	t.Helper()

	if err := conn.SetReadDeadline(time.Now().Add(limit)); err != nil {
		t.Fatalf("SetReadDeadline() error = %v", err)
	}
	value, err := parser.Parse()
	if err != nil {
		t.Fatalf("no %s within %v: %v", what, limit, err)
	}
	return value
}

func TestPipelinedRepliesArriveBeforeABlockingCommandBlocks(t *testing.T) {
	// Replies are sent when the server is about to wait, so a command that blocks
	// the connection has to send the replies queued ahead of it first.
	t.Run("BLPOP", func(t *testing.T) {
		addr, cancel, errCh := startTestServer(t, defaultTestConfig())
		defer func() {
			cancel()
			waitForServerStop(t, errCh)
		}()

		conn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatalf("Dial(%q) error = %v", addr, err)
		}
		defer closeTestResource(t, conn)
		parser := protocol.NewParser(conn)

		// PING, SET, then a BLPOP on an empty list that blocks the connection.
		if _, err := conn.Write(encodeCommands(t, []string{"PING"}, []string{"SET", "k", "v"}, []string{"BLPOP", "queue"})); err != nil {
			t.Fatalf("Write() error = %v", err)
		}
		assertValuesEqual(t, readReplyWithin(t, conn, parser, 2*time.Second, "reply to PING while BLPOP blocks"), protocol.SimpleString{Value: "PONG"})
		assertValuesEqual(t, readReplyWithin(t, conn, parser, 2*time.Second, "reply to SET while BLPOP blocks"), protocol.SimpleString{Value: "OK"})

		other, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatalf("Dial(%q) error = %v", addr, err)
		}
		defer closeTestResource(t, other)
		assertCommandResponse(t, other, protocol.NewParser(other), protocol.Integer{Value: 1}, "RPUSH", "queue", "item")

		assertValuesEqual(t, readReplyWithin(t, conn, parser, 2*time.Second, "reply to BLPOP once an item is pushed"), protocol.Array{Elements: []protocol.Value{
			protocol.BulkString{Data: []byte("queue")},
			protocol.BulkString{Data: []byte("item")},
		}})
	})

	t.Run("WAIT", func(t *testing.T) {
		addr, cancel, errCh := startTestServer(t, defaultTestConfig())
		defer func() {
			cancel()
			waitForServerStop(t, errCh)
		}()

		conn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatalf("Dial(%q) error = %v", addr, err)
		}
		defer closeTestResource(t, conn)
		parser := protocol.NewParser(conn)

		// With no replica, WAIT 1 waits out its whole second.
		start := time.Now()
		if _, err := conn.Write(encodeCommands(t, []string{"PING"}, []string{"WAIT", "1", "1000"})); err != nil {
			t.Fatalf("Write() error = %v", err)
		}
		assertValuesEqual(t, readReplyWithin(t, conn, parser, 700*time.Millisecond, "reply to PING while WAIT blocks"), protocol.SimpleString{Value: "PONG"})
		if elapsed := time.Since(start); elapsed > 700*time.Millisecond {
			t.Fatalf("the PING reply took %v, it must not wait for WAIT's 1s timeout", elapsed)
		}
		assertValuesEqual(t, readReplyWithin(t, conn, parser, 3*time.Second, "reply to WAIT"), protocol.Integer{Value: 0})
	})
}

func TestPipelinedRepliesAreAllDeliveredWhenTheClientClosesItsSendSide(t *testing.T) {
	addr, cancel, errCh := startTestServer(t, defaultTestConfig())
	defer func() {
		cancel()
		waitForServerStop(t, errCh)
	}()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) error = %v", addr, err)
	}
	defer closeTestResource(t, conn)

	const requests = 200
	commands := make([][]string, requests)
	for i := range commands {
		commands[i] = []string{"PING"}
	}
	if _, err := conn.Write(encodeCommands(t, commands...)); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if err := conn.(*net.TCPConn).CloseWrite(); err != nil {
		t.Fatalf("CloseWrite() error = %v", err)
	}

	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline() error = %v", err)
	}
	var got strings.Builder
	buf := make([]byte, 4096)
	for {
		n, err := conn.Read(buf)
		got.Write(buf[:n])
		if err != nil {
			break
		}
	}
	if want := strings.Repeat("+PONG\r\n", requests); got.String() != want {
		t.Fatalf("received %d bytes, want %d replies (%d bytes) after the client half-closed", got.Len(), requests, len(want))
	}
}
