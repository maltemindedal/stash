package test

import (
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/rdb"
)

// TestAClientThatHangsUpInsideABulkPayloadIsNotAnsweredWithAProtocolError sends
// half a bulk string and shuts down its sending side, keeping its receiving side
// open to see what the server says. A client that leaves is not a client that
// sent bad bytes: the server closes without a reply and logs no warning. The
// default mode used to read the torn payload as a parse error, since the
// parser reports it as io.ErrUnexpectedEOF, not io.EOF, and answered it with an
// error nobody was left to read.
func TestAClientThatHangsUpInsideABulkPayloadIsNotAnsweredWithAProtocolError(t *testing.T) {
	requests := []struct {
		name    string
		request string
	}{
		{name: "inside the payload of the only value", request: "$10\r\nabc"},
		{name: "inside the payload of a command argument", request: "*2\r\n$4\r\nECHO\r\n$10\r\nabc"},
		{name: "before the payload of a command argument", request: "*2\r\n$4\r\nECHO\r\n$10\r\n"},
		{name: "between a payload and its CRLF", request: "*2\r\n$4\r\nECHO\r\n$3\r\nabc"},
		{name: "inside the CRLF after a payload", request: "*2\r\n$4\r\nECHO\r\n$3\r\nabc\r"},
	}

	for _, eventLoop := range []bool{false, true} {
		name := "goroutine per connection"
		if eventLoop {
			name = "event loop"
		}
		t.Run(name, func(t *testing.T) {
			var logs synchronizedBuffer
			cfg := defaultTestConfig()
			cfg.EventLoop = eventLoop
			logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelWarn}))
			addr, stop, errCh := startTestServerWithLogger(t, cfg, logger)
			defer func() {
				stop()
				waitForServerStop(t, errCh)
			}()

			for _, tt := range requests {
				t.Run(tt.name, func(t *testing.T) {
					conn, err := net.Dial("tcp", addr)
					if err != nil {
						t.Fatalf("Dial(%q) error = %v", addr, err)
					}
					defer closeTestResource(t, conn)

					if _, err := io.WriteString(conn, tt.request); err != nil {
						t.Fatalf("Write() error = %v", err)
					}
					if err := conn.(*net.TCPConn).CloseWrite(); err != nil {
						t.Fatalf("CloseWrite() error = %v", err)
					}
					if err := conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
						t.Fatalf("SetReadDeadline() error = %v", err)
					}
					reply, err := io.ReadAll(conn)
					if err != nil {
						t.Fatalf("ReadAll() error = %v, want the server to close the connection (got %q so far)", err, reply)
					}
					if len(reply) != 0 {
						t.Fatalf("reply = %q, want none for a client that hung up", reply)
					}
				})
			}

			if output := logs.String(); output != "" {
				t.Fatalf("server logged %q, want nothing at warn level or above for clients that hung up", output)
			}
		})
	}
}

// TestAMasterThatHangsUpInsideAFrameEndsTheLinkWithoutAParseWarning has a fake
// Master finish the handshake, send half of a replicated SET and close. The
// Replica's link ends as it does for any lost Master. It used to log "replication
// stream parse failed" first, as if the Master had sent bad bytes.
func TestAMasterThatHangsUpInsideAFrameEndsTheLinkWithoutAParseWarning(t *testing.T) {
	masterListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() fake master error = %v", err)
	}
	defer closeTestResource(t, masterListener)

	masterErrCh := make(chan error, 1)
	go func() {
		conn, err := masterListener.Accept()
		if err != nil {
			masterErrCh <- err
			return
		}
		defer func() { _ = conn.Close() }()

		parser := protocol.NewParser(conn)
		steps := []struct {
			wantName string
			reply    protocol.Value
		}{
			{wantName: "PING", reply: protocol.SimpleString{Value: "PONG"}},
			{wantName: "REPLCONF", reply: protocol.SimpleString{Value: "OK"}},
			{wantName: "PSYNC", reply: protocol.SimpleString{Value: "FULLRESYNC test-replid 0"}},
		}
		for _, step := range steps {
			request, err := decodeReplicaRequest(parser)
			if err != nil {
				masterErrCh <- err
				return
			}
			if request.Name != step.wantName {
				masterErrCh <- fmt.Errorf("replica sent %q, want %q", request.Name, step.wantName)
				return
			}
			if err := protocol.WriteValue(conn, step.reply); err != nil {
				masterErrCh <- err
				return
			}
		}
		if err := protocol.WriteValue(conn, protocol.BulkString{Data: rdb.EmptySnapshot()}); err != nil {
			masterErrCh <- err
			return
		}

		// The first 8 of the 10 bytes of the key's payload, then the close.
		_, err = io.WriteString(conn, "*3\r\n$3\r\nSET\r\n$10\r\nabcdefgh")
		masterErrCh <- err
	}()

	var logs synchronizedBuffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelWarn}))
	cfg := defaultTestConfig()
	cfg.ReplicaOf = masterListener.Addr().String()
	_, stop, serverErrCh := startTestServerWithLogger(t, cfg, logger)
	defer stop()

	// The Replica says so when a link ends, whatever ended it, and only after
	// the parse failure was logged if it was going to be.
	waitForLogFragments(t, &logs, 5*time.Second, "the link to the master ended")
	if output := logs.String(); strings.Contains(output, "replication stream parse failed") {
		t.Fatalf("replica logged a parse failure for a Master that hung up:\n%s", output)
	}

	select {
	case err := <-masterErrCh:
		if err != nil {
			t.Fatalf("fake master error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("fake master did not finish")
	}

	stop()
	waitForServerStop(t, serverErrCh)
}
