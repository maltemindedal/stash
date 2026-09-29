package test

import (
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

// TestServerClosesTheConnectionAfterAProtocolError covers both networking
// modes. Once a frame cannot be parsed the stream is out of step, so Redis, and
// the event-loop path, answer with one error and close. The default path used to
// answer every following byte of the garbage with its own error and log line and
// keep serving: a megabyte of junk cost the server well over 74 MB of log output.
func TestServerClosesTheConnectionAfterAProtocolError(t *testing.T) {
	for _, eventLoop := range []bool{false, true} {
		name := "goroutine per connection"
		if eventLoop {
			name = "event loop"
		}
		t.Run(name, func(t *testing.T) {
			cfg := defaultTestConfig()
			cfg.EventLoop = eventLoop
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

			// A valid command, then an inline command (unsupported), then more
			// bytes that would each have drawn their own error.
			request := "*1\r\n$4\r\nPING\r\n" + "GET key\r\n" + strings.Repeat("x", 64) + "*1\r\n$4\r\nPING\r\n"
			if _, err := io.WriteString(conn, request); err != nil {
				t.Fatalf("Write() error = %v", err)
			}
			if err := conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
				t.Fatalf("SetReadDeadline() error = %v", err)
			}
			reply, err := io.ReadAll(conn)
			if err != nil {
				t.Fatalf("ReadAll() error = %v, want the server to close the connection (got %q so far)", err, reply)
			}

			text := string(reply)
			if !strings.HasPrefix(text, "+PONG\r\n") {
				t.Fatalf("reply = %q, want the valid command answered first", text)
			}
			if got := strings.Count(text, "-ERR"); got != 1 {
				t.Fatalf("reply = %q, want exactly one protocol error before the close, got %d", text, got)
			}
			if strings.Count(text, "+PONG") != 1 {
				t.Fatalf("reply = %q, want nothing served after the protocol error", text)
			}
		})
	}
}
