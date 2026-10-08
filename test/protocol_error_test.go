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

// TestABulkLengthOverTheLimitGetsTheSameReplyInBothModes sends only the header of
// a SET whose value declares more than the 512 MiB bulk string limit. Both
// networking modes must answer as soon as the header arrives, with the same
// error, and close. The Event loop used to answer a length just over the limit
// with its read-buffer text, and one near MaxInt not at all.
func TestABulkLengthOverTheLimitGetsTheSameReplyInBothModes(t *testing.T) {
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

			for _, length := range []string{"536870913", "9223372036854775807"} {
				length := length
				t.Run("length "+length, func(t *testing.T) {
					// Only the header is sent. Nothing waits for a value that never comes.
					reply := readUntilClosed(t, addr, "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$"+length+"\r\n")
					want := "-ERR protocol: parse array element 2: protocol: bulk string length " + length + " exceeds 536870912 byte limit\r\n"
					if reply != want {
						t.Fatalf("reply = %q, want %q", reply, want)
					}
				})
			}
		})
	}
}
