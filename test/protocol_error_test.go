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

// TestALineOverTheLimitGetsTheSameReplyInBothModes sends ECHO x with its bulk
// length written as a zero-padded line, so the line is exactly as long as the
// test needs. Both networking modes accept one that fits in 64 KiB, CRLF
// included, and answer a longer one with one protocol error before they close.
// Default mode used to accept lines of up to 69,632 bytes: it counted only the
// 4 KiB fragments that ended without an LF.
func TestALineOverTheLimitGetsTheSameReplyInBothModes(t *testing.T) {
	// echo writes raw bytes, because assertCommandResponse writes canonical
	// lengths. The bulk length line is n bytes after the "$", CRLF included.
	echo := func(n int) string {
		return "*2\r\n$4\r\nECHO\r\n$" + strings.Repeat("0", n-3) + "1\r\nx\r\n"
	}
	const lineError = "-ERR protocol: parse array element 1: protocol: line exceeds 65536 byte limit\r\n"

	tests := []struct {
		name      string
		lineBytes int
		want      string
		closes    bool
	}{
		{name: "a line of exactly 65,536 bytes is accepted", lineBytes: 65536, want: "$1\r\nx\r\n"},
		{name: "a line one byte longer is refused", lineBytes: 65537, want: lineError, closes: true},
		{name: "a line of 69,632 bytes, the most default mode used to accept, is refused", lineBytes: 69632, want: lineError, closes: true},
	}

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

			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					var reply string
					if tt.closes {
						reply = readUntilClosed(t, addr, echo(tt.lineBytes))
					} else {
						reply = readReplies(t, addr, echo(tt.lineBytes), 2)
					}
					if reply != tt.want {
						t.Fatalf("reply = %q, want %q", reply, tt.want)
					}
				})
			}
		})
	}
}
