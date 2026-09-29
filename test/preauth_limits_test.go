package test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"syscall"
	"testing"
	"time"
)

// respCommand encodes one command as an array of bulk strings.
func respCommand(args ...string) string {
	var frame bytes.Buffer
	fmt.Fprintf(&frame, "*%d\r\n", len(args))
	for _, arg := range args {
		fmt.Fprintf(&frame, "$%d\r\n%s\r\n", len(arg), arg)
	}
	return frame.String()
}

// readUntilClosed sends payload and returns everything the server answers up to
// the point it closes the connection (or fails the test if it does not).
func readUntilClosed(t *testing.T, addr string, payload string) string {
	t.Helper()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) error = %v", addr, err)
	}
	defer closeTestResource(t, conn)
	if _, err := io.WriteString(conn, payload); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline() error = %v", err)
	}
	reply, err := io.ReadAll(conn)
	// A server that closes while the client is still sending resets the
	// connection; the reply that came before the reset is what counts.
	if err != nil && (!errors.Is(err, syscall.ECONNRESET) || len(reply) == 0) {
		t.Fatalf("ReadAll() error = %v, want the server to close the connection (got %q so far)", err, reply)
	}
	return string(reply)
}

// readReplies sends payload and reads until it has n replies' worth of lines or
// the deadline passes, returning what arrived. Replies here are simple strings,
// errors and one bulk string, so counting CRLF-terminated lines is enough.
func readReplies(t *testing.T, addr string, payload string, wantLines int) string {
	t.Helper()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) error = %v", addr, err)
	}
	defer closeTestResource(t, conn)
	if _, err := io.WriteString(conn, payload); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	var got strings.Builder
	buf := make([]byte, 4096)
	deadline := time.Now().Add(3 * time.Second)
	for strings.Count(got.String(), "\r\n") < wantLines {
		if err := conn.SetReadDeadline(deadline); err != nil {
			t.Fatalf("SetReadDeadline() error = %v", err)
		}
		n, err := conn.Read(buf)
		got.Write(buf[:n])
		if err != nil {
			t.Fatalf("Read() error = %v after %q, want %d lines", err, got.String(), wantLines)
		}
	}
	return got.String()
}

func TestUnauthenticatedClientsAreHeldToSmallFrames(t *testing.T) {
	for _, eventLoop := range []bool{false, true} {
		name := "goroutine per connection"
		if eventLoop {
			name = "event loop"
		}
		t.Run(name, func(t *testing.T) {
			cfg := defaultTestConfig()
			cfg.EventLoop = eventLoop
			cfg.RequirePass = "secret"
			addr, stop, errCh := startTestServer(t, cfg)
			defer func() {
				stop()
				waitForServerStop(t, errCh)
			}()

			bigValue := strings.Repeat("v", 100*1024)

			t.Run("too many arguments before AUTH", func(t *testing.T) {
				reply := readUntilClosed(t, addr, respCommand("A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K"))
				if !strings.HasPrefix(reply, "-ERR protocol: array length 11 exceeds 10 element limit for a client that has not authenticated") || strings.Count(reply, "\r\n") != 1 {
					t.Fatalf("reply = %q, want one protocol error", reply)
				}
			})

			t.Run("a large value before AUTH", func(t *testing.T) {
				reply := readUntilClosed(t, addr, respCommand("SET", "key", bigValue))
				if !strings.Contains(reply, "bulk string length 102400 exceeds 16384 byte limit for a client that has not authenticated") {
					t.Fatalf("reply = %q, want the pre-auth bulk limit error", reply)
				}
			})

			t.Run("a header that declares a huge value is refused without waiting for it", func(t *testing.T) {
				// Only the header is sent. Nothing waits for 500 MB that never comes.
				reply := readUntilClosed(t, addr, "*1\r\n$500000000\r\n")
				if !strings.Contains(reply, "has not authenticated") {
					t.Fatalf("reply = %q, want the pre-auth bulk limit error", reply)
				}
			})

			t.Run("AUTH and PING still work", func(t *testing.T) {
				reply := readReplies(t, addr, respCommand("PING")+respCommand("AUTH", "wrong")+respCommand("AUTH", "secret")+respCommand("PING"), 4)
				want := "+PONG\r\n-WRONGPASS invalid username-password pair or user is disabled.\r\n+OK\r\n+PONG\r\n"
				if reply != want {
					t.Fatalf("reply = %q, want %q", reply, want)
				}
			})

			t.Run("large frames are fine once authenticated", func(t *testing.T) {
				rpush := []string{"RPUSH", "list"}
				for i := 0; i < 50; i++ {
					rpush = append(rpush, "x")
				}
				reply := readReplies(t, addr, respCommand("AUTH", "secret")+respCommand("SET", "key", bigValue)+respCommand(rpush...), 3)
				if reply != "+OK\r\n+OK\r\n:50\r\n" {
					t.Fatalf("reply = %q, want +OK +OK :50", reply)
				}
			})

			t.Run("a large frame pipelined right behind a successful AUTH is accepted", func(t *testing.T) {
				// Both arrive in one segment. The limit for the SET must be the one
				// that applies after the AUTH ahead of it has run, as in Redis.
				reply := readReplies(t, addr, respCommand("AUTH", "secret")+respCommand("SET", "piped", bigValue)+respCommand("GET", "piped"), 4)
				if want := "+OK\r\n+OK\r\n$102400\r\n" + bigValue + "\r\n"; reply != want {
					t.Fatalf("reply = %.60q..., want +OK +OK and the 100 KiB value back", reply)
				}
			})

			t.Run("a large frame pipelined behind a failed AUTH is refused", func(t *testing.T) {
				reply := readUntilClosed(t, addr, respCommand("AUTH", "wrong")+respCommand("SET", "piped2", bigValue))
				if !strings.HasPrefix(reply, "-WRONGPASS") || !strings.Contains(reply, "has not authenticated") {
					t.Fatalf("reply = %q, want WRONGPASS and then the pre-auth limit error", reply)
				}
			})
		})
	}
}

func TestServerWithoutAPasswordKeepsTheDefaultFrameLimits(t *testing.T) {
	for _, eventLoop := range []bool{false, true} {
		cfg := defaultTestConfig()
		cfg.EventLoop = eventLoop
		addr, stop, errCh := startTestServer(t, cfg)

		reply := readReplies(t, addr, respCommand("SET", "key", strings.Repeat("v", 100*1024)), 1)
		if reply != "+OK\r\n" {
			t.Fatalf("eventLoop=%v: reply = %q, want +OK", eventLoop, reply)
		}
		stop()
		waitForServerStop(t, errCh)
	}
}
