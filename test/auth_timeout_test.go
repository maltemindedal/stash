package test

import (
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"testing"
	"time"
)

// stillOpen reports whether the server has left the connection alone for the
// given wait: a read with a deadline must time out rather than see EOF or data.
func stillOpen(t *testing.T, conn net.Conn, wait time.Duration) bool {
	t.Helper()

	if err := conn.SetReadDeadline(time.Now().Add(wait)); err != nil {
		t.Fatalf("SetReadDeadline() error = %v", err)
	}
	_, err := conn.Read(make([]byte, 1))
	return errors.Is(err, os.ErrDeadlineExceeded)
}

func TestConnectionsThatNeverAuthenticateAreClosed(t *testing.T) {
	const timeout = 150 * time.Millisecond

	for _, eventLoop := range []bool{false, true} {
		name := "goroutine per connection"
		if eventLoop {
			name = "event loop"
		}
		t.Run(name, func(t *testing.T) {
			cfg := defaultTestConfig()
			cfg.EventLoop = eventLoop
			cfg.RequirePass = "secret"
			cfg.AuthTimeout = timeout
			addr, stop, errCh := startTestServer(t, cfg)
			defer func() {
				stop()
				waitForServerStop(t, errCh)
			}()

			dial := func(t *testing.T) net.Conn {
				t.Helper()
				conn, err := net.Dial("tcp", addr)
				if err != nil {
					t.Fatalf("Dial(%q) error = %v", addr, err)
				}
				t.Cleanup(func() { closeTestResource(t, conn) })
				return conn
			}

			t.Run("an idle connection", func(t *testing.T) {
				conn := dial(t)
				started := time.Now()
				if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
					t.Fatalf("SetReadDeadline() error = %v", err)
				}
				reply, err := io.ReadAll(conn)
				if err != nil {
					t.Fatalf("ReadAll() error = %v (got %q), want the server to close the idle connection", err, reply)
				}
				if elapsed := time.Since(started); elapsed < timeout/2 || elapsed > 3*time.Second {
					t.Fatalf("connection closed after %v, want about %v", elapsed, timeout)
				}
			})

			t.Run("a client that trickles a frame in slowly", func(t *testing.T) {
				conn := dial(t)
				if _, err := io.WriteString(conn, "*2\r\n$4\r\nAUTH\r\n$6\r\nsecr"); err != nil {
					t.Fatalf("Write() error = %v", err)
				}
				if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
					t.Fatalf("SetReadDeadline() error = %v", err)
				}
				if _, err := io.ReadAll(conn); err != nil {
					t.Fatalf("ReadAll() error = %v, want the server to close a connection that never finished authenticating", err)
				}
			})

			t.Run("a client that authenticates in time is left alone", func(t *testing.T) {
				conn := dial(t)
				if _, err := io.WriteString(conn, respCommand("AUTH", "secret")); err != nil {
					t.Fatalf("Write() error = %v", err)
				}
				buf := make([]byte, 16)
				if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
					t.Fatalf("SetReadDeadline() error = %v", err)
				}
				n, err := conn.Read(buf)
				if err != nil || string(buf[:n]) != "+OK\r\n" {
					t.Fatalf("AUTH reply = (%q, %v), want +OK", buf[:n], err)
				}

				// Well past the deadline it would have had.
				if !stillOpen(t, conn, 3*timeout) {
					t.Fatal("an authenticated connection was closed after the authentication deadline")
				}
				if _, err := io.WriteString(conn, respCommand("PING")); err != nil {
					t.Fatalf("Write(PING) error = %v", err)
				}
				if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
					t.Fatalf("SetReadDeadline() error = %v", err)
				}
				n, err = conn.Read(buf)
				if err != nil || !strings.HasPrefix(string(buf[:n]), "+PONG") {
					t.Fatalf("PING reply = (%q, %v), want +PONG", buf[:n], err)
				}
			})
		})
	}
}

func TestAuthTimeoutOnlyAppliesWhereAPasswordIsRequired(t *testing.T) {
	const timeout = 100 * time.Millisecond

	cases := []struct {
		name     string
		password string
		timeout  time.Duration
	}{
		{name: "no password", password: "", timeout: timeout},
		{name: "the timeout disabled", password: "secret", timeout: 0},
	}
	for _, tt := range cases {
		for _, eventLoop := range []bool{false, true} {
			mode := "goroutine per connection"
			if eventLoop {
				mode = "event loop"
			}
			t.Run(tt.name+", "+mode, func(t *testing.T) {
				cfg := defaultTestConfig()
				cfg.EventLoop = eventLoop
				cfg.RequirePass = tt.password
				cfg.AuthTimeout = tt.timeout
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

				if !stillOpen(t, conn, 4*timeout) {
					t.Fatalf("an idle connection was closed with password %q and timeout %v", tt.password, tt.timeout)
				}
			})
		}
	}
}
