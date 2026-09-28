//go:build linux

package test

import (
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/maltemindedal/stash/internal/protocol"
)

// keepaliveTimerArmed reports whether the kernel has a TCP keepalive timer armed
// on the server side of the connection between serverAddr and clientAddr, read
// from /proc/net/tcp (the timer column is 02 for keepalive).
func keepaliveTimerArmed(t *testing.T, serverAddr, clientAddr net.Addr) bool {
	t.Helper()

	raw, err := os.ReadFile("/proc/net/tcp")
	if err != nil {
		t.Skipf("cannot read /proc/net/tcp: %v", err)
	}
	serverPort := fmt.Sprintf(":%04X", serverAddr.(*net.TCPAddr).Port)
	clientPort := fmt.Sprintf(":%04X", clientAddr.(*net.TCPAddr).Port)

	for _, line := range strings.Split(string(raw), "\n")[1:] {
		fields := strings.Fields(line)
		if len(fields) < 6 || !strings.HasSuffix(fields[1], serverPort) || !strings.HasSuffix(fields[2], clientPort) {
			continue
		}
		timer, _, _ := strings.Cut(fields[5], ":")
		return timer == "02"
	}
	return false
}

// TestAcceptedConnectionsUseTCPKeepalive checks that a client that vanishes
// without closing its socket is eventually noticed in both networking modes. The
// net package enables keepalive on the connections the default path accepts; the
// event loop accepts raw descriptors and has to do it itself.
func TestAcceptedConnectionsUseTCPKeepalive(t *testing.T) {
	tests := []struct {
		name string
		cfg  func() (addr string, stop func(), errCh <-chan error)
	}{
		{name: "goroutine per connection", cfg: func() (string, func(), <-chan error) { return startTestServer(t, defaultTestConfig()) }},
		{name: "event loop", cfg: func() (string, func(), <-chan error) { return startTestServer(t, eventLoopTestConfig()) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addr, stop, errCh := tt.cfg()
			defer func() {
				stop()
				waitForServerStop(t, errCh)
			}()

			conn, err := net.Dial("tcp", addr)
			if err != nil {
				t.Fatalf("Dial(%q) error = %v", addr, err)
			}
			defer closeTestResource(t, conn)
			assertCommandResponse(t, conn, protocol.NewParser(conn), protocol.SimpleString{Value: "PONG"}, "PING")

			deadline := time.Now().Add(2 * time.Second)
			for !keepaliveTimerArmed(t, conn.RemoteAddr(), conn.LocalAddr()) {
				if time.Now().After(deadline) {
					t.Fatal("the server side of the connection has no TCP keepalive timer armed")
				}
				time.Sleep(20 * time.Millisecond)
			}
		})
	}
}
