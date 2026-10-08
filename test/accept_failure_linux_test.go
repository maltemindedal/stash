//go:build linux

package test

import (
	"errors"
	"net"
	"os"
	"strconv"
	"syscall"
	"testing"
	"time"
)

// acceptFailureGuard only turns a hang into a message: a server whose accept
// loop failed returns in milliseconds, so the guard is far above any real wait.
const acceptFailureGuard = 10 * time.Second

// breakAcceptLoop makes the next accept on the server's listening socket fail
// with EINVAL, without cancelling any context. It finds the socket among this
// process's descriptors by its port, because the net package keeps the
// descriptor to itself, and shuts down its read side.
func breakAcceptLoop(t *testing.T, addr string) {
	t.Helper()

	_, portText, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("SplitHostPort(%q) error = %v", addr, err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatalf("Atoi(%q) error = %v", portText, err)
	}
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Skipf("cannot read /proc/self/fd: %v", err)
	}
	for _, entry := range entries {
		fd, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		if listening, err := syscall.GetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_ACCEPTCONN); err != nil || listening != 1 {
			continue
		}
		sa, err := syscall.Getsockname(fd)
		if err != nil {
			continue
		}
		if in4, ok := sa.(*syscall.SockaddrInet4); ok && in4.Port == port {
			if err := syscall.Shutdown(fd, syscall.SHUT_RD); err != nil {
				t.Fatalf("Shutdown(%d, SHUT_RD) error = %v", fd, err)
			}
			return
		}
	}
	t.Fatalf("no listening socket on port %d", port)
}

// TestAReplicaWhoseAcceptLoopFailsStopsAndReturnsTheError checks that a server
// whose accept loop fails with something other than a closed listener returns
// that error from ListenAndServe after its teardown, as a Master does, and that
// a Replica does not keep redialling its Master meanwhile. The test never
// cancels the server's context except to clean up after a hang.
func TestAReplicaWhoseAcceptLoopFailsStopsAndReturnsTheError(t *testing.T) {
	// The Replica rows point at an address where nothing listens, so their link
	// keeps redialling.
	deadMaster, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	deadAddr := deadMaster.Addr().String()
	if err := deadMaster.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	tests := []struct {
		name      string
		replicaOf string
		eventLoop bool
	}{
		{name: "master", replicaOf: ""},
		{name: "replica", replicaOf: deadAddr},
		{name: "master with the event loop", replicaOf: "", eventLoop: true},
		{name: "replica with the event loop", replicaOf: deadAddr, eventLoop: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := defaultTestConfig()
			cfg.ReplicaOf = tt.replicaOf
			cfg.EventLoop = tt.eventLoop
			addr, cancel, errCh := startTestServer(t, cfg)
			defer cancel()

			breakAcceptLoop(t, addr)

			select {
			case err := <-errCh:
				if !errors.Is(err, syscall.EINVAL) {
					t.Fatalf("ListenAndServe() error = %v, want the accept error (EINVAL)", err)
				}
			case <-time.After(acceptFailureGuard):
				cancel()
				select {
				case err := <-errCh:
					t.Fatalf("ListenAndServe() had not returned %s after its accept loop failed; it returned %v once its context was cancelled", acceptFailureGuard, err)
				case <-time.After(acceptFailureGuard):
					t.Fatalf("ListenAndServe() had not returned %s after its accept loop failed, nor %s after its context was cancelled", acceptFailureGuard, acceptFailureGuard)
				}
			}
		})
	}
}
