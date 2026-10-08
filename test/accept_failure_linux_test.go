//go:build linux

package test

import (
	"context"
	"net"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/maltemindedal/stash/internal/command"
	stashlogger "github.com/maltemindedal/stash/internal/logger"
	"github.com/maltemindedal/stash/internal/server"
	"github.com/maltemindedal/stash/internal/storage"
)

// TestAReplicaWhoseAcceptLoopFailsStopsAndReturnsTheError covers a server whose
// accept loop fails with an error that is not temporary, without any context
// being cancelled. ListenAndServe has to return that error after its teardown
// (which closes the append-only file and writes the shutdown snapshot), so the
// process logs it and exits. A Master did. A Replica kept running: nothing
// stopped its link, which went on redialling its Master with the context only a
// signal cancels, and the wait for the link at the end of ListenAndServe never
// ended.
func TestAReplicaWhoseAcceptLoopFailsStopsAndReturnsTheError(t *testing.T) {
	// An address where nothing listens, so a Replica's link keeps redialling.
	deadMaster, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	deadAddr := deadMaster.Addr().String()
	closeTestResource(t, deadMaster)

	tests := []struct {
		name      string
		replicaOf string
		eventLoop bool
	}{
		{name: "master", replicaOf: ""},
		{name: "replica", replicaOf: deadAddr},
		{name: "master event loop", replicaOf: "", eventLoop: true},
		{name: "replica event loop", replicaOf: deadAddr, eventLoop: true},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			cfg := defaultTestConfig()
			cfg.ReplicaOf = tc.replicaOf
			cfg.EventLoop = tc.eventLoop
			logger := stashlogger.New(cfg.LogLevel)
			store := storage.NewStore()
			srv := server.New(cfg, logger, store, command.NewExecutor(store, logger))

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			errCh := make(chan error, 1)
			go func() { errCh <- srv.ListenAndServe(ctx) }()

			breakAcceptLoop(t, waitForAddr(t, srv))

			// The bound only turns a hang into a message: it is five times the wait
			// before a Replica's first reconnect.
			select {
			case err := <-errCh:
				if err == nil || !strings.Contains(err.Error(), "server: accept connection") {
					t.Fatalf("ListenAndServe() error = %v, want the accept error", err)
				}
			case <-time.After(5 * time.Second):
				cancel()
				select {
				case err := <-errCh:
					t.Fatalf("ListenAndServe() had not returned 5s after its accept loop failed; after cancelling its context it returned %v", err)
				case <-time.After(5 * time.Second):
					t.Fatal("ListenAndServe() had not returned 5s after its accept loop failed, and did not return after its context was cancelled either")
				}
			}
		})
	}
}

// breakAcceptLoop makes the next accept on the server's listening socket fail
// with EINVAL, a non-temporary error, without cancelling any context: it calls
// shutdown(2) on the socket. The socket is found through /proc/self/fd, as the
// one listening (SO_ACCEPTCONN) IPv4 socket on addr's port, so a socket another
// test left open is never touched.
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
		t.Fatalf("ReadDir(/proc/self/fd) error = %v", err)
	}
	for _, entry := range entries {
		fd, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		if listening, err := syscall.GetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_ACCEPTCONN); err != nil || listening != 1 {
			continue
		}
		name, err := syscall.Getsockname(fd)
		if err != nil {
			continue
		}
		if in4, ok := name.(*syscall.SockaddrInet4); ok && in4.Port == port {
			if err := syscall.Shutdown(fd, syscall.SHUT_RD); err != nil {
				t.Fatalf("Shutdown(fd %d, SHUT_RD) error = %v", fd, err)
			}
			return
		}
	}
	t.Fatalf("no listening socket on port %d", port)
}
