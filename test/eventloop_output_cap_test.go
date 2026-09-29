//go:build linux || darwin

package test

import (
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/maltemindedal/stash/internal/protocol"
)

// stalledSubscriber subscribes to channel and then never reads.
func stalledSubscriber(t *testing.T, addr string, channel string) net.Conn {
	t.Helper()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) subscriber error = %v", addr, err)
	}
	t.Cleanup(func() { closeTestResource(t, conn) })
	if err := protocol.WriteValue(conn, request("SUBSCRIBE", channel)); err != nil {
		t.Fatalf("WriteValue(SUBSCRIBE) error = %v", err)
	}
	if _, err := protocol.NewParser(conn).Parse(); err != nil {
		t.Fatalf("Parse() SUBSCRIBE ack error = %v", err)
	}
	return conn
}

// publishMiB publishes count messages of one MiB each and waits for every reply.
func publishMiB(t *testing.T, addr string, channel string, count int) {
	t.Helper()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) publisher error = %v", addr, err)
	}
	defer closeTestResource(t, conn)
	parser := protocol.NewParser(conn)

	message := strings.Repeat("m", 1<<20)
	for i := 0; i < count; i++ {
		if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
			t.Fatalf("SetDeadline() error = %v", err)
		}
		if err := protocol.WriteValue(conn, request("PUBLISH", channel, message)); err != nil {
			t.Fatalf("PUBLISH %d error = %v", i, err)
		}
		if _, err := parser.Parse(); err != nil {
			t.Fatalf("PUBLISH %d reply error = %v", i, err)
		}
	}
}

// drain reads whatever the server sent to conn. It reports how many bytes
// arrived and whether the server had closed the connection (EOF or a reset)
// rather than left it open until the deadline.
func drain(conn net.Conn, wait time.Duration) (received int64, closed bool) {
	if err := conn.SetReadDeadline(time.Now().Add(wait)); err != nil {
		return 0, false
	}
	received, err := io.Copy(io.Discard, conn)
	return received, !errors.Is(err, os.ErrDeadlineExceeded)
}

func TestEventLoopClosesASubscriberWhoseOutputPassesThePerConnectionLimit(t *testing.T) {
	// A subscriber that never reads used to be allowed 512 MiB of queued output
	// (and it could hold that twice, in the push queue and the write buffer).
	// The limit is 32 MiB, Redis's pub/sub hard limit; the kernel's socket
	// buffers hold a few more megabytes before the server's buffer starts to fill.
	addr, stop, errCh := startTestServer(t, eventLoopTestConfig())
	defer func() {
		stop()
		waitForServerStop(t, errCh)
	}()

	subscriber := stalledSubscriber(t, addr, "news")
	publishMiB(t, addr, "news", 96)

	received, closed := drain(subscriber, 5*time.Second)
	if !closed {
		t.Fatalf("the subscriber was still connected after %d bytes were queued for it, want it closed at the limit", received)
	}
	const limit = 64 << 20
	if received > limit {
		t.Fatalf("the subscriber received %d bytes before it was closed, want well under the 32 MiB limit plus socket buffers", received)
	}
}

func TestEventLoopStillSendsLargeRepliesToAClientThatAskedForThem(t *testing.T) {
	// The push limit is for output nobody asked for. A client that requests a
	// 48 MiB value, more than the limit, must still get all of it.
	addr, stop, errCh := startTestServer(t, eventLoopTestConfig())
	defer func() {
		stop()
		waitForServerStop(t, errCh)
	}()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) error = %v", addr, err)
	}
	defer closeTestResource(t, conn)
	parser := protocol.NewParser(conn)

	value := strings.Repeat("v", 48<<20)
	if err := conn.SetDeadline(time.Now().Add(30 * time.Second)); err != nil {
		t.Fatalf("SetDeadline() error = %v", err)
	}
	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "OK"}, "SET", "big", value)
	assertCommandResponse(t, conn, parser, protocol.BulkString{Data: []byte(value)}, "GET", "big")
}
