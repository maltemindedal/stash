package test

import (
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/maltemindedal/stash/internal/protocol"
)

// stallReplyReads has the client pipeline PING requests with large arguments and
// never read a reply. Each reply echoes the argument, so once the socket buffers
// fill the server's reply write to this client blocks, and it stays blocked for
// as long as the client does not read.
func stallReplyReads(conn net.Conn) {
	arg := strings.Repeat("x", 512<<10)
	frame := []byte(fmt.Sprintf("*2\r\n$4\r\nPING\r\n$%d\r\n%s\r\n", len(arg), arg))
	_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	for i := 0; i < 64; i++ {
		if _, err := conn.Write(frame); err != nil {
			return
		}
	}
}

// requestWithin sends one command and waits at most limit for its reply.
func requestWithin(t *testing.T, conn net.Conn, limit time.Duration, parts ...string) protocol.Value {
	t.Helper()

	if err := conn.SetDeadline(time.Now().Add(limit)); err != nil {
		t.Fatalf("SetDeadline() error = %v", err)
	}
	if err := protocol.WriteValue(conn, request(parts...)); err != nil {
		t.Fatalf("send %v: %v", parts, err)
	}
	value, err := protocol.NewParser(conn).Parse()
	if err != nil {
		t.Fatalf("%v got no reply within %v: %v (the server is blocked behind a client that stopped reading)", parts, limit, err)
	}
	return value
}

func TestServerPublishIsNotBlockedByStalledSubscriber(t *testing.T) {
	addr, cancel, errCh := startTestServer(t, defaultTestConfig())
	defer func() {
		cancel()
		waitForServerStop(t, errCh)
	}()

	subscriber, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) subscriber error = %v", addr, err)
	}
	defer closeTestResource(t, subscriber)
	if err := protocol.WriteValue(subscriber, request("SUBSCRIBE", "news")); err != nil {
		t.Fatalf("WriteValue(SUBSCRIBE) error = %v", err)
	}
	if _, err := protocol.NewParser(subscriber).Parse(); err != nil {
		t.Fatalf("Parse() SUBSCRIBE ack error = %v", err)
	}
	go stallReplyReads(subscriber)
	time.Sleep(500 * time.Millisecond)

	publisher, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) publisher error = %v", addr, err)
	}
	defer closeTestResource(t, publisher)

	start := time.Now()
	requestWithin(t, publisher, 3*time.Second, "PUBLISH", "news", "hello")
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("PUBLISH took %v with a stalled subscriber, want it bounded by the 100ms delivery deadline", elapsed)
	}
	if got := requestWithin(t, publisher, 3*time.Second, "PING"); got != (protocol.SimpleString{Value: "PONG"}) {
		t.Fatalf("PING after PUBLISH = %#v, want PONG", got)
	}
}
