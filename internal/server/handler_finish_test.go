package server

import (
	"context"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/maltemindedal/stash/internal/config"
	"github.com/maltemindedal/stash/internal/server/output"
	"github.com/maltemindedal/stash/internal/storage"
)

// readSignalConn reports when the handler first reads from its connection,
// which it does once it has bound the connection's Output.
type readSignalConn struct {
	net.Conn
	once    sync.Once
	reading chan struct{}
}

func (c *readSignalConn) Read(p []byte) (int, error) {
	c.once.Do(func() { close(c.reading) })
	return c.Conn.Read(p)
}

// TestAPeerThatShutsItsSendingSideAndNeverReadsIsClosedAfterTheStallTimeout
// has a client receive more pushed output than its socket takes, send one more
// request, shut its sending side and read nothing. The handler has then run the
// connection's last request, and its reply waits behind what the pump cannot
// write. It used to wait for that reply to be written for as long as the
// server ran, a goroutine and a connection for every such client. It now waits
// only while the peer takes something: one that takes nothing for the stall
// timeout is closed.
func TestAPeerThatShutsItsSendingSideAndNeverReadsIsClosedAfterTheStallTimeout(t *testing.T) {
	const stallTimeout = 200 * time.Millisecond
	srv := newTestServer(t, config.Default(), slog.New(slog.NewTextHandler(io.Discard, nil)), storage.NewStore(), stubExecutor{})
	srv.outputs = output.NewShared(output.StallTimeout(stallTimeout))

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	defer func() { _ = listener.Close() }()
	client, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer func() { _ = client.Close() }()
	accepted, err := listener.Accept()
	if err != nil {
		t.Fatalf("Accept() error = %v", err)
	}
	conn := &readSignalConn{Conn: accepted, reading: make(chan struct{})}
	// Small buffers, so that the two sockets hold far less than is pushed.
	if err := accepted.(*net.TCPConn).SetWriteBuffer(4 << 10); err != nil {
		t.Fatalf("SetWriteBuffer() error = %v", err)
	}
	if err := client.(*net.TCPConn).SetReadBuffer(4 << 10); err != nil {
		t.Fatalf("SetReadBuffer() error = %v", err)
	}

	clientID, state := srv.registerClient(conn)
	srv.handlerWG.Add(1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.handleConnection(context.Background(), clientID, conn)
	}()
	select {
	case <-conn.reading:
	case <-time.After(5 * time.Second):
		t.Fatal("the handler had not begun to read the connection after 5s")
	}

	// More than the sockets take, and less than the 1 MiB that would hold the
	// connection's requests back.
	if !state.Output().Push(make([]byte, 512<<10)) {
		t.Fatalf("Push() = false, want the message queued: %v", state.Output().Err())
	}
	if _, err := client.Write([]byte(pingFrame)); err != nil {
		t.Fatalf("Write(PING) error = %v", err)
	}
	if err := client.(*net.TCPConn).CloseWrite(); err != nil {
		t.Fatalf("CloseWrite() error = %v", err)
	}
	sent := time.Now()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("handleConnection() had not returned 10s after a peer that reads nothing sent its last request and shut its sending side, want the connection closed once it took nothing for the stall timeout of %v", stallTimeout)
	}
	if waited := time.Since(sent); waited < stallTimeout/2 {
		t.Fatalf("handleConnection() returned after %v, sooner than half the stall timeout of %v: the peer's socket took everything, so nothing made the handler wait", waited, stallTimeout)
	}
}
