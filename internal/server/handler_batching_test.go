package server

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maltemindedal/stash/internal/config"
	"github.com/maltemindedal/stash/internal/storage"
)

// scriptedConn is a net.Conn whose input the test supplies chunk by chunk (Read
// blocks until the next chunk arrives, and reports EOF once the channel is
// closed) and whose output it can inspect, including how many Write calls, that
// is how many write(2) calls a real socket would have seen, produced it.
type scriptedConn struct {
	input   chan []byte
	pending []byte

	mu     sync.Mutex
	output bytes.Buffer
	writes int
}

func newScriptedConn() *scriptedConn { return &scriptedConn{input: make(chan []byte, 8)} }

func (c *scriptedConn) Read(p []byte) (int, error) {
	if len(c.pending) == 0 {
		chunk, ok := <-c.input
		if !ok {
			return 0, io.EOF
		}
		c.pending = chunk
	}
	n := copy(p, c.pending)
	c.pending = c.pending[n:]
	return n, nil
}

func (c *scriptedConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writes++
	return c.output.Write(p)
}

func (c *scriptedConn) snapshot() (string, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.output.String(), c.writes
}

func (c *scriptedConn) Close() error                       { return nil }
func (c *scriptedConn) LocalAddr() net.Addr                { return stubAddr("local") }
func (c *scriptedConn) RemoteAddr() net.Addr               { return stubAddr("remote") }
func (c *scriptedConn) SetDeadline(_ time.Time) error      { return nil }
func (c *scriptedConn) SetReadDeadline(_ time.Time) error  { return nil }
func (c *scriptedConn) SetWriteDeadline(_ time.Time) error { return nil }

// serveScripted runs handleConnection on conn and returns a channel closed when
// the handler has returned.
func serveScripted(t *testing.T, conn *scriptedConn) <-chan struct{} {
	t.Helper()

	srv := newTestServer(t, config.Default(), slog.New(slog.NewTextHandler(io.Discard, nil)), storage.NewStore(), stubExecutor{})
	clientID, _ := srv.registerClient(conn)
	srv.handlerWG.Add(1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.handleConnection(context.Background(), clientID, conn)
	}()
	return done
}

func waitDone(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handleConnection() did not return")
	}
}

const pingFrame = "*1\r\n$4\r\nPING\r\n"

func TestHandleConnectionBatchesRepliesToPipelinedRequests(t *testing.T) {
	// 50 requests arrive in one read. Each reply used to be flushed as soon as its
	// command finished, one write(2) apiece; they are now sent together when the
	// handler is about to wait for more input.
	const requests = 50
	conn := newScriptedConn()
	conn.input <- []byte(strings.Repeat(pingFrame, requests))
	close(conn.input)

	waitDone(t, serveScripted(t, conn))

	output, writes := conn.snapshot()
	if want := strings.Repeat("+OK\r\n", requests); output != want {
		t.Fatalf("output = %q, want %d replies in order", output, requests)
	}
	if writes > 3 {
		t.Fatalf("%d pipelined requests took %d writes to answer, want them batched (it was one per request)", requests, writes)
	}
}

func TestHandleConnectionAnswersBeforeWaitingForTheRestOfARequest(t *testing.T) {
	// A complete request followed by half of another: the first must be answered
	// while the handler waits for the rest, not held back until it arrives.
	conn := newScriptedConn()
	conn.input <- []byte(pingFrame + "*1\r\n$4\r\nPI")
	done := serveScripted(t, conn)

	deadline := time.Now().Add(5 * time.Second)
	for {
		if output, _ := conn.snapshot(); output == "+OK\r\n" {
			break
		}
		if time.Now().After(deadline) {
			output, _ := conn.snapshot()
			t.Fatalf("output = %q while the handler waited for the rest of the second request, want the first reply", output)
		}
		time.Sleep(time.Millisecond)
	}

	conn.input <- []byte("NG\r\n")
	close(conn.input)
	waitDone(t, done)
	if output, _ := conn.snapshot(); output != "+OK\r\n+OK\r\n" {
		t.Fatalf("final output = %q, want two replies", output)
	}
}

func TestHandleConnectionDeliversEveryReplyWhenTheClientClosesItsSendSide(t *testing.T) {
	// A client that pipelines requests and then half-closes still gets every reply.
	conn := newScriptedConn()
	conn.input <- []byte(strings.Repeat(pingFrame, 7))
	close(conn.input)

	waitDone(t, serveScripted(t, conn))

	if output, _ := conn.snapshot(); output != strings.Repeat("+OK\r\n", 7) {
		t.Fatalf("output = %q, want all 7 replies delivered before the connection closed", output)
	}
}
