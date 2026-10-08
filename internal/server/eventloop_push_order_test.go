//go:build linux || darwin

package server

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/maltemindedal/stash/internal/config"
	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/storage"
)

// idlePoller never reports readiness, so a test that uses it decides when each
// step of the event loop runs.
type idlePoller struct{}

func (idlePoller) Add(int) error                 { return nil }
func (idlePoller) Set(int, bool, bool) error     { return nil }
func (idlePoller) Remove(int) error              { return nil }
func (idlePoller) Wait([]pollEvent) (int, error) { return 0, nil }
func (idlePoller) Wake() error                   { return nil }
func (idlePoller) Close() error                  { return nil }

// pushedMessage is a pub/sub message another client published on "news".
const pushedMessage = "*3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$5\r\nhello\r\n"

// newHandDrivenEventLoop returns an event loop serving one connection over a
// socket pair, with a stub executor that answers +OK. It also returns the
// client's end of the pair. The loop is not running: the test calls its steps.
func newHandDrivenEventLoop(t *testing.T) (*eventLoop, *eventConn, int) {
	t.Helper()

	srv := New(config.Default(), slog.New(slog.NewTextHandler(io.Discard, nil)), storage.NewStore(), stubExecutor{})
	loop := &eventLoop{
		srv:     srv,
		poller:  idlePoller{},
		ctx:     context.Background(),
		conns:   make(map[int]*eventConn),
		events:  make([]pollEvent, pollerEventBatch),
		readBuf: make([]byte, eventLoopReadChunk),
	}
	serverFD, clientFD := testSocketPair(t)
	loop.registerConn(serverFD, nil)

	conn := loop.conns[serverFD]
	if conn == nil {
		t.Fatal("registerConn() did not register the connection")
	}
	return loop, conn, clientFD
}

// pushToClient delivers payload to the connection the way PUBLISH does: through
// the client's response writer, from whichever goroutine runs the command.
func pushToClient(t *testing.T, loop *eventLoop, conn *eventConn, payload string) {
	t.Helper()

	state := loop.srv.getClientState(conn.clientID)
	if err := state.WriteEncodedWithDeadline([]byte(payload), 5*time.Second); err != nil {
		t.Fatalf("WriteEncodedWithDeadline() error = %v", err)
	}
}

// readSocket returns everything the socket holds right now.
func readSocket(t *testing.T, fd int) string {
	t.Helper()

	var out []byte
	chunk := make([]byte, 4096)
	for {
		n, err := syscall.Read(fd, chunk)
		if n > 0 {
			out = append(out, chunk[:n]...)
		}
		switch {
		case err == syscall.EINTR:
		case err == syscall.EAGAIN:
			return string(out)
		case err != nil:
			t.Fatalf("Read() error = %v", err)
		case n == 0:
			return string(out)
		}
	}
}

func TestEventLoopSendsAPushQueuedBeforeARequestAheadOfItsReply(t *testing.T) {
	tests := []struct {
		name     string
		requests int
	}{
		{name: "one request", requests: 1},
		{name: "three pipelined requests", requests: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			loop, conn, clientFD := newHandDrivenEventLoop(t)

			// Another client's PUBLISH ran inline on the loop earlier in this
			// iteration, and queued the message for this subscriber.
			pushToClient(t, loop, conn, pushedMessage)
			// The subscriber's own requests run later in the same iteration.
			if err := conn.machine.Feed([]byte(strings.Repeat(pingFrame, tt.requests))); err != nil {
				t.Fatalf("Feed() error = %v", err)
			}
			loop.processConn(conn)
			loop.applyQueuedWork() // the loop's next iteration

			want := pushedMessage + strings.Repeat("+OK\r\n", tt.requests)
			if got := readSocket(t, clientFD); got != want {
				t.Fatalf("subscriber received %q, want %q", got, want)
			}
		})
	}
}

func TestEventLoopSendsAPushQueuedDuringARequestAfterItsReply(t *testing.T) {
	loop, conn, clientFD := newHandDrivenEventLoop(t)

	// A message published while this connection's request runs is not part of
	// the request's reply, so it follows the reply.
	conn.run = func(context.Context, protocol.Value) ([]protocol.Value, error) {
		pushToClient(t, loop, conn, pushedMessage)
		return []protocol.Value{protocol.SimpleString{Value: "OK"}}, nil
	}
	if err := conn.machine.Feed([]byte(pingFrame)); err != nil {
		t.Fatalf("Feed() error = %v", err)
	}
	loop.processConn(conn)
	loop.applyQueuedWork() // the loop's next iteration

	want := "+OK\r\n" + pushedMessage
	if got := readSocket(t, clientFD); got != want {
		t.Fatalf("subscriber received %q, want %q", got, want)
	}
}

func TestEventLoopSendsAPushQueuedDuringARequestAheadOfTheNextPipelinedReply(t *testing.T) {
	loop, conn, clientFD := newHandDrivenEventLoop(t)

	// The first request publishes; the second runs after the message was queued,
	// so the message comes between their replies.
	requests := 0
	conn.run = func(context.Context, protocol.Value) ([]protocol.Value, error) {
		requests++
		if requests == 1 {
			pushToClient(t, loop, conn, pushedMessage)
		}
		return []protocol.Value{protocol.SimpleString{Value: "OK"}}, nil
	}
	if err := conn.machine.Feed([]byte(strings.Repeat(pingFrame, 2))); err != nil {
		t.Fatalf("Feed() error = %v", err)
	}
	loop.processConn(conn)
	loop.applyQueuedWork() // the loop's next iteration

	want := "+OK\r\n" + pushedMessage + "+OK\r\n"
	if got := readSocket(t, clientFD); got != want {
		t.Fatalf("subscriber received %q, want %q", got, want)
	}
}
