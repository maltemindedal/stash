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

// pushOrderMessage is the pub/sub message PUBLISH news hello delivers to a
// subscriber of news.
const pushOrderMessage = "*3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$5\r\nhello\r\n"

// pushOrderPushTimeout is the delivery timeout the pushes in these tests use, as
// PUBLISH does. It never elapses: the event loop's push writer does not block.
const pushOrderPushTimeout = 100 * time.Millisecond

// handDrivenPoller reports no readiness and accepts every registration, so a
// test runs the event loop's steps itself in the order it chooses.
type handDrivenPoller struct{}

func (handDrivenPoller) Add(int) error                 { return nil }
func (handDrivenPoller) Set(int, bool, bool) error     { return nil }
func (handDrivenPoller) Remove(int) error              { return nil }
func (handDrivenPoller) Wait([]pollEvent) (int, error) { return 0, nil }
func (handDrivenPoller) Wake() error                   { return nil }
func (handDrivenPoller) Close() error                  { return nil }

// newHandDrivenEventLoop returns an event loop that only the test drives, one
// connection registered on it whose requests stubExecutor answers with +OK, and
// the client end of that connection's socket.
func newHandDrivenEventLoop(t *testing.T) (*eventLoop, *eventConn, int) {
	t.Helper()

	srv := newTestServer(t, config.Default(), slog.New(slog.NewTextHandler(io.Discard, nil)), storage.NewStore(), stubExecutor{})
	loop := &eventLoop{
		srv:     srv,
		poller:  handDrivenPoller{},
		ctx:     context.Background(),
		conns:   make(map[int]*eventConn),
		events:  make([]pollEvent, pollerEventBatch),
		readBuf: make([]byte, eventLoopReadChunk),
	}
	serverFD, clientFD := testSocketPair(t)
	loop.registerConn(serverFD, nil)
	conn := loop.conns[serverFD]
	if conn == nil {
		t.Fatal("registerConn() left no connection registered")
	}
	return loop, conn, clientFD
}

// pushToConn delivers frame to conn the way PUBLISH delivers a message to a
// subscriber: through the connection's client state, from outside the
// connection's own request.
func pushToConn(t *testing.T, loop *eventLoop, conn *eventConn, frame string) {
	t.Helper()

	state := loop.srv.getClientState(conn.clientID)
	if err := state.WriteEncodedWithDeadline([]byte(frame), pushOrderPushTimeout); err != nil {
		t.Fatalf("WriteEncodedWithDeadline() error = %v", err)
	}
}

// readSent returns every byte the server has written to the client end of the
// socket pair so far.
func readSent(t *testing.T, clientFD int) string {
	t.Helper()

	var sent []byte
	buf := make([]byte, 4096)
	for {
		n, err := syscall.Read(clientFD, buf)
		if n > 0 {
			sent = append(sent, buf[:n]...)
		}
		if err == syscall.EINTR {
			continue
		}
		if err == syscall.EAGAIN {
			return string(sent)
		}
		if err != nil {
			t.Fatalf("Read(client) error = %v", err)
		}
		if n == 0 {
			return string(sent)
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
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			loop, conn, clientFD := newHandDrivenEventLoop(t)

			// Another client's PUBLISH runs inline on the loop and delivers to
			// this connection; the connection's own requests run later in the
			// same iteration.
			pushToConn(t, loop, conn, pushOrderMessage)
			if err := conn.machine.Feed([]byte(strings.Repeat(pingFrame, tt.requests))); err != nil {
				t.Fatalf("Feed() error = %v", err)
			}
			loop.processConn(conn)
			loop.applyQueuedWork() // the loop's next iteration

			want := pushOrderMessage + strings.Repeat("+OK\r\n", tt.requests)
			if got := readSent(t, clientFD); got != want {
				t.Fatalf("client received %q, want %q", got, want)
			}
		})
	}
}

func TestEventLoopSendsAPushQueuedDuringARequestAfterItsReply(t *testing.T) {
	loop, conn, clientFD := newHandDrivenEventLoop(t)

	// A push from another goroutine, such as a replica applying its master's
	// PUBLISH, arrives while the connection's request runs.
	conn.run = func(context.Context, protocol.Value) ([]protocol.Value, error) {
		pushToConn(t, loop, conn, pushOrderMessage)
		return []protocol.Value{protocol.SimpleString{Value: "OK"}}, nil
	}
	if err := conn.machine.Feed([]byte(pingFrame)); err != nil {
		t.Fatalf("Feed() error = %v", err)
	}
	loop.processConn(conn)
	loop.applyQueuedWork() // the loop's next iteration

	want := "+OK\r\n" + pushOrderMessage
	if got := readSent(t, clientFD); got != want {
		t.Fatalf("client received %q, want %q", got, want)
	}
}
