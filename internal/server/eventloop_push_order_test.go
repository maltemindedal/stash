//go:build linux || darwin

package server

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"syscall"
	"testing"

	"github.com/maltemindedal/stash/internal/config"
	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/replication"
	"github.com/maltemindedal/stash/internal/server/output"
	"github.com/maltemindedal/stash/internal/storage"
)

// pushOrderMessage is the pub/sub message PUBLISH news hello delivers to a
// subscriber of news.
const pushOrderMessage = "*3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$5\r\nhello\r\n"

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
// the client end of that connection's socket. limits replace the server's
// output limits.
func newHandDrivenEventLoop(t *testing.T, limits ...output.Limit) (*eventLoop, *eventConn, int) {
	t.Helper()

	srv := newTestServer(t, config.Default(), slog.New(slog.NewTextHandler(io.Discard, nil)), storage.NewStore(), stubExecutor{})
	srv.outputs = output.NewShared(limits...)
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
// subscriber: to the Output of the connection's client state.
func pushToConn(t *testing.T, loop *eventLoop, conn *eventConn, frame string) {
	t.Helper()

	state := loop.srv.getClientState(conn.clientID)
	if !state.Output().Push([]byte(frame)) {
		t.Fatal("Push() = false, want the frame queued")
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

func TestEventLoopSendsAPushHeldByARequestAfterItsReply(t *testing.T) {
	loop, conn, clientFD := newHandDrivenEventLoop(t)

	// A request that lets Pushed messages in, as SUBSCRIBE does, holds them
	// behind its reply. One arrives from another goroutine while it runs.
	conn.run = func(context.Context, protocol.Value) ([]protocol.Value, error) {
		conn.out.HoldPushes()
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

// TestEventLoopSendsAWritePropagatedDuringAFullResyncAfterItsReply runs a
// request that attaches its connection as a Replica, as PSYNC does on the
// loop, and has a write propagated before the request has replied, as an
// expiry DEL is from the goroutine that sweeps. The attach cut told the
// connection's Output to hold what is pushed to it, so the write reaches the
// socket after the FULLRESYNC reply and the snapshot.
func TestEventLoopSendsAWritePropagatedDuringAFullResyncAfterItsReply(t *testing.T) {
	loop, conn, clientFD := newHandDrivenEventLoop(t)

	var reply []protocol.Value
	conn.run = func(context.Context, protocol.Value) ([]protocol.Value, error) {
		attachment := attachWithoutCut(loop.srv.master, replication.Candidate{ID: conn.clientID, Out: conn.out})
		loop.srv.master.Propagate([]byte(fullResyncStreamFrame), false)
		reply = attachment.Reply()
		return reply, nil
	}
	if err := conn.machine.Feed([]byte(pingFrame)); err != nil {
		t.Fatalf("Feed() error = %v", err)
	}
	loop.processConn(conn)
	loop.applyQueuedWork() // the loop's next iteration

	want := encodedReply(t, reply) + fullResyncStreamFrame
	if got := readSent(t, clientFD); got != want {
		t.Fatalf("the replica was sent %q, want the full resync reply and then the write: %q", got, want)
	}
}

func TestEventLoopWritesAPushFromAnotherGoroutineWithoutAnEvent(t *testing.T) {
	loop, conn, clientFD := newHandDrivenEventLoop(t)

	// No request and no readiness event: the push alone has to get the
	// connection written on the loop's next iteration.
	pushed := make(chan struct{})
	go func() {
		defer close(pushed)
		pushToConn(t, loop, conn, pushOrderMessage)
	}()
	<-pushed
	if !loop.hasQueuedWork() {
		t.Fatal("hasQueuedWork() = false after a push, want the loop to find the connection before it waits")
	}
	loop.applyQueuedWork()

	if got := readSent(t, clientFD); got != pushOrderMessage {
		t.Fatalf("client received %q, want %q", got, pushOrderMessage)
	}
}
