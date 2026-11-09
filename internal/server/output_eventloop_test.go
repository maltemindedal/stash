//go:build linux || darwin

package server

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// wakeCountingPoller is a hand-driven poller that counts how often it is woken.
type wakeCountingPoller struct {
	handDrivenPoller
	wakes atomic.Int32
}

func (p *wakeCountingPoller) Wake() error {
	p.wakes.Add(1)
	return nil
}

// TestEventLoopIsWokenOnlyWhileItWaits pushes to a connection from outside the
// loop. A loop that is running finds the connection on its list before it waits
// again, so nothing wakes it; a loop that has announced its wait is woken, once
// for however many pushes the connection gets before the loop writes it. A
// wakeup lost between the two would leave the message unwritten until the next
// event on any socket.
func TestEventLoopIsWokenOnlyWhileItWaits(t *testing.T) {
	loop, conn, clientFD := newHandDrivenEventLoop(t)
	poller := &wakeCountingPoller{}
	loop.poller = poller

	pushToConn(t, loop, conn, pushOrderMessage)
	if got := poller.wakes.Load(); got != 0 {
		t.Fatalf("the poller was woken %d times for a push to a running loop, want 0", got)
	}
	if !loop.hasQueuedWork() {
		t.Fatal("hasQueuedWork() = false after the push, want the loop to see it before it waits")
	}
	loop.applyQueuedWork()
	if got := readSent(t, clientFD); got != pushOrderMessage {
		t.Fatalf("client received %q, want %q", got, pushOrderMessage)
	}
	if loop.hasQueuedWork() {
		t.Fatal("hasQueuedWork() = true after the loop wrote the connection")
	}

	loop.waiting.Store(true)
	pushToConn(t, loop, conn, pushOrderMessage)
	pushToConn(t, loop, conn, pushOrderMessage)
	if got := poller.wakes.Load(); got != 1 {
		t.Fatalf("the poller was woken %d times for two pushes to a waiting loop, want once", got)
	}
	loop.waiting.Store(false)
	loop.applyQueuedWork()
	if got, want := readSent(t, clientFD), pushOrderMessage+pushOrderMessage; got != want {
		t.Fatalf("client received %q, want %q", got, want)
	}
}

// TestEventLoopClosesAReplicaThatAcceptsNothingWithinTheStallTime queues more
// of the stream for a replica than its socket takes and never reads it. The
// loop used to keep such a replica until 256 MiB was waiting for it: the 30 s
// stall rule was a write deadline, and the loop's sockets have none.
func TestEventLoopClosesAReplicaThatAcceptsNothingWithinTheStallTime(t *testing.T) {
	loop, conn, _ := newHandDrivenEventLoop(t)
	loop.srv.outputs.stallTimeout = 250 * time.Millisecond
	conn.out.MarkReplica()

	if !conn.out.Push(make([]byte, 8<<20)) {
		t.Fatalf("Push() = false, want the stream queued: %v", conn.out.Err())
	}
	loop.applyQueuedWork()
	if conn.out.Backlog() == 0 {
		t.Fatal("the socket took all 8 MiB, want output left waiting for a peer that reads nothing")
	}
	if loop.conns[conn.fd] != conn {
		t.Fatal("the connection was closed before the stall time passed")
	}

	waitForOutput(t, "the stalled replica's output to be closed", func() bool { return conn.out.Err() != nil })
	if err := conn.out.Err(); !errors.Is(err, errReplicaStalled) {
		t.Fatalf("Err() = %v, want errReplicaStalled", err)
	}
	waitForOutput(t, "the loop to be asked to close the connection", loop.hasQueuedWork)
	loop.applyQueuedWork() // the loop's next iteration takes the close request
	if loop.conns[conn.fd] != nil {
		t.Fatal("the loop still has the connection, want it closed")
	}
}

// TestEventLoopKeepsAReplicaThatTakesItsStream is the same replica reading what
// it is sent: output waits for it at every look, and the socket keeps accepting.
func TestEventLoopKeepsAReplicaThatTakesItsStream(t *testing.T) {
	loop, conn, clientFD := newHandDrivenEventLoop(t)
	loop.srv.outputs.stallTimeout = 50 * time.Millisecond
	conn.out.MarkReplica()

	chunk := make([]byte, 1<<20)
	received := 0
	for start := time.Now(); time.Since(start) < 4*loop.srv.outputs.stallTimeout; {
		if conn.out.Backlog() < len(chunk) && !conn.out.Push(chunk) {
			t.Fatalf("Push() = false, want a replica that is reading kept: %v", conn.out.Err())
		}
		loop.applyQueuedWork()
		loop.finishConnEvent(conn)
		received += len(readSent(t, clientFD))
		time.Sleep(time.Millisecond)
	}
	if err := conn.out.Err(); err != nil {
		t.Fatalf("Err() = %v after the replica read %d bytes, want it kept", err, received)
	}
	if received == 0 {
		t.Fatal("the replica received nothing")
	}
}
