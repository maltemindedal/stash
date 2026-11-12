//go:build linux || darwin

package server

import (
	"errors"
	"net"
	"runtime"
	"sync/atomic"
	"syscall"
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
// stall rule was a write deadline, and the loop's sockets have none. When the
// stall time has passed the rule asks the loop to try the socket once more, and
// the loop closes the replica when that try takes nothing either.
func TestEventLoopClosesAReplicaThatAcceptsNothingWithinTheStallTime(t *testing.T) {
	loop, conn, _ := newHandDrivenEventLoop(t, withStallTimeout(250*time.Millisecond))
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

	waitForOutput(t, "the stall rule to give the loop work once the stall time has passed", loop.hasQueuedWork)
	loop.applyQueuedWork() // the loop's next iteration
	if err := conn.out.Err(); !errors.Is(err, errReplicaStalled) {
		t.Fatalf("Err() = %v, want errReplicaStalled", err)
	}
	if loop.conns[conn.fd] != nil {
		t.Fatal("the loop still has the connection, want it closed")
	}
}

// TestEventLoopKeepsAReplicaThatReadsEverythingWhileTheLoopIsAway offers a
// replica more of its stream than its socket takes at once, as any burst
// larger than the socket's buffer does. The replica reads everything the socket
// holds and goes on reading, and the loop does not come back to the connection
// for longer than the stall time, as it does not while it runs one long request
// inline: a long pipeline, the snapshot of another replica's full resync. The
// socket could take more the whole time and nothing offered it more. The stall
// rule's timer used to close such a replica. It cannot tell a socket that is
// full from one that nobody tried, so it leaves the verdict to the loop's next
// try, which finds room, and the count starts again.
func TestEventLoopKeepsAReplicaThatReadsEverythingWhileTheLoopIsAway(t *testing.T) {
	const stallTimeout = 50 * time.Millisecond
	loop, conn, clientFD := newHandDrivenEventLoop(t, withStallTimeout(stallTimeout))
	conn.out.MarkReplica()

	const stream = 8 << 20
	if !conn.out.Push(make([]byte, stream)) {
		t.Fatalf("Push() = false, want the stream queued: %v", conn.out.Err())
	}
	loop.applyQueuedWork() // the loop offers the batch, and the socket takes what fits
	if taken := stream - conn.out.Backlog(); taken == 0 || taken == stream {
		t.Fatalf("the socket took %d of %d bytes, want part of them", taken, stream)
	}

	received := 0
	for away := 1; away <= 3; away++ {
		// The replica reads all the while. The loop is busy elsewhere until
		// the stall time has passed, and then comes back.
		waitForOutput(t, "the stall rule to ask the loop to try the socket again", func() bool {
			received += len(readSent(t, clientFD))
			return loop.hasQueuedWork()
		})
		if err := conn.out.Err(); err != nil {
			t.Fatalf("Err() = %v the %d. time the loop stayed away for the stall time of %v, after the replica read all %d bytes its socket was given; want the replica kept until the loop has tried its socket", err, away, stallTimeout, received)
		}
		before := conn.out.Backlog()
		loop.applyQueuedWork()
		if err := conn.out.Err(); err != nil {
			t.Fatalf("Err() = %v after the loop came back to a socket with room, want the replica kept", err)
		}
		if loop.conns[conn.fd] != conn {
			t.Fatal("the loop closed the connection of a replica that reads everything it is sent")
		}
		if conn.out.Backlog() == before {
			t.Fatalf("the loop's try wrote nothing to a socket the replica had emptied, with %d bytes waiting", before)
		}
	}
}

// TestEventLoopKeepsAReplicaThatTakesItsStream is the same replica reading what
// it is sent: output waits for it at every look, and the socket keeps accepting.
func TestEventLoopKeepsAReplicaThatTakesItsStream(t *testing.T) {
	const stallTimeout = 50 * time.Millisecond
	loop, conn, clientFD := newHandDrivenEventLoop(t, withStallTimeout(stallTimeout))
	conn.out.MarkReplica()

	chunk := make([]byte, 1<<20)
	received := 0
	for start := time.Now(); time.Since(start) < 4*stallTimeout; {
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

// TestEventLoopEndsAReplicasStreamOnceWhatIsQueuedIsWritten ends a Replica's
// stream through its Output while the loop drains, as shutdown does
// (replication.Master.EndStreams). The loop writes what is queued for the
// Replica, shuts only the write side, so that the Replica reads the end of
// the stream, and keeps the connection until the Replica closes its own side.
// Closed outright, an acknowledgement the Replica sent meanwhile would draw a
// reset that discards what it had not read yet.
func TestEventLoopEndsAReplicasStreamOnceWhatIsQueuedIsWritten(t *testing.T) {
	tests := []struct {
		name   string
		queued string
	}{
		{name: "with output still queued", queued: pushOrderMessage},
		{name: "with nothing queued"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			loop, conn, clientFD := newDrainingEventLoopWithAReplica(t)

			if tt.queued != "" && !conn.out.Push([]byte(tt.queued)) {
				t.Fatalf("Push() = false, want the stream queued: %v", conn.out.Err())
			}
			conn.out.EndStream()
			loop.applyQueuedWork()

			if got := readSent(t, clientFD); got != tt.queued {
				t.Fatalf("the replica was sent %q before the end of its stream, want %q", got, tt.queued)
			}
			if n, err := syscall.Read(clientFD, make([]byte, 1)); n != 0 || err != nil {
				t.Fatalf("reading past the stream = (%d, %v), want its end: the write side was not shut", n, err)
			}
			if loop.conns[conn.fd] != conn {
				t.Fatal("the loop closed the connection before the replica closed its own side")
			}

			if err := syscall.Shutdown(clientFD, syscall.SHUT_WR); err != nil {
				t.Fatalf("Shutdown(client) error = %v", err)
			}
			loop.connReadable(conn)
			if loop.conns[conn.fd] != nil {
				t.Fatal("the loop still has the connection after the replica closed its side, want it closed")
			}
		})
	}
}

// TestEventLoopClosesADrainingReplicasConnectionWhenAskedTo asks a loop that
// drains to close a Replica's connection. Shutdown asks once its grace has
// passed, for a Replica that has not closed its own side, and the connection's
// Output asks when the Replica falls behind. The loop used to read such a
// request as one to end the Replica's stream and keep the connection, and
// ended the drain on a timer of its own. That timer raced shutdown's grace:
// when it won, shutdown found no Replica left to report.
func TestEventLoopClosesADrainingReplicasConnectionWhenAskedTo(t *testing.T) {
	tests := []struct {
		name  string
		close func(loop *eventLoop, conn *eventConn)
	}{
		{name: "by shutdown, through the client registry", close: func(loop *eventLoop, _ *eventConn) {
			if err := loop.srv.registry.CloseAll(); err != nil {
				t.Errorf("CloseAll() error = %v", err)
			}
		}},
		{name: "by its Output", close: func(_ *eventLoop, conn *eventConn) {
			conn.out.Close(errPushLimitExceeded)
		}},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			loop, conn, clientFD := newDrainingEventLoopWithAReplica(t)

			tt.close(loop, conn)
			loop.applyQueuedWork()

			if loop.conns[conn.fd] != nil {
				t.Fatal("the loop still has the replica's connection after it was asked to close it")
			}
			if n, err := syscall.Read(clientFD, make([]byte, 1)); n != 0 || err != nil {
				t.Fatalf("reading from the replica's side = (%d, %v), want the connection closed", n, err)
			}
		})
	}
}

// TestEventLoopKeepsNoConnectionItClosedOnRequest closes a replica's connection
// from outside the loop while a batch is partly written, as the stall rule and
// the push limits do: the closed Output asks for the loop's attention, and the
// loop closes the connection. Nothing may keep the connection after that, or the batch its
// Output was writing, which for a replica closed during its full resync is the
// snapshot. The loop used to empty its list of such requests and leave them in
// the array behind it, so the last connection closed this way stayed reachable
// until another one was.
func TestEventLoopKeepsNoConnectionItClosedOnRequest(t *testing.T) {
	loop, _, _ := newHandDrivenEventLoop(t)
	// The loop outlives the connection, as a server's does.
	defer runtime.KeepAlive(loop)
	collected := make(chan struct{})
	func() {
		var conn *eventConn
		for _, registered := range loop.conns {
			conn = registered
		}
		// Only the connection refers to this address, so it is collected when
		// the connection is.
		addr := &net.TCPAddr{Port: 1}
		runtime.SetFinalizer(addr, func(*net.TCPAddr) { close(collected) })
		conn.remoteAddr = addr

		conn.out.MarkReplica()
		if !conn.out.Push(make([]byte, 8<<20)) {
			t.Fatalf("Push() = false, want the stream queued: %v", conn.out.Err())
		}
		loop.applyQueuedWork() // part of the batch reaches the socket
		conn.out.Close(errPushLimitExceeded)
		loop.applyQueuedWork() // the loop takes the request to close it
		if len(loop.conns) != 0 {
			t.Fatal("the loop still has the connection after it was asked to close it")
		}
	}()

	for deadline := time.Now().Add(5 * time.Second); ; {
		runtime.GC()
		select {
		case <-collected:
			return
		case <-time.After(10 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			t.Fatal("the connection was still reachable 5s after the loop closed it on request, want nothing to keep it")
		}
	}
}

// TestAnEventLoopConnectionClosedBeforeItsOutputExistsIsClosedOnceItDoes closes
// a connection through the client registry's handle before the loop has built
// the connection's Output, as a shutdown that lands while the loop registers a
// connection does: the registry holds the handle from the moment it names the
// connection. The Close is kept, and the Output is closed when the handle is
// given it.
func TestAnEventLoopConnectionClosedBeforeItsOutputExistsIsClosedOnceItDoes(t *testing.T) {
	handle := &eventConnHandle{conn: &eventConn{}}
	if err := handle.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	out := newPolledOutput(newOutputs(), nil, discardLogger)
	handle.bind(out)
	if err := out.Err(); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("Err() = %v after the handle closed before it was bound, want the Output closed", err)
	}
}

// newDrainingEventLoopWithAReplica returns a hand-driven loop that has begun to
// drain, as it does when the server shuts down, and the Replica's connection it
// kept open.
func newDrainingEventLoopWithAReplica(t *testing.T) (*eventLoop, *eventConn, int) {
	t.Helper()

	loop, conn, clientFD := newHandDrivenEventLoop(t)
	conn.out.MarkReplica()
	loop.beginDrain(nil)
	if loop.conns[conn.fd] != conn {
		t.Fatal("the drain closed the replica's connection, want it kept to be sent what is queued for it")
	}
	return loop, conn, clientFD
}
