package server

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maltemindedal/stash/internal/protocol"
)

// outputWire is the socket of a connection served by its own goroutine, in
// memory. While stalled, a write to it blocks, as a write to a peer that
// stopped reading does, until the wire is closed.
type outputWire struct {
	mu         sync.Mutex
	received   bytes.Buffer
	writes     int
	largest    int
	writeShuts int
	stalled    bool
	writeBegun chan struct{}
	beginOnce  sync.Once
	closed     chan struct{}
	closeOnce  sync.Once
	writeErr   error
}

func newOutputWire() *outputWire {
	return &outputWire{writeBegun: make(chan struct{}), closed: make(chan struct{})}
}

func newStalledOutputWire() *outputWire {
	wire := newOutputWire()
	wire.stalled = true
	return wire
}

func (w *outputWire) Write(p []byte) (int, error) {
	w.beginOnce.Do(func() { close(w.writeBegun) })
	w.mu.Lock()
	stalled, err := w.stalled, w.writeErr
	w.mu.Unlock()
	if err != nil {
		return 0, err
	}
	if stalled {
		<-w.closed
		return 0, net.ErrClosed
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	w.writes++
	w.largest = max(w.largest, len(p))
	return w.received.Write(p)
}

func (w *outputWire) Close() error {
	w.closeOnce.Do(func() { close(w.closed) })
	return nil
}

func (w *outputWire) CloseWrite() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.writeShuts++
	return nil
}

func (w *outputWire) sent() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.received.String()
}

func (w *outputWire) writeCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.writes
}

// largestWrite is the most bytes one write handed to the socket.
func (w *outputWire) largestWrite() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.largest
}

func (w *outputWire) writeShutCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.writeShuts
}

func (w *outputWire) isClosed() bool {
	select {
	case <-w.closed:
		return true
	default:
		return false
	}
}

// newGoroutineOutput returns an Output written by the goroutine-mode driver to
// wire, under the limits in shared.
func newGoroutineOutput(shared *outputs, wire *outputWire) *Output {
	return newOutput(shared, &goroutineOutputDriver{wire: wire}, discardLogger)
}

// waitForOutput polls until done reports true, and fails the test with what it
// was waiting for after five seconds.
func waitForOutput(t *testing.T, what string, done func() bool) {
	t.Helper()

	for deadline := time.Now().Add(5 * time.Second); !done(); {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// within fails the test when call has not returned after five seconds: the
// operations of an Output that must never wait on a socket.
func within(t *testing.T, what string, call func()) {
	t.Helper()

	done := make(chan struct{})
	go func() {
		defer close(done)
		call()
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s was still waiting after 5s", what)
	}
}

func TestPipelinedRepliesLeaveInOneWrite(t *testing.T) {
	wire := newOutputWire()
	out := newGoroutineOutput(newOutputs(), wire)

	for i := 0; i < 50; i++ {
		if err := out.Reply(protocol.SimpleString{Value: "OK"}); err != nil {
			t.Fatalf("Reply() error = %v", err)
		}
	}
	if got := wire.writeCount(); got != 0 {
		t.Fatalf("the socket was written %d times before the flush, want the replies held back", got)
	}
	if err := out.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	if got, want := wire.sent(), strings.Repeat("+OK\r\n", 50); got != want {
		t.Fatalf("sent %q, want the 50 replies in order", got)
	}
	if got := wire.writeCount(); got != 1 {
		t.Fatalf("the socket was written %d times, want the 50 replies in one write", got)
	}
	if got := out.Backlog(); got != 0 {
		t.Fatalf("Backlog() = %d after the flush, want 0", got)
	}
	// Nothing queued is not an error.
	if err := out.Flush(); err != nil {
		t.Fatalf("Flush() with nothing queued error = %v", err)
	}
}

func TestAPushNeverWaitsForAStalledConnection(t *testing.T) {
	wire := newStalledOutputWire()
	out := newGoroutineOutput(newOutputs(), wire)
	defer out.Close(nil)

	within(t, "a push to a connection that stopped reading", func() {
		for i := 0; i < 1000; i++ {
			if !out.Push([]byte("+message\r\n")) {
				t.Errorf("Push() %d = false, want it queued", i)
				return
			}
		}
	})
	<-wire.writeBegun
	if got, want := out.Backlog(), 1000*len("+message\r\n"); got != want {
		t.Fatalf("Backlog() = %d, want the %d bytes pushed still waiting", got, want)
	}

	// The connection's own goroutine does not wait behind the stuck write either.
	if err := out.Reply(protocol.SimpleString{Value: "PONG"}); err != nil {
		t.Fatalf("Reply() error = %v", err)
	}
	within(t, "a flush behind a stuck writer", func() {
		if err := out.Flush(); err != nil {
			t.Errorf("Flush() behind a stuck writer error = %v, want it left to that writer", err)
		}
	})
}

func TestPushesHeldByARequestFollowItsReply(t *testing.T) {
	tests := []struct {
		name string
		hold bool
		want string
	}{
		{name: "a push queued before the reply leaves before it", hold: false, want: "+message\r\n+OK\r\n"},
		{name: "a push held by the request follows its reply", hold: true, want: "+OK\r\n+message\r\n"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			wire := newOutputWire()
			out := newGoroutineOutput(newOutputs(), wire)

			// Held first, so that nothing is written before the reply is queued.
			out.HoldPushes()
			if !tt.hold {
				if err := out.Reply(); err != nil {
					t.Fatalf("Reply() error = %v", err)
				}
			}
			if !out.Push([]byte("+message\r\n")) {
				t.Fatal("Push() = false, want it queued")
			}
			if tt.hold && wire.sent() != "" {
				t.Fatalf("sent %q while the request had not replied, want the push held", wire.sent())
			}
			if err := out.Reply(protocol.SimpleString{Value: "OK"}); err != nil {
				t.Fatalf("Reply() error = %v", err)
			}
			if err := out.Flush(); err != nil {
				t.Fatalf("Flush() error = %v", err)
			}

			waitForOutput(t, "the reply and the push to be written", func() bool { return len(wire.sent()) == len(tt.want) })
			if got := wire.sent(); got != tt.want {
				t.Fatalf("sent %q, want %q", got, tt.want)
			}
		})
	}
}

func TestAPushPastThePushLimitClosesTheConnection(t *testing.T) {
	shared := newOutputs()
	shared.pushLimit = 64
	wire := newStalledOutputWire()
	out := newGoroutineOutput(shared, wire)

	// A reply can be as large as a stored value: only pushes are held to the limit.
	if err := out.Reply(protocol.BulkString{Data: bytes.Repeat([]byte("x"), 1024)}); err != nil {
		t.Fatalf("Reply() of 1 KiB error = %v, want a reply above the push limit accepted", err)
	}
	if out.Err() != nil {
		t.Fatalf("Err() = %v after a large reply, want the connection open", out.Err())
	}

	if out.Push([]byte("+message\r\n")) {
		t.Fatal("Push() = true with more than the push limit waiting, want the connection closed")
	}
	if err := out.Err(); !errors.Is(err, errPushLimitExceeded) {
		t.Fatalf("Err() = %v, want errPushLimitExceeded", err)
	}
	if !wire.isClosed() {
		t.Fatal("the socket is open, want it closed with the Output")
	}
	if got := out.Backlog(); got != 0 {
		t.Fatalf("Backlog() = %d after the close, want what was waiting dropped", got)
	}
	if err := out.Reply(protocol.SimpleString{Value: "OK"}); !errors.Is(err, errPushLimitExceeded) {
		t.Fatalf("Reply() after the close error = %v, want why it was closed", err)
	}
}

func TestThePushBudgetClosesTheConnectionWithTheMostWaiting(t *testing.T) {
	shared := newOutputs()
	shared.pushLimit = 100
	shared.replicaPushLimit = 1000
	shared.pushBudget = 150

	open := func() (*Output, *outputWire) {
		wire := newStalledOutputWire()
		return newGoroutineOutput(shared, wire), wire
	}
	small, _ := open()
	large, largeWire := open()
	last, _ := open()
	replica, _ := open()
	replica.MarkReplica()
	replier, _ := open()
	defer func() {
		for _, out := range []*Output{small, large, last, replica, replier} {
			out.Close(nil)
		}
	}()

	// A replica's stream and a client's own replies are outside the budget.
	if !replica.Push(make([]byte, 500)) {
		t.Fatal("Push() to the replica = false, want its stream outside the budget")
	}
	if err := replier.Reply(protocol.BulkString{Data: make([]byte, 500)}); err != nil {
		t.Fatalf("Reply() error = %v", err)
	}
	if !small.Push(make([]byte, 60)) || !large.Push(make([]byte, 80)) {
		t.Fatal("Push() = false within the budget, want both queued")
	}
	if got := shared.total.Load(); got != 140 {
		t.Fatalf("the budget counts %d bytes, want the 140 pushed to subscribers", got)
	}

	if !last.Push(make([]byte, 40)) {
		t.Fatal("Push() = false for the connection with the least waiting, want another one closed")
	}
	if err := large.Err(); !errors.Is(err, errPushBudgetExceeded) {
		t.Fatalf("the connection with the most waiting: Err() = %v, want errPushBudgetExceeded", err)
	}
	if !largeWire.isClosed() {
		t.Fatal("the socket of the connection with the most waiting is open, want it closed")
	}
	for name, out := range map[string]*Output{"small": small, "last": last, "replica": replica, "replier": replier} {
		if err := out.Err(); err != nil {
			t.Fatalf("%s connection: Err() = %v, want it open", name, err)
		}
	}
	if got := shared.total.Load(); got != 100 {
		t.Fatalf("the budget counts %d bytes after the close, want the 100 still waiting", got)
	}
}

func TestAReplicaThatAcceptsNothingWithinTheStallTimeIsClosed(t *testing.T) {
	shared := newOutputs()
	shared.stallTimeout = 30 * time.Millisecond

	t.Run("a replica that accepts nothing is closed", func(t *testing.T) {
		wire := newStalledOutputWire()
		out := newGoroutineOutput(shared, wire)
		out.MarkReplica()
		if !out.Push([]byte("stream")) {
			t.Fatal("Push() = false, want the stream queued")
		}

		waitForOutput(t, "the stalled replica to be closed", func() bool { return out.Err() != nil })
		if err := out.Err(); !errors.Is(err, errReplicaStalled) {
			t.Fatalf("Err() = %v, want errReplicaStalled", err)
		}
		waitForOutput(t, "the socket to be closed with the Output", wire.isClosed)
	})

	t.Run("a replica that takes its stream stays", func(t *testing.T) {
		wire := newOutputWire()
		out := newGoroutineOutput(shared, wire)
		defer out.Close(nil)
		out.MarkReplica()

		for round := 0; round < 5; round++ {
			if !out.Push([]byte("stream")) {
				t.Fatalf("Push() = false in round %d, want the stream queued: %v", round, out.Err())
			}
			waitForOutput(t, "the stream to be written", func() bool { return out.Backlog() == 0 })
			time.Sleep(shared.stallTimeout)
		}
		if err := out.Err(); err != nil {
			t.Fatalf("Err() = %v, want a replica that is keeping up left open", err)
		}
	})

	t.Run("a client that is not a replica is not held to the stall rule", func(t *testing.T) {
		wire := newStalledOutputWire()
		out := newGoroutineOutput(shared, wire)
		defer out.Close(nil)
		if !out.Push([]byte("+message\r\n")) {
			t.Fatal("Push() = false, want the message queued")
		}

		time.Sleep(4 * shared.stallTimeout)
		if err := out.Err(); err != nil {
			t.Fatalf("Err() = %v, want a subscriber closed only by its limits", err)
		}
	})
}

// TestAReplicaIsNotStalledWhileItsStreamWaitsBehindAReplyThatHasNotCome marks
// a connection a Replica's and holds what is pushed to it, as a Replica's
// stream is held while its full resync reply is produced. Until that Reply
// comes the socket is offered nothing, however long the snapshot takes, so the
// Replica is not stalled. The stall rule used to count from the first frame
// that waited, and would close a Replica whose snapshot took longer than the
// stall timeout before it was sent a byte. Once the Reply has released the
// stream and a writer offers it to the socket, the rule counts.
func TestAReplicaIsNotStalledWhileItsStreamWaitsBehindAReplyThatHasNotCome(t *testing.T) {
	shared := newOutputs()
	shared.stallTimeout = 30 * time.Millisecond
	wire := newStalledOutputWire()
	out := newGoroutineOutput(shared, wire)
	out.MarkReplica()
	out.HoldPushes()
	if !out.Push([]byte("stream")) {
		t.Fatal("Push() = false, want the stream held")
	}

	time.Sleep(4 * shared.stallTimeout)
	if err := out.Err(); err != nil {
		t.Fatalf("Err() = %v with the stream held behind a reply that has not come, want the connection open", err)
	}

	if err := out.Reply(protocol.SimpleString{Value: "FULLRESYNC"}); err != nil {
		t.Fatalf("Reply() error = %v", err)
	}
	waitForOutput(t, "the replica that takes nothing of the reply to be closed", func() bool { return out.Err() != nil })
	if err := out.Err(); !errors.Is(err, errReplicaStalled) {
		t.Fatalf("Err() = %v, want errReplicaStalled", err)
	}
}

// nothingTaken is a socket of the Event loop whose send buffer is full: a write
// to it returns at once, having taken nothing.
type nothingTaken struct{}

func (nothingTaken) Write([]byte) (int, error) { return 0, nil }

// TestAReplicaIsNotStalledByOutputItsSocketWasNotOffered queues a Reply that
// is not flushed and then marks the connection a Replica's, as a replica that
// sends REPLCONF and PSYNC in one write has the +OK to its REPLCONF queued
// when the attach cut makes it a Replica. The connection then produces the
// snapshot, and offers the socket nothing until it has. The stall rule counted
// the Reply as waiting from the mark, and closed a Replica whose snapshot took
// longer than the stall timeout, although it read everything it was sent. The
// rule counts once a writer offers the batch and the socket takes nothing of
// it.
func TestAReplicaIsNotStalledByOutputItsSocketWasNotOffered(t *testing.T) {
	tests := []struct {
		name string
		open func(shared *outputs) (out *Output, offer func())
	}{
		{name: "by the goroutine mode's driver", open: func(shared *outputs) (*Output, func()) {
			out := newGoroutineOutput(shared, newStalledOutputWire())
			// The write blocks until the stall rule closes the connection.
			return out, func() { go func() { _ = out.Flush() }() }
		}},
		{name: "by the Event loop's flush", open: func(shared *outputs) (*Output, func()) {
			state := &ClientState{ID: 1}
			out := newOutput(shared, unscheduledOutputDriver{}, discardLogger)
			state.BindOutput(out)
			machine := NewConnMachine(state)
			return out, func() {
				if err := machine.Flush(nothingTaken{}); err != nil {
					t.Errorf("Flush() error = %v, want a full socket left for later", err)
				}
			}
		}},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			shared := newOutputs()
			shared.stallTimeout = 30 * time.Millisecond
			out, offer := tt.open(shared)
			defer out.Close(nil)

			if err := out.Reply(protocol.SimpleString{Value: "OK"}); err != nil {
				t.Fatalf("Reply() error = %v", err)
			}
			out.MarkReplica()
			time.Sleep(4 * shared.stallTimeout)
			if err := out.Err(); err != nil {
				t.Fatalf("Err() = %v with a reply queued that no writer has offered to the socket, want the connection open", err)
			}

			offer()
			waitForOutput(t, "the replica that takes nothing of what it is offered to be closed", func() bool { return out.Err() != nil })
			if err := out.Err(); !errors.Is(err, errReplicaStalled) {
				t.Fatalf("Err() = %v, want errReplicaStalled", err)
			}
		})
	}
}

// TestAClosedReplicasOutputIsNotKeptForTheStallTimeout closes a Replica's
// connection while the stall rule's timer is running, as a Replica that leaves
// in the middle of its full resync does, and waits for what the Output holds
// to be collected. The timer used to keep the Output reachable until it fired,
// and with it the batch that was being written: every Replica that left
// mid-snapshot held a buffer the size of the snapshot for 30 seconds.
//
// The socket stands for everything the Output holds. An Output refers to
// itself, and the collector runs no finalizer for such an object.
func TestAClosedReplicasOutputIsNotKeptForTheStallTimeout(t *testing.T) {
	collected := make(chan struct{})
	func() {
		wire := newStalledOutputWire()
		runtime.SetFinalizer(wire, func(*outputWire) { close(collected) })
		out := newGoroutineOutput(newOutputs(), wire)
		out.MarkReplica()
		if !out.Push([]byte("stream")) {
			t.Fatal("Push() = false, want the stream queued")
		}
		<-wire.writeBegun
		out.Close(nil)
	}()

	for deadline := time.Now().Add(5 * time.Second); ; {
		runtime.GC()
		select {
		case <-collected:
			return
		case <-time.After(10 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			t.Fatalf("what a closed replica's Output held was still reachable after 5s, want nothing to keep it for the stall timeout of %v", outputReplicaStallTimeout)
		}
	}
}

func TestCloseDoesNotWaitForAStuckWrite(t *testing.T) {
	wire := newStalledOutputWire()
	out := newGoroutineOutput(newOutputs(), wire)

	if err := out.Reply(protocol.SimpleString{Value: "PONG"}); err != nil {
		t.Fatalf("Reply() error = %v", err)
	}
	flushed := make(chan error, 1)
	go func() { flushed <- out.Flush() }()
	<-wire.writeBegun

	cause := errors.New("closed by the test")
	within(t, "Close behind a stuck write", func() { out.Close(cause) })
	within(t, "a second Close", func() { out.Close(nil) })

	select {
	case err := <-flushed:
		if err == nil {
			t.Fatal("Flush() error = nil, want the stuck write to fail once the connection is closed")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the stuck write was never released")
	}
	if err := out.Err(); !errors.Is(err, cause) {
		t.Fatalf("Err() = %v, want the first cause", err)
	}
	if out.Push([]byte("+message\r\n")) {
		t.Fatal("Push() after Close = true, want it refused")
	}
	if err := out.Flush(); !errors.Is(err, cause) {
		t.Fatalf("Flush() after Close error = %v, want the cause", err)
	}
}

func TestAFailedWriteClosesTheConnection(t *testing.T) {
	wire := newOutputWire()
	wire.writeErr = errors.New("connection reset by peer")
	out := newGoroutineOutput(newOutputs(), wire)

	if !out.Push([]byte("+message\r\n")) {
		t.Fatal("Push() = false, want the message queued")
	}
	waitForOutput(t, "the failed write to close the connection", func() bool { return out.Err() != nil })
	if err := out.Err(); !errors.Is(err, wire.writeErr) {
		t.Fatalf("Err() = %v, want the write's error", err)
	}
	waitForOutput(t, "the socket to be closed with the Output", wire.isClosed)
}

func TestEndStreamShutsTheWriteSideAfterWhatIsQueued(t *testing.T) {
	t.Run("with output still queued", func(t *testing.T) {
		wire := newOutputWire()
		out := newGoroutineOutput(newOutputs(), wire)
		defer out.Close(nil)

		out.HoldPushes()
		if !out.Push([]byte("stream")) {
			t.Fatal("Push() = false, want the stream queued")
		}
		if err := out.Reply(protocol.SimpleString{Value: "FULLRESYNC"}); err != nil {
			t.Fatalf("Reply() error = %v", err)
		}
		out.EndStream()

		waitForOutput(t, "the write side to be shut", func() bool { return wire.writeShutCount() == 1 })
		if got, want := wire.sent(), "+FULLRESYNC\r\nstream"; got != want {
			t.Fatalf("sent %q before the write side was shut, want %q", got, want)
		}
		if wire.isClosed() {
			t.Fatal("the socket is closed, want it left open for the peer to close")
		}
	})

	t.Run("with nothing queued", func(t *testing.T) {
		wire := newOutputWire()
		out := newGoroutineOutput(newOutputs(), wire)
		defer out.Close(nil)

		out.EndStream()
		out.EndStream()
		if got := wire.writeShutCount(); got != 1 {
			t.Fatalf("the write side was shut %d times, want once", got)
		}
	})
}

func TestConcurrentPushesAndRepliesAllArriveWhole(t *testing.T) {
	wire := newOutputWire()
	out := newGoroutineOutput(newOutputs(), wire)
	defer out.Close(nil)

	const pushers, each = 4, 500
	var wg sync.WaitGroup
	for p := 0; p < pushers; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < each; i++ {
				out.Push([]byte("+message\r\n"))
			}
		}()
	}
	for i := 0; i < each; i++ {
		if err := out.Reply(protocol.SimpleString{Value: "OK"}); err != nil {
			t.Fatalf("Reply() error = %v", err)
		}
		if err := out.Flush(); err != nil {
			t.Fatalf("Flush() error = %v", err)
		}
	}
	wg.Wait()

	want := pushers*each*len("+message\r\n") + each*len("+OK\r\n")
	waitForOutput(t, "everything to be written", func() bool { return len(wire.sent()) == want })
	parser := protocol.NewParser(strings.NewReader(wire.sent()))
	counts := map[string]int{}
	for i := 0; i < pushers*each+each; i++ {
		value, err := parser.Parse()
		if err != nil {
			t.Fatalf("frame %d: Parse() error = %v, want every frame whole", i, err)
		}
		counts[value.(protocol.SimpleString).Value]++
	}
	if counts["message"] != pushers*each || counts["OK"] != each {
		t.Fatalf("received %v, want %d messages and %d replies", counts, pushers*each, each)
	}
}

// TestAConnectionThatQueuedNoReplyNeverWaitsToFlush is a replica's connection
// with more than the high-water mark of its stream waiting behind a stuck
// write. Its goroutine flushes before every read and has no reply of its own
// to send, so it must not wait for the backlog: it would stop reading the
// replica's acknowledgements. A client with replies of its own waits there.
func TestAConnectionThatQueuedNoReplyNeverWaitsToFlush(t *testing.T) {
	wire := newStalledOutputWire()
	out := newGoroutineOutput(newOutputs(), wire)
	out.MarkReplica()

	if !out.Push(make([]byte, 2*outputHighWater)) {
		t.Fatal("Push() = false, want the stream queued")
	}
	<-wire.writeBegun
	within(t, "a flush with no reply queued", func() {
		if err := out.Flush(); err != nil {
			t.Errorf("Flush() error = %v, want nothing to do", err)
		}
	})

	if err := out.Reply(protocol.SimpleString{Value: "PONG"}); err != nil {
		t.Fatalf("Reply() error = %v", err)
	}
	flushed := make(chan error, 1)
	go func() { flushed <- out.Flush() }()
	select {
	case err := <-flushed:
		t.Fatalf("Flush() returned %v with a reply behind 2 MiB that the peer is not reading, want it to wait", err)
	case <-time.After(50 * time.Millisecond):
	}
	out.Close(nil)
	select {
	case <-flushed:
	case <-time.After(5 * time.Second):
		t.Fatal("Flush() still waiting after the connection was closed")
	}
}

// TestAReplicasSnapshotReplyIsNotCountedAgainstItsStreamLimit queues a full
// resync reply as large as the Replica Push limit and then the stream behind
// it. The limit is for the stream: counting the snapshot closed any replica
// whose snapshot reached the limit at its first propagated write, and it would
// synchronise again from the start, for ever.
func TestAReplicasSnapshotReplyIsNotCountedAgainstItsStreamLimit(t *testing.T) {
	shared := newOutputs()
	shared.replicaPushLimit = 1000
	wire := newStalledOutputWire()
	out := newGoroutineOutput(shared, wire)
	defer out.Close(nil)

	out.HoldPushes()
	out.MarkReplica()
	if !out.Push(make([]byte, 600)) {
		t.Fatalf("Push() of the stream held behind the reply = false: %v", out.Err())
	}
	if err := out.Reply(protocol.BulkString{Data: make([]byte, 5000)}); err != nil {
		t.Fatalf("Reply() error = %v", err)
	}
	if !out.Push(make([]byte, 300)) {
		t.Fatalf("Push() = false with 900 bytes of stream behind a 5000 byte snapshot, want only the stream counted: %v", out.Err())
	}

	if out.Push(make([]byte, 200)) {
		t.Fatal("Push() = true with 1100 bytes of stream waiting against a limit of 1000")
	}
	if err := out.Err(); !errors.Is(err, errPushLimitExceeded) {
		t.Fatalf("Err() = %v, want errPushLimitExceeded", err)
	}
}

// slowOutputWire takes a moment for every write, like a socket whose peer reads
// steadily but not fast.
type slowOutputWire struct {
	outputWire
	delay time.Duration
}

func (w *slowOutputWire) Write(p []byte) (int, error) {
	time.Sleep(w.delay)
	return w.outputWire.Write(p)
}

// TestAFlushLeavesWhatIsPushedMeanwhileToThePump flushes a reply while other
// connections keep pushing. The connection's own goroutine writes the batch its
// reply is in and returns. It used to go on writing for as long as pushes kept
// arriving, and so never read the peer's next request: for a replica under
// steady writes, its acknowledgements.
func TestAFlushLeavesWhatIsPushedMeanwhileToThePump(t *testing.T) {
	wire := &slowOutputWire{outputWire: *newOutputWire(), delay: 2 * time.Millisecond}
	out := newOutput(newOutputs(), &goroutineOutputDriver{wire: wire}, discardLogger)
	defer out.Close(nil)

	if err := out.Reply(protocol.BulkString{Data: make([]byte, 8<<10)}); err != nil {
		t.Fatalf("Reply() error = %v", err)
	}
	stop := make(chan struct{})
	var pusher sync.WaitGroup
	pusher.Add(1)
	go func() {
		defer pusher.Done()
		for {
			select {
			case <-stop:
				return
			default:
				out.Push([]byte("+message\r\n"))
				time.Sleep(200 * time.Microsecond)
			}
		}
	}()
	defer func() {
		close(stop)
		pusher.Wait()
	}()

	within(t, "a flush while pushes keep arriving", func() {
		if err := out.Flush(); err != nil {
			t.Errorf("Flush() error = %v", err)
		}
	})
	// The pushes are still written, by the pump.
	sent := len(wire.sent())
	waitForOutput(t, "the pump to write what was pushed after the flush", func() bool { return len(wire.sent()) > sent })
}

// heldOutputWire is a socket that takes nothing until it is released and
// everything after that, like a peer that was slow for a while.
type heldOutputWire struct {
	*outputWire
	released chan struct{}
}

func (w *heldOutputWire) Write(p []byte) (int, error) {
	<-w.released
	return w.outputWire.Write(p)
}

// TestPushesBehindASlowSocketArriveInOrderInFewWrites pushes a thousand frames
// while the socket takes nothing, as a burst of writes reaches a Replica that
// is slow to read. No push waits for the socket. Once it takes bytes again,
// the frames arrive in the order they were pushed, and in two writes at most:
// the batch that was being written, then everything queued behind it.
func TestPushesBehindASlowSocketArriveInOrderInFewWrites(t *testing.T) {
	wire := &heldOutputWire{outputWire: newOutputWire(), released: make(chan struct{})}
	out := newOutput(newOutputs(), &goroutineOutputDriver{wire: wire}, discardLogger)
	defer out.Close(nil)

	var want strings.Builder
	within(t, "a thousand pushes behind a socket that takes nothing", func() {
		for i := 0; i < 1000; i++ {
			frame := fmt.Sprintf("+message-%04d\r\n", i)
			want.WriteString(frame)
			if !out.Push([]byte(frame)) {
				t.Errorf("Push() %d = false, want it queued", i)
				return
			}
		}
	})
	if got := wire.sent(); got != "" {
		t.Fatalf("sent %q before the socket took anything", got)
	}

	close(wire.released)
	waitForOutput(t, "everything pushed to be written", func() bool { return len(wire.sent()) == want.Len() })
	if got := wire.sent(); got != want.String() {
		t.Fatal("the frames did not arrive in the order they were pushed")
	}
	if got := wire.writeCount(); got > 2 {
		t.Fatalf("the socket was written %d times for 1000 frames, want at most 2", got)
	}
}

// TestOutputIsWrittenInChunksSoTheStallRuleBoundsEachStretch queues more than
// three chunks at once. In the goroutine mode a write returns only when the
// socket has taken all of it, so the Replica stall rule can only see progress
// between writes: one write of the whole backlog would have to finish within
// the stall timeout, however much was waiting.
func TestOutputIsWrittenInChunksSoTheStallRuleBoundsEachStretch(t *testing.T) {
	const total = 3*outputWriteChunk + 10

	tests := []struct {
		name  string
		write func(t *testing.T, wire *outputWire)
	}{
		{name: "by the goroutine mode's pump", write: func(t *testing.T, wire *outputWire) {
			out := newGoroutineOutput(newOutputs(), wire)
			t.Cleanup(func() { out.Close(nil) })
			if !out.Push(make([]byte, total)) {
				t.Fatalf("Push() = false, want it queued: %v", out.Err())
			}
			waitForOutput(t, "everything pushed to be written", func() bool { return len(wire.sent()) == total })
		}},
		{name: "by the Event loop's flush", write: func(t *testing.T, wire *outputWire) {
			state := &ClientState{ID: 1}
			machine := NewConnMachine(state)
			if !state.Output().Push(make([]byte, total)) {
				t.Fatalf("Push() = false, want it queued: %v", state.Output().Err())
			}
			if err := machine.Flush(wire); err != nil {
				t.Fatalf("Flush() error = %v", err)
			}
		}},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			wire := newOutputWire()
			tt.write(t, wire)

			if got := len(wire.sent()); got != total {
				t.Fatalf("wrote %d bytes, want %d", got, total)
			}
			if got := wire.largestWrite(); got != outputWriteChunk {
				t.Fatalf("the largest write was %d bytes, want the %d byte chunk", got, outputWriteChunk)
			}
			if got := wire.writeCount(); got != 4 {
				t.Fatalf("the socket was written %d times, want three chunks and the 10 bytes left", got)
			}
		})
	}
}

// TestAPushKeepsNoReferenceToItsFrame overwrites a frame once Push has
// returned. The Recorder hands the stream a buffer it reuses for the next
// write as soon as Propagate returns, so the Output must have copied it,
// whether the frame is queued or held behind a Reply.
func TestAPushKeepsNoReferenceToItsFrame(t *testing.T) {
	tests := []struct {
		name string
		held bool
		want string
	}{
		{name: "a frame that is queued", want: "+one\r\n"},
		{name: "a frame held behind a reply", held: true, want: "+OK\r\n+one\r\n"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			wire := newOutputWire()
			out := newGoroutineOutput(newOutputs(), wire)
			defer out.Close(nil)

			if tt.held {
				out.HoldPushes()
			}
			frame := []byte("+one\r\n")
			if !out.Push(frame) {
				t.Fatal("Push() = false, want it queued")
			}
			copy(frame, "+XXX")
			if tt.held {
				if err := out.Reply(protocol.SimpleString{Value: "OK"}); err != nil {
					t.Fatalf("Reply() error = %v", err)
				}
				if err := out.Flush(); err != nil {
					t.Fatalf("Flush() error = %v", err)
				}
			}

			waitForOutput(t, "the frame to be written", func() bool { return len(wire.sent()) == len(tt.want) })
			if got := wire.sent(); got != tt.want {
				t.Fatalf("sent %q, want %q: the frame as it was when it was pushed", got, tt.want)
			}
		})
	}
}

// TestPushesHeldWhenTheConnectionClosesAreNeverWritten closes a connection
// while Pushed messages wait behind a Reply that has not come, as a Replica is
// closed for falling behind while its full resync is still being produced.
// Closing does not wait for the Reply, what was held is dropped, and the Reply
// that comes afterwards is refused, so nothing of it reaches the socket.
func TestPushesHeldWhenTheConnectionClosesAreNeverWritten(t *testing.T) {
	wire := newOutputWire()
	out := newGoroutineOutput(newOutputs(), wire)

	out.HoldPushes()
	if !out.Push([]byte("+held\r\n")) {
		t.Fatal("Push() = false, want it held")
	}
	cause := errors.New("closed by the test")
	within(t, "Close with pushes held behind a reply", func() { out.Close(cause) })

	if !wire.isClosed() {
		t.Fatal("the socket is open, want it closed with the Output")
	}
	if got := out.Backlog(); got != 0 {
		t.Fatalf("Backlog() = %d after the close, want what was held dropped", got)
	}
	if out.Push([]byte("+late\r\n")) {
		t.Fatal("Push() after Close = true, want it refused")
	}
	if err := out.Reply(protocol.SimpleString{Value: "OK"}); !errors.Is(err, cause) {
		t.Fatalf("Reply() after the close error = %v, want why it was closed", err)
	}
	if err := out.Flush(); !errors.Is(err, cause) {
		t.Fatalf("Flush() after the close error = %v, want why it was closed", err)
	}
	if got := wire.sent(); got != "" {
		t.Fatalf("sent %q, want nothing written for a connection closed before its reply", got)
	}
}

// outputDrivers are the two ways an Output's bytes reach a socket, for a test
// of what both must write: fill queues output on out, and send returns once
// want bytes of it have reached wire.
var outputDrivers = []struct {
	name string
	open func(t *testing.T, wire *outputWire) (out *Output, send func(want int))
}{
	{name: "by the goroutine mode's driver", open: func(t *testing.T, wire *outputWire) (*Output, func(int)) {
		out := newGoroutineOutput(newOutputs(), wire)
		t.Cleanup(func() { out.Close(nil) })
		return out, func(want int) {
			if err := out.Flush(); err != nil {
				t.Fatalf("Flush() error = %v", err)
			}
			waitForOutput(t, "everything queued to be written", func() bool { return len(wire.sent()) >= want })
		}
	}},
	{name: "by the Event loop's flush", open: func(t *testing.T, wire *outputWire) (*Output, func(int)) {
		state := &ClientState{ID: 1}
		machine := NewConnMachine(state)
		return state.Output(), func(int) {
			if err := machine.Flush(wire); err != nil {
				t.Fatalf("Flush() error = %v", err)
			}
		}
	}},
}

// TestALargeReplyKeepsItsPlaceAmongWhatIsQueued queues Replies of
// outputRetainedBuffer and more, which an Output takes whole instead of
// copying them behind what is already queued, between small Replies and Pushed
// messages. Everything is written in the order it was queued: what was queued
// before a large Reply, the Reply, the Pushed messages it released, and what
// was pushed after it.
func TestALargeReplyKeepsItsPlaceAmongWhatIsQueued(t *testing.T) {
	large := func(fill byte) protocol.BulkString {
		return protocol.BulkString{Data: bytes.Repeat([]byte{fill}, outputRetainedBuffer)}
	}
	encoded := func(values ...protocol.Value) string {
		payload, err := protocol.EncodeValues(values)
		if err != nil {
			t.Fatalf("EncodeValues() error = %v", err)
		}
		return string(payload)
	}
	reply := func(t *testing.T, out *Output, values ...protocol.Value) {
		t.Helper()
		if err := out.Reply(values...); err != nil {
			t.Fatalf("Reply() error = %v", err)
		}
	}
	push := func(t *testing.T, out *Output, frame string) {
		t.Helper()
		if !out.Push([]byte(frame)) {
			t.Fatalf("Push() = false, want it queued: %v", out.Err())
		}
	}

	tests := []struct {
		name string
		fill func(t *testing.T, out *Output)
		want string
	}{
		{
			name: "between small replies, with pushes held behind it and pushed after it",
			fill: func(t *testing.T, out *Output) {
				reply(t, out, protocol.SimpleString{Value: "first"})
				out.HoldPushes()
				push(t, out, "+held\r\n")
				reply(t, out, large('a'))
				push(t, out, "+pushed\r\n")
				reply(t, out, protocol.SimpleString{Value: "last"})
			},
			want: "+first\r\n" + encoded(large('a')) + "+held\r\n+pushed\r\n+last\r\n",
		},
		{
			name: "with other values in the same reply, as a full resync reply has",
			fill: func(t *testing.T, out *Output) {
				out.HoldPushes()
				push(t, out, "+stream\r\n")
				reply(t, out, protocol.SimpleString{Value: "FULLRESYNC"}, large('s'))
				push(t, out, "+more\r\n")
			},
			want: "+FULLRESYNC\r\n" + encoded(large('s')) + "+stream\r\n+more\r\n",
		},
		{
			name: "one after another",
			fill: func(t *testing.T, out *Output) {
				push(t, out, "+before\r\n")
				reply(t, out, large('a'))
				reply(t, out, large('b'))
				push(t, out, "+between\r\n")
				reply(t, out, large('c'))
			},
			want: "+before\r\n" + encoded(large('a'), large('b')) + "+between\r\n" + encoded(large('c')),
		},
		{
			name: "small, releasing that much of held pushes",
			fill: func(t *testing.T, out *Output) {
				reply(t, out, protocol.SimpleString{Value: "first"})
				out.HoldPushes()
				push(t, out, strings.Repeat("h", outputRetainedBuffer))
				push(t, out, "+held last\r\n")
				reply(t, out, protocol.SimpleString{Value: "OK"})
				push(t, out, "+pushed\r\n")
			},
			want: "+first\r\n+OK\r\n" + strings.Repeat("h", outputRetainedBuffer) + "+held last\r\n+pushed\r\n",
		},
	}

	for _, driver := range outputDrivers {
		driver := driver
		t.Run(driver.name, func(t *testing.T) {
			for _, tt := range tests {
				tt := tt
				t.Run(tt.name, func(t *testing.T) {
					wire := newOutputWire()
					out, send := driver.open(t, wire)

					tt.fill(t, out)
					send(len(tt.want))

					if got := wire.sent(); got != tt.want {
						t.Fatalf("sent %d bytes that differ from the %d queued, in the order they were queued, at byte %d", len(got), len(tt.want), firstDifference(got, tt.want))
					}
					waitForOutput(t, "the backlog to count everything as written", func() bool { return out.Backlog() == 0 })
				})
			}
		})
	}
}

// firstDifference is the index of the first byte at which a and b differ.
func firstDifference(a, b string) int {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return min(len(a), len(b))
}

// TestAFlushWritesEveryReplyQueuedBeforeIt flushes a small Reply and a large
// one behind it, with nothing pushed. The connection's own goroutine writes
// both before Flush returns: a Reply left to another goroutine would be dropped
// when the handler, with nothing more to read, closes the connection.
func TestAFlushWritesEveryReplyQueuedBeforeIt(t *testing.T) {
	wire := newOutputWire()
	out := newGoroutineOutput(newOutputs(), wire)
	defer out.Close(nil)

	large := protocol.BulkString{Data: make([]byte, outputRetainedBuffer)}
	want, err := protocol.EncodeValues([]protocol.Value{protocol.SimpleString{Value: "PONG"}, large, protocol.SimpleString{Value: "PONG"}})
	if err != nil {
		t.Fatalf("EncodeValues() error = %v", err)
	}
	for _, value := range []protocol.Value{protocol.SimpleString{Value: "PONG"}, large, protocol.SimpleString{Value: "PONG"}} {
		if err := out.Reply(value); err != nil {
			t.Fatalf("Reply() error = %v", err)
		}
	}
	if err := out.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	if got := len(wire.sent()); got != len(want) {
		t.Fatalf("%d bytes were written when Flush returned, want all %d of the replies", got, len(want))
	}
}

// lockHoldingDriver is a driver whose schedule waits until it is released. An
// Output calls schedule with its lock held, so a Push to an Output it drives
// stays inside that lock, where a test can keep it.
type lockHoldingDriver struct {
	unscheduledOutputDriver
	entered chan struct{}
	release chan struct{}
}

func (d *lockHoldingDriver) schedule(*Output) {
	close(d.entered)
	<-d.release
}

// allocatedBytes is how many bytes the process has allocated so far.
func allocatedBytes() uint64 {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return stats.TotalAlloc
}

// TestALargeReplyIsEncodedWhileAPushHoldsTheOutputsLock sends a full resync
// reply to an Output whose lock a Push holds, and waits for the snapshot's
// buffer to be allocated while that Push still holds it. A Reply that needs
// the lock to encode would also keep every Push out while it does, and the
// replication Master pushes a Replica's stream under its own lock, where every
// writer waits: a write waited two seconds for a 384 MiB snapshot.
func TestALargeReplyIsEncodedWhileAPushHoldsTheOutputsLock(t *testing.T) {
	driver := &lockHoldingDriver{entered: make(chan struct{}), release: make(chan struct{})}
	out := newOutput(newOutputs(), driver, discardLogger)

	pushed := make(chan bool, 1)
	go func() { pushed <- out.Push([]byte("+write\r\n")) }()
	<-driver.entered

	snapshot := protocol.BulkString{Data: make([]byte, 16<<20)}
	before := allocatedBytes()
	replied := make(chan error, 1)
	go func() { replied <- out.Reply(protocol.SimpleString{Value: "FULLRESYNC"}, snapshot) }()

	waitForOutput(t, "the Reply to encode its snapshot while a Push holds the Output's lock", func() bool {
		return allocatedBytes()-before >= uint64(len(snapshot.Data))
	})
	select {
	case err := <-replied:
		t.Fatalf("Reply() returned %v while a Push held the Output's lock, want it to queue what it encoded after that Push", err)
	default:
	}

	close(driver.release)
	if !<-pushed {
		t.Fatal("Push() = false, want it queued")
	}
	if err := <-replied; err != nil {
		t.Fatalf("Reply() error = %v", err)
	}
	want, err := protocol.EncodeValues([]protocol.Value{protocol.SimpleString{Value: "FULLRESYNC"}, snapshot})
	if err != nil {
		t.Fatalf("EncodeValues() error = %v", err)
	}
	wire := newOutputWire()
	if err := out.writeTo(wire); err != nil {
		t.Fatalf("writeTo() error = %v", err)
	}
	if got := wire.sent(); got != "+write\r\n"+string(want) {
		t.Fatalf("sent %d bytes, want the push and then the %d bytes of the reply", len(got), len(want))
	}
}

// TestAReplyReleasesLargeHeldPushesWithoutCopyingThem holds 8 MiB of a
// Replica's stream behind a Reply. Releasing them is done under the Output's
// lock, so they are queued as the buffer they are in: copying them behind the
// Reply would keep every Push out for the time of the copy.
func TestAReplyReleasesLargeHeldPushesWithoutCopyingThem(t *testing.T) {
	const held = 8 << 20
	wire := newOutputWire()
	state := &ClientState{ID: 1}
	machine := NewConnMachine(state)
	out := state.Output()

	out.HoldPushes()
	frame := bytes.Repeat([]byte("s"), 1<<20)
	for pushed := 0; pushed < held; pushed += len(frame) {
		if !out.Push(frame) {
			t.Fatalf("Push() = false, want the stream held: %v", out.Err())
		}
	}

	before := allocatedBytes()
	if err := out.Reply(protocol.SimpleString{Value: "OK"}); err != nil {
		t.Fatalf("Reply() error = %v", err)
	}
	if allocated := allocatedBytes() - before; allocated >= held {
		t.Fatalf("the Reply allocated %d bytes to release %d bytes of held pushes, want them queued without a copy", allocated, held)
	}

	if err := machine.Flush(wire); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	if got, want := wire.sent(), "+OK\r\n"+strings.Repeat("s", held); got != want {
		t.Fatalf("sent %d bytes, want the reply and then the %d bytes held behind it", len(got), held)
	}
}

// TestAClientWithNoOutputIsAClosedConnectionToTheMaster hands the Master the
// Output of a Client state that was never bound to a connection, which is nil,
// as PSYNC would. Attaching it does nothing to it, it refuses the first frame
// with the cause of a closed connection, and so the Master detaches the
// Replica as one whose connection ended.
func TestAClientWithNoOutputIsAClosedConnectionToTheMaster(t *testing.T) {
	out := (&ClientState{ID: 1}).Output()

	out.MarkReplica()
	out.HoldPushes()
	out.EndStream()
	if out.Push([]byte("+message\r\n")) {
		t.Fatal("Push() = true for a client with no connection, want it refused")
	}
	if err := out.Err(); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("Err() = %v, want a closed connection's", err)
	}
}
