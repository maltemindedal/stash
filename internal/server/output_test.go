package server

import (
	"bytes"
	"errors"
	"net"
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
