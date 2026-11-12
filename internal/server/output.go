package server

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maltemindedal/stash/internal/protocol"
)

// outputPushLimit is how much Client output may wait for a connection that
// receives Pushed messages (pub/sub messages, MONITOR events) before it is
// closed. It is Redis's pub/sub hard limit: a subscriber that has not drained
// 32 MiB is not keeping up. A connection that receives none is not held to it,
// since a Reply can be as large as a stored value.
const outputPushLimit = 32 << 20

// outputReplicaPushLimit is the same limit for a Replica, which is expected to
// fall further behind during a burst of writes: Redis's replica hard limit.
const outputReplicaPushLimit = 256 << 20

// outputPushBudget bounds the Client output waiting for every connection that
// receives Pushed messages together, Replicas excluded. A message is copied
// once per subscriber, so many slow subscribers each below their own limit
// could otherwise hold many times the memory of one. Above it, the connection
// with the most waiting is closed.
const outputPushBudget = 128 << 20

// outputReplicaStallTimeout is how long a Replica's socket may take nothing of
// a batch of output it is offered before the Replica is dropped. Output that no
// writer has offered to the socket does not count, since the Replica is not
// failing to take it: Replies that are not flushed yet, and Pushed messages
// held behind a Reply that has not come. Nor does the time the Event loop
// spends away from the socket: there the Replica is dropped when the loop's
// next try after that time takes nothing either (checkStall).
const outputReplicaStallTimeout = 30 * time.Second

// outputWriteThrough is how much Client output a connection's goroutine lets
// wait before a Reply writes it out without waiting for the next read, as a
// 4 KiB bufio.Writer would.
const outputWriteThrough = 4 << 10

// outputHighWater bounds what may be left to write before a connection's
// latest Reply is out (replyBacklogLocked). Above it the connection's own
// requests stop being read and run until the peer has read down to it, so that
// a client that pipelines without reading cannot turn small requests into
// unbounded Replies. What was pushed to the connection ahead of that Reply
// counts, since it is written first. What is pushed after it does not: the
// Push limit bounds that, and a Replica with its stream waiting must still
// have its acknowledgements read. The Event loop holds everything that waits
// for a connection by the same rule (MayReadRequests).
const outputHighWater = 1 << 20

// outputWriteChunk is the most bytes handed to a socket in one write. In the
// goroutine mode a write blocks until the socket has taken all of it, so this
// is also the stretch of a Replica's stream that must be accepted within
// outputReplicaStallTimeout.
const outputWriteChunk = 1 << 20

// outputRetainedBuffer is the capacity above which a drained buffer is released
// rather than kept for the next batch: a large Reply would otherwise leave its
// size on every connection that asked for one.
const outputRetainedBuffer = 2 << 20

// errPushLimitExceeded reports that more Client output is waiting for a
// connection that receives Pushed messages, or a Replica's stream, than it may
// hold.
var errPushLimitExceeded = errors.New("server: more output waiting than the connection's push limit")

// errPushBudgetExceeded reports that a connection was closed to bring the
// output waiting for all push-receiving connections back within the budget.
var errPushBudgetExceeded = errors.New("server: closed to stay within the budget for pushed output")

// errReplicaStalled reports that a Replica's socket took nothing for
// outputReplicaStallTimeout of the output it was offered.
var errReplicaStalled = errors.New("server: replica accepted no output within the stall timeout")

// errOutputClosed is what a closed Output reports when it was closed without a
// cause, as an ordinary teardown closes it.
var errOutputClosed = fmt.Errorf("server: client output closed: %w", net.ErrClosed)

// outputs holds what every connection's Output shares: the limits, and the
// Push budget's running total. Its lock comes after any Output's.
type outputs struct {
	pushLimit        int64
	replicaPushLimit int64
	pushBudget       int64
	writeThrough     int64
	stallTimeout     time.Duration

	// total is the output waiting for the connections in pushers.
	total atomic.Int64

	mu sync.Mutex
	// pushers are the open, non-replica connections that have received a
	// Pushed message: the ones the budget counts and may close.
	pushers map[*Output]struct{}
}

func newOutputs() *outputs {
	return &outputs{
		pushLimit:        outputPushLimit,
		replicaPushLimit: outputReplicaPushLimit,
		pushBudget:       outputPushBudget,
		writeThrough:     outputWriteThrough,
		stallTimeout:     outputReplicaStallTimeout,
		pushers:          make(map[*Output]struct{}),
	}
}

// enforceBudget closes push-receiving connections, the one with the most
// waiting first, until their total is back within the budget. It holds no
// Output's lock: it reads each one's atomic backlog.
func (s *outputs) enforceBudget() {
	for s.total.Load() > s.pushBudget {
		var victim *Output
		var most int64
		s.mu.Lock()
		for o := range s.pushers {
			if waiting := o.backlog.Load(); waiting > most {
				victim, most = o, waiting
			}
		}
		s.mu.Unlock()
		if victim == nil {
			return
		}
		victim.Close(fmt.Errorf("%w of %d bytes: it had the most waiting, %d bytes", errPushBudgetExceeded, s.pushBudget, most))
	}
}

// outputDriver is how one networking mode gets an Output's bytes to its
// socket. The goroutine mode writes with blocking writes, from the
// connection's own goroutine or from a pump goroutine that runs only while
// Pushed messages wait. The Event loop writes everything itself, and is told
// which connections have output to write.
type outputDriver interface {
	// schedule is called with Output.mu held when bytes were queued that the
	// connection's own executor will not flush: a Pushed message, or the ones a
	// Reply released. It must not block.
	schedule(o *Output)
	// flush is Output.Flush, which waits for another writer while more than
	// mark is left to write before the connection's latest Reply is out, and
	// with a mark of zero Output.Drain.
	flush(o *Output, mark int64) error
	// shut closes the connection, once, without waiting for a write in
	// progress. It is called with no Output lock held.
	shut()
	// closeWrite ends the stream to the peer after everything queued was
	// written: Output.EndStream.
	closeWrite()
	// peerClosed reports whether the peer has closed its side, when the driver
	// can tell without reading.
	peerClosed() bool
	// retry asks the driver's writer to offer the socket the batch that is
	// inflight once more, and reports whether it will. The Event loop does: its
	// writes return at once, and it comes back to a socket that took part of a
	// batch only when the socket can take more or when it is asked to. A writer
	// of the goroutine mode stays inside its write for as long as a batch is on
	// offer, so there is nothing to ask of it. It is called with Output.mu held
	// and must not block.
	retry(o *Output) bool
}

// stallPhase is where the Replica stall rule stands for an Output.
type stallPhase uint8

const (
	// stallIdle is the rule at rest: nothing is on offer to a Replica's socket.
	stallIdle stallPhase = iota
	// stallCounting has a timer due to look at lastProgress (checkStall).
	stallCounting
	// stallDue is the stall timeout passed with nothing taken, on a driver
	// whose writer was asked to try the socket once more (retry). What that
	// try takes is the verdict (write).
	stallDue
)

// Output is the Client output of one connection: its Replies and the Pushed
// messages it receives (pub/sub messages, MONITOR events, and for a Replica
// its stream), in the order they were produced. No method but Flush and Drain
// waits on the peer's socket. Safe for concurrent use.
//
// Lock order: the lock of whoever pushes (a registry's, or the replication
// Master's for a Replica's stream), then Output.mu, then the shared budget's.
// Output.mu is never held across a socket write, and nothing called under it
// takes a pusher's lock.
type Output struct {
	mu sync.Mutex
	// ahead are whole buffers waiting to be written before queued, in order, and
	// aheadBytes is how many bytes they hold. A large Reply is queued here
	// without being copied (Reply), behind the buffer that was queued until then.
	// A Flush that finds one here seals what is queued behind it too
	// (sealBatchLocked).
	ahead      [][]byte
	aheadBytes int
	// queued is what waits to be written after ahead, appended under mu.
	queued []byte
	// held are the Pushed messages waiting behind the current request's Reply.
	held []byte
	// inflight is the batch being written; inflight[sent:] is still to go. Both
	// are written only by whoever is writing, under mu.
	inflight []byte
	sent     int
	// spare is an emptied buffer kept for the next batch that is queued while
	// another is being written.
	spare []byte
	// writing is set while a goroutine of the goroutine mode owns the socket.
	writing bool
	// written is how many bytes the socket has taken since the connection
	// opened, and replyEnd is where the latest Reply ends, counted the same way
	// (queueEndLocked): the Replies are all written once written has reached
	// replyEnd. What was pushed ahead of a Reply is written before that mark is
	// reached, and what is pushed after the latest Reply lies behind it.
	written  int64
	replyEnd int64
	holding  bool
	closed   bool
	cause    error
	// replica holds the connection to the Replica Push limit and stall rule,
	// and leaves it out of the Push budget.
	replica bool
	// counted is set while the connection is one of shared.pushers.
	counted bool
	// ending is set once the stream is to end when everything queued has been
	// written (EndStream).
	ending bool
	// lastProgress is when the socket last accepted a byte, or when it was
	// offered the batch that is inflight, and stall is where the stall rule
	// stands on it. The rule's timer finds the Output through stallWatch, which
	// Close empties.
	lastProgress time.Time
	stall        stallPhase
	stallWatch   *atomic.Pointer[Output]

	// drained is signalled when a writer finishes, when the connection is
	// closed, and after every write that leaves no more than outputHighWater to
	// write before the latest Reply is out, the one that writes its last byte
	// included.
	drained *sync.Cond
	// backlog is every byte in queued, held and inflight[sent:].
	backlog atomic.Int64

	driver outputDriver
	shared *outputs
	logger *slog.Logger
}

func newOutput(shared *outputs, driver outputDriver, logger *slog.Logger) *Output {
	o := &Output{driver: driver, shared: shared, logger: logger}
	o.drained = sync.NewCond(&o.mu)
	return o
}

// NewOutput returns the Client output of a connection served by its own
// goroutine, writing to w. Closing the Output closes w when it is an io.Closer.
// The server builds each connection's Output itself; this is for code that
// drives a Client state without a server.
func NewOutput(w io.Writer) *Output {
	driver := &goroutineOutputDriver{wire: w}
	return newOutput(newOutputs(), driver, discardLogger)
}

// Reply queues the Replies to the request the connection just ran (none is
// allowed) and releases the Pushed messages held since HoldPushes. Only the
// connection's own executor calls it. Replies leave together when a
// connection's goroutine next waits for input (Flush), unless outputWriteThrough
// is waiting already: Reply then writes them out as Flush does, and waits as
// Flush does. On the Event loop it never waits. A Push to the connection takes
// the lock Reply holds, so a large Reply is encoded before that lock is taken
// and queued whole (isLargeReply).
func (o *Output) Reply(values ...protocol.Value) error {
	var whole []byte
	if isLargeReply(values) {
		encoded, err := protocol.EncodeValues(values)
		if err != nil {
			return err
		}
		whole = encoded
	}

	o.mu.Lock()
	if o.closed {
		err := o.errLocked()
		o.mu.Unlock()
		return err
	}
	added := len(whole)
	if whole != nil {
		o.queueWholeLocked(whole)
	} else {
		before := len(o.queued)
		queued, err := protocol.AppendValues(o.queued, values)
		if err != nil {
			o.mu.Unlock()
			return err
		}
		o.queued = queued
		added = len(queued) - before
	}
	if added > 0 {
		o.growLocked(added)
		// Before the held Pushed messages are released, which follow the Reply.
		o.replyEnd = o.queueEndLocked()
	}

	if o.holding {
		o.holding = false
		if len(o.held) > 0 {
			if len(o.held) >= outputRetainedBuffer {
				o.queueWholeLocked(o.held)
				o.held = nil
			} else {
				o.queued = append(o.queued, o.held...)
				o.held = o.held[:0]
				if cap(o.held) > outputRetainedBuffer {
					o.held = nil
				}
			}
			// The connection's goroutine must not be the one that writes a stream
			// of Pushed messages to a peer that may have stopped reading.
			o.driver.schedule(o)
		}
	}
	o.mu.Unlock()

	if o.backlog.Load() >= o.shared.writeThrough {
		return o.driver.flush(o, outputHighWater)
	}
	return nil
}

// isLargeReply reports whether values hold at least outputRetainedBuffer bytes
// of bulk strings. Reply encodes such a Reply before it takes the lock, into a
// buffer of its own. A buffer that size is never kept for the next batch, so
// the Reply costs no allocation it would not have cost in queued.
//
// Only the bulk strings that are Replies themselves count, not those inside an
// array, so an ordinary Reply pays one type check per value and no walk. That
// covers the connection that matters: the one that is pushed to while it is
// sent a large Reply is a Replica during its full resync, whose stream writers
// push under the replication Master's lock, and its snapshot is one bulk
// string. A large array is still encoded under the lock.
func isLargeReply(values []protocol.Value) bool {
	size := 0
	for _, value := range values {
		if bulk, ok := value.(protocol.BulkString); ok {
			size += len(bulk.Data)
		}
	}
	return size >= outputRetainedBuffer
}

// queueWholeLocked queues buffer behind everything queued without copying it.
// What is in queued is ahead of it, so that buffer goes first.
func (o *Output) queueWholeLocked(buffer []byte) {
	o.sealQueuedLocked()
	o.ahead = append(o.ahead, buffer)
	o.aheadBytes += len(buffer)
}

// sealQueuedLocked makes what is in queued the last of the whole buffers, so
// that what is queued from now on waits behind it in a buffer of its own.
func (o *Output) sealQueuedLocked() {
	if len(o.queued) == 0 {
		return
	}
	o.ahead = append(o.ahead, o.queued)
	o.aheadBytes += len(o.queued)
	o.queued, o.spare = o.spare[:0], nil
}

// sealBatchLocked makes everything queued now the batch of a writer that
// writes nothing queued after it began (Flush), and returns how many buffers
// that is. Behind whole buffers, what is in queued becomes the last of them:
// left where it is, it would grow by what is pushed while the buffers ahead of
// it are written. With no whole buffer the batch is queued as it is, which the
// writer takes in one piece under its next hold of the lock: a Push that lands
// in that instant rides with the batch. It is one batch either way, which is
// what bounds the writer.
func (o *Output) sealBatchLocked() int {
	if len(o.ahead) == 0 {
		return 1
	}
	o.sealQueuedLocked()
	return len(o.ahead)
}

// Push queues a frame another connection produced. It never waits. It returns
// false if the connection is closed, or if this Push closed it for passing its
// Push limit or the Push budget.
func (o *Output) Push(frame []byte) bool {
	if o == nil {
		return false
	}

	o.mu.Lock()
	if o.closed {
		o.mu.Unlock()
		return false
	}
	// A subscriber is held to its limit for everything waiting, since only a
	// client that is not reading lets pushes pile up behind its Replies. A
	// Replica's limit is for its stream alone: the snapshot it is still being
	// sent can be larger than the limit, and says nothing of how far behind it
	// is. Its one Reply, the full resync, is ahead of its whole stream, so the
	// stream is what waits behind that Reply.
	limit := o.shared.pushLimit
	waiting := o.backlog.Load() + int64(len(frame))
	if o.replica {
		limit = o.shared.replicaPushLimit
		waiting -= o.replyBacklogLocked()
	}
	if waiting > limit {
		o.mu.Unlock()
		o.Close(fmt.Errorf("%w: %d bytes against a limit of %d", errPushLimitExceeded, waiting, limit))
		return false
	}

	if !o.replica && !o.counted {
		o.counted = true
		o.shared.total.Add(o.backlog.Load())
		o.shared.mu.Lock()
		o.shared.pushers[o] = struct{}{}
		o.shared.mu.Unlock()
	}
	if o.holding {
		o.held = append(o.held, frame...)
		o.growLocked(len(frame))
	} else {
		o.queued = append(o.queued, frame...)
		o.growLocked(len(frame))
		o.driver.schedule(o)
	}
	overBudget := o.counted && o.shared.total.Load() > o.shared.pushBudget
	o.mu.Unlock()

	if overBudget {
		o.shared.enforceBudget()
		return !o.isClosed()
	}
	return true
}

// HoldPushes makes Pushed messages queued from now on wait until the current
// request has replied. A request that changes which Pushed messages reach its
// own connection (SUBSCRIBE, UNSUBSCRIBE, MONITOR, the attach cut of a PSYNC)
// calls it at that change, so that what the change lets in follows the
// request's Reply.
func (o *Output) HoldPushes() {
	if o == nil {
		return
	}
	o.mu.Lock()
	o.holding = true
	o.mu.Unlock()
}

// Flush writes out what is queued, for the connection's own goroutine: before
// it waits for the next request and before a command blocks. When another goroutine is already writing it leaves the
// writing to that one, and waits only while more than outputHighWater is left
// to write before the connection's latest Reply is out: a client that does not
// read what it is sent stops having its requests run. It reports why the
// connection is closed, if it is. The Event loop writes its connections
// itself, so there Flush does nothing.
func (o *Output) Flush() error {
	if o == nil {
		return nil
	}
	return o.driver.flush(o, outputHighWater)
}

// Drain returns once every Reply queued so far has been written, or the
// connection is closed. It is for the connection's own goroutine when it has
// run its last request: the Close that follows drops what is still queued.
// Like Flush it writes what is queued when nobody has the socket. When another
// goroutine has it, Drain waits until that one has written the latest Reply.
//
// It does not wait for what was pushed after that Reply, beyond the write that
// carries the Reply's last byte. The goroutine of a Replica that has been sent
// its full resync reply therefore leaves at once, however much of its stream
// waits. A peer that never reads keeps the goroutine here until the connection
// is closed, by shutdown or by a limit on what is pushed to it, as a Reply to
// a peer that never reads keeps it in Flush. The Event loop writes out a
// closing connection itself (ConnMachine.Flush), so there Drain does nothing.
func (o *Output) Drain() {
	// Why the connection is closed, if it is, changes nothing for a caller
	// that is about to close it.
	_ = o.driver.flush(o, 0)
}

// Close closes the connection from any goroutine without waiting for a write in
// progress, and drops what was waiting for it. It never touches registries;
// teardown abandons the Client state.
func (o *Output) Close(cause error) {
	if o == nil {
		return
	}

	o.mu.Lock()
	if o.closed {
		o.mu.Unlock()
		return
	}
	o.closed = true
	o.cause = cause
	waiting := o.backlog.Load()
	o.growLocked(-int(waiting))
	o.ahead, o.aheadBytes, o.queued, o.held = nil, 0, nil, nil
	if o.stallWatch != nil {
		o.stallWatch.Store(nil)
	}
	if o.counted {
		o.counted = false
		o.shared.mu.Lock()
		delete(o.shared.pushers, o)
		o.shared.mu.Unlock()
	}
	o.drained.Broadcast()
	o.mu.Unlock()

	if errors.Is(cause, errPushLimitExceeded) || errors.Is(cause, errPushBudgetExceeded) || errors.Is(cause, errReplicaStalled) {
		o.logger.Warn("closing a connection that is not keeping up with what is pushed to it", "pending_bytes", waiting, "error", cause)
	}
	o.driver.shut()
}

// EndStream ends the stream to the peer once everything queued has been
// written: the write side is shut and the connection stays open for the peer to
// close. Shutdown ends a Replica's stream this way
// (replication.Master.EndStreams).
func (o *Output) EndStream() {
	if o == nil {
		return
	}

	o.mu.Lock()
	if o.closed || o.ending {
		o.mu.Unlock()
		return
	}
	o.ending = true
	idle := !o.writing && o.pendingLocked() == 0
	if !idle {
		o.driver.schedule(o)
	}
	o.mu.Unlock()

	if idle {
		o.driver.closeWrite()
	}
}

// MarkReplica makes the connection a Replica's: held to the Replica Push limit
// and stall rule, and outside the Push budget. The attach cut calls it before
// the Replica's stream is pushed here (replication.Master.Attach).
func (o *Output) MarkReplica() {
	if o == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()

	o.replica = true
	if o.counted {
		o.counted = false
		o.shared.total.Add(-o.backlog.Load())
		o.shared.mu.Lock()
		delete(o.shared.pushers, o)
		o.shared.mu.Unlock()
	}
	if o.offeredLocked() {
		o.countStallLocked()
	}
}

// Backlog reports how many bytes of Client output wait for the connection.
func (o *Output) Backlog() int {
	if o == nil {
		return 0
	}
	return int(o.backlog.Load())
}

// MayReadRequests reports whether the connection's own requests may be read
// and run: whether at most outputHighWater bytes are left to write before its
// latest Reply is out. What was pushed after that Reply does not count, so a
// Replica whose stream waits still has its acknowledgements read; the Push
// limit bounds that output instead. Both networking modes hold a connection's
// requests back by this one rule. The lock is taken only when the whole
// backlog is above the mark.
func (o *Output) MayReadRequests() bool {
	if o == nil || o.backlog.Load() <= outputHighWater {
		return true
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.replyBacklogLocked() <= outputHighWater
}

// Err reports why the connection was closed, or nil while it is open. A nil
// Output, which refuses every Push, is closed.
func (o *Output) Err() error {
	if o == nil {
		return errOutputClosed
	}

	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.closed {
		return nil
	}
	return o.errLocked()
}

// PeerClosed reports whether the peer has closed its side of the connection,
// without reading from it. It is false where that cannot be told: on the Event
// loop, on an unsupported platform, and for a nil Output.
func (o *Output) PeerClosed() bool {
	if o == nil {
		return false
	}
	return o.driver.peerClosed()
}

func (o *Output) isClosed() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.closed
}

// isReplica reports whether the connection is a Replica's (MarkReplica).
func (o *Output) isReplica() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.replica
}

func (o *Output) errLocked() error {
	if o.cause != nil {
		return o.cause
	}
	return errOutputClosed
}

// pendingLocked is how many bytes can be written now: everything but what is
// held behind a Reply.
func (o *Output) pendingLocked() int {
	return o.aheadBytes + len(o.queued) + len(o.inflight) - o.sent
}

// queueEndLocked is where the queue ends: how many bytes have been queued for
// writing since the connection opened, the written ones and the ones that can
// be written now. A Pushed message that is held counts once a Reply has
// released it.
func (o *Output) queueEndLocked() int64 {
	return o.written + int64(o.pendingLocked())
}

// replyBacklogLocked is how many bytes are left to write before the
// connection's latest Reply is out: its unwritten Replies and what was pushed
// ahead of them. It is zero once that Reply is written, however much was
// pushed after it.
func (o *Output) replyBacklogLocked() int64 {
	return max(o.replyEnd-o.written, 0)
}

// growLocked adds delta, which may be negative, to the backlog and to the Push
// budget's total when the connection counts toward it.
func (o *Output) growLocked(delta int) {
	if delta == 0 {
		return
	}
	o.backlog.Add(int64(delta))
	if o.counted {
		o.shared.total.Add(int64(delta))
	}
}

// offeredLocked reports whether the socket is being offered a batch that it has
// not taken all of.
func (o *Output) offeredLocked() bool {
	return o.sent < len(o.inflight)
}

// countStallLocked starts the Replica stall rule's count for the batch on
// offer: the socket has the stall timeout from now to take some of it. A closed
// Output's socket is offered nothing, whatever its writer left unsent, so no
// count starts for it. Its timer would hold the Output, and that batch, for
// the stall timeout (armStallLocked): the attach cut of a PSYNC marks a
// connection a Replica's even when it has closed meanwhile.
func (o *Output) countStallLocked() {
	if o.closed {
		return
	}
	o.lastProgress = time.Now()
	o.armStallLocked(o.shared.stallTimeout)
}

// armStallLocked has checkStall run after wait, unless it is due to run
// already.
func (o *Output) armStallLocked(wait time.Duration) {
	if o.stall == stallCounting {
		return
	}
	o.stall = stallCounting
	if o.stallWatch == nil {
		o.stallWatch = new(atomic.Pointer[Output])
		o.stallWatch.Store(o)
	}
	// The timer holds the Output through stallWatch, which Close empties. A
	// timer that held the Output itself would keep a closed connection's
	// Output reachable until it was due, and with it the batch a writer was
	// offering: for a Replica that left during its full resync, the snapshot.
	// Stopping it would not do: on Go 1.21 a stopped timer can stay in the
	// runtime's heap until its time.
	watch := o.stallWatch
	time.AfterFunc(wait, func() {
		if watched := watch.Load(); watched != nil {
			watched.checkStall()
		}
	})
}

// checkStall is the stall rule's timer. When a Replica's socket has taken
// nothing for the stall timeout of the batch it is offered, it closes the
// Replica, or leaves that to the writer where the writer has to try the socket
// first. Otherwise it looks again when the timeout could next have passed.
// With no batch on offer it stops looking: the next batch starts the count
// again (write).
//
// Only the writer can say whether the socket takes anything. In the goroutine
// mode it has been inside a blocking write since lastProgress, so the socket
// took nothing in that time. The Event loop tried the socket when the batch
// went on offer and then went on to other work, which can be one request that
// runs longer than the timeout. The socket may have had room for most of that
// time, so the loop is asked to try it now, and closes the Replica if that try
// takes nothing (write).
func (o *Output) checkStall() {
	o.mu.Lock()
	o.stall = stallIdle
	if o.closed || !o.offeredLocked() {
		o.mu.Unlock()
		return
	}
	if idle := time.Since(o.lastProgress); idle < o.shared.stallTimeout {
		o.armStallLocked(o.shared.stallTimeout - idle)
		o.mu.Unlock()
		return
	}
	if o.driver.retry(o) {
		o.stall = stallDue
		o.mu.Unlock()
		return
	}
	o.mu.Unlock()

	o.Close(o.stalledErr())
}

func (o *Output) stalledErr() error {
	return fmt.Errorf("%w of %v", errReplicaStalled, o.shared.stallTimeout)
}

// writeTo writes what is queued to w until nothing is left or w fails, and
// returns w's error. The caller is the only writer: the goroutine that set
// writing, or the Event loop. A w that accepts part of a chunk, and reports that
// it would block as the Event loop's does or reports nothing (io.ErrShortWrite),
// leaves the rest for the next call. When the stall rule asked for this try
// (checkStall) and w takes nothing, the error is errReplicaStalled, for the
// caller to close the connection with.
func (o *Output) writeTo(w io.Writer) error {
	return o.write(w, everyBuffer)
}

// everyBuffer, as the number of buffers write is to write, has it go on until
// nothing is queued.
const everyBuffer = -1

// errMoreQueued is what write returns, still holding the socket (writing stays
// set), when it has written the buffers it was asked for and more is queued.
var errMoreQueued = errors.New("server: more output was queued during the write")

// write is writeTo for a writer that takes no more than buffers buffers of what
// is queued: the batch that sealBatchLocked counted for it when it took the
// socket, or everyBuffer. If more is queued once those are written, it returns
// errMoreQueued and leaves writing set, for the caller to hand the socket on:
// what was queued meanwhile was pushed by other connections.
func (o *Output) write(w io.Writer, buffers int) error {
	for {
		o.mu.Lock()
		if o.closed {
			err := o.errLocked()
			o.stopWritingLocked()
			o.mu.Unlock()
			return err
		}
		if o.sent == len(o.inflight) {
			switch {
			case len(o.ahead) == 0 && len(o.queued) == 0:
				end := o.ending
				o.stopWritingLocked()
				o.mu.Unlock()
				if end {
					o.driver.closeWrite()
				}
				return nil
			case buffers == 0:
				o.mu.Unlock()
				return errMoreQueued
			case len(o.ahead) > 0:
				o.inflight, o.sent = o.ahead[0], 0
				o.aheadBytes -= len(o.inflight)
				// Moved down, not resliced, so that the list keeps its array for
				// the next large Reply.
				rest := copy(o.ahead, o.ahead[1:])
				o.ahead[rest] = nil
				o.ahead = o.ahead[:rest]
			default:
				o.inflight, o.queued, o.spare, o.sent = o.queued, o.spare[:0], nil, 0
			}
			if buffers > 0 {
				buffers--
			}
			if o.replica {
				o.countStallLocked()
			}
		}
		chunk := o.inflight[o.sent:]
		if len(chunk) > outputWriteChunk {
			chunk = chunk[:outputWriteChunk]
		}
		o.mu.Unlock()

		n, err := w.Write(chunk)

		o.mu.Lock()
		o.sent += n
		if o.sent == len(o.inflight) {
			// The batch is written. Its buffer is kept for the next one unless a
			// large Reply grew it: the connection may stay idle. Nothing queued
			// meanwhile means the next batch fills this same buffer, the one in
			// the processor's cache, and the other stays spare; a client that
			// reads each pipeline's replies before it sends the next then uses
			// one buffer, not two in turn.
			written := o.inflight[:0]
			if cap(written) > outputRetainedBuffer {
				written = nil
			}
			o.inflight, o.sent = nil, 0
			if len(o.queued) == 0 && !o.closed {
				o.queued, o.spare = written, o.queued
			} else {
				o.spare = written
			}
		}
		if n > 0 && !o.closed {
			if o.replica {
				o.lastProgress = time.Now()
			}
			o.growLocked(-n)
			o.written += int64(n)
			if o.replyBacklogLocked() <= outputHighWater {
				o.drained.Broadcast()
			}
		}
		if o.stall == stallDue && !o.closed {
			// This was the try the stall rule asked for. A socket that took
			// some of the batch has the stall timeout again for the rest.
			o.stall = stallIdle
			switch {
			case n == 0:
				err = o.stalledErr()
			case o.offeredLocked():
				o.countStallLocked()
			}
		}
		if err == nil && n < len(chunk) {
			err = io.ErrShortWrite
		}
		if err != nil {
			o.stopWritingLocked()
		}
		o.mu.Unlock()
		if err != nil {
			return err
		}
	}
}

func (o *Output) stopWritingLocked() {
	o.writing = false
	o.drained.Broadcast()
}

// goroutineOutputDriver writes an Output with blocking writes. The
// connection's own goroutine writes its Replies (Flush); a pump goroutine
// writes whatever is queued when a Pushed message arrives and nobody is
// writing, and exits when nothing is left.
type goroutineOutputDriver struct {
	wire     io.Writer
	shutOnce sync.Once
	// pump is what the pump goroutine runs, built on its first start so that
	// starting it again allocates nothing. Guarded by Output.mu.
	pump func()
}

func (d *goroutineOutputDriver) schedule(o *Output) {
	if o.writing {
		return
	}
	o.writing = true
	d.startPump(o)
}

// startPump starts the pump goroutine. The caller holds Output.mu and has set
// writing.
func (d *goroutineOutputDriver) startPump(o *Output) {
	if d.pump == nil {
		// A failed write closes the connection (write), which is all there is
		// to do about it here.
		d.pump = func() { _ = d.write(o, everyBuffer) }
	}
	go d.pump()
}

func (d *goroutineOutputDriver) flush(o *Output, mark int64) error {
	o.mu.Lock()
	// Another goroutine may have the socket, and then writes the Replies queued
	// here with everything else. This one waits for it while more than mark is
	// left to write before its latest Reply is out, and no longer. For Flush
	// that is only backpressure on the connection's own requests. What is
	// pushed after that Reply never makes it wait: its goroutine reads the
	// peer's requests, which for a Replica that has fallen behind are the
	// acknowledgements that show how far.
	for o.writing && !o.closed && o.replyBacklogLocked() > mark {
		o.drained.Wait()
	}
	if o.closed {
		err := o.errLocked()
		o.mu.Unlock()
		return err
	}
	if o.writing || o.pendingLocked() == 0 {
		o.mu.Unlock()
		return nil
	}
	o.writing = true
	// This goroutine writes what is queued now, which is its Replies: a push
	// would have started the pump. What other connections push while it writes
	// is the pump's to write. This goroutine reads the peer's requests, which
	// for a Replica are its acknowledgements, and it would not get back to them
	// under a steady stream of pushes, or behind the stream that was pushed
	// while a full resync reply went out.
	batch := o.sealBatchLocked()
	o.mu.Unlock()

	return d.write(o, batch)
}

func (d *goroutineOutputDriver) write(o *Output, buffers int) error {
	err := o.write(d.wire, buffers)
	if errors.Is(err, errMoreQueued) {
		o.mu.Lock()
		if o.closed {
			o.stopWritingLocked()
		} else {
			d.startPump(o)
		}
		o.mu.Unlock()
		return nil
	}
	if err != nil {
		o.Close(err)
	}
	return err
}

func (d *goroutineOutputDriver) shut() {
	d.shutOnce.Do(func() {
		if closer, ok := d.wire.(io.Closer); ok {
			_ = closer.Close()
		}
	})
}

// writeCloser is a socket whose write side can be shut on its own, as a TCP
// connection's can.
type writeCloser interface {
	CloseWrite() error
}

func (d *goroutineOutputDriver) closeWrite() {
	if wire, ok := d.wire.(writeCloser); ok {
		// One that fails is closed with every other connection afterwards.
		_ = wire.CloseWrite()
	}
}

func (d *goroutineOutputDriver) peerClosed() bool {
	conn, ok := d.wire.(net.Conn)
	return ok && peerClosed(conn)
}

func (d *goroutineOutputDriver) retry(*Output) bool { return false }

// unscheduledOutputDriver is the driver of an Output that only its owner
// writes, by calling writeTo: a ConnMachine built without an Event loop. It has
// no writer to ask for anything, so the stall rule's timer closes its Output
// itself.
type unscheduledOutputDriver struct{}

func (unscheduledOutputDriver) schedule(*Output)           {}
func (unscheduledOutputDriver) flush(*Output, int64) error { return nil }
func (unscheduledOutputDriver) shut()                      {}
func (unscheduledOutputDriver) closeWrite()                {}
func (unscheduledOutputDriver) peerClosed() bool           { return false }
func (unscheduledOutputDriver) retry(*Output) bool         { return false }

var discardLogger = slog.New(slog.NewTextHandler(io.Discard, nil))
