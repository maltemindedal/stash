// Package output is everything one connection is sent: its Replies and the
// Pushed messages it receives, in the order they were produced, in both
// networking modes. The server builds an Output for each connection and
// everything the connection is sent goes through it.
//
// The limits on what may wait for a connection are decided here. A caller
// asks an Output a question by name (MayReadRequests, Service's Status) and
// has no threshold of its own to compare a byte count with.
package output

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
	"github.com/maltemindedal/stash/internal/server/output/internal/ledger"
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
// latest Reply is out (ledger.Ledger.Gate). Above it the connection's own
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

// ErrPushLimitExceeded reports that more Client output is waiting for a
// connection that receives Pushed messages, or a Replica's stream, than it may
// hold.
var ErrPushLimitExceeded = errors.New("output: more output waiting than the connection's push limit")

// errPushBudgetExceeded reports that a connection was closed to bring the
// output waiting for all push-receiving connections back within the budget.
var errPushBudgetExceeded = errors.New("output: closed to stay within the budget for pushed output")

// ErrReplicaStalled reports that a Replica's socket took nothing for
// outputReplicaStallTimeout of the output it was offered.
var ErrReplicaStalled = errors.New("output: replica accepted no output within the stall timeout")

// errOutputClosed is what a closed Output reports when it was closed without a
// cause, as an ordinary teardown closes it.
var errOutputClosed = fmt.Errorf("output: client output closed: %w", net.ErrClosed)

// Shared holds what every connection's Output of one server shares: the
// limits, and the Push budget's running total. Its lock comes after any
// Output's.
type Shared struct {
	// limits are the thresholds of every connection's Ledger, which alone
	// compares a byte count with one.
	limits       ledger.Limits
	pushBudget   int64
	stallTimeout time.Duration

	// total is what every connection counts toward the Push budget together:
	// the sum of what each last reported (Output.accountLocked).
	total atomic.Int64

	mu sync.Mutex
	// pushers are the open connections that have received a Pushed message
	// as a subscriber: the ones the budget may close. One that became a
	// Replica's since stays listed and counts nothing.
	pushers map[*Output]struct{}
}

// Limit replaces one of a server's limits when its Shared is built. Limits go
// in here and cannot be read back.
type Limit func(*Shared)

// ReplicaPushLimit bounds what waits for a Replica behind its latest Reply.
func ReplicaPushLimit(bytes int64) Limit {
	return func(s *Shared) { s.limits.ReplicaPushLimit = bytes }
}

// StallTimeout is how long a Replica's socket may take nothing of what it
// is offered.
func StallTimeout(timeout time.Duration) Limit {
	return func(s *Shared) { s.stallTimeout = timeout }
}

// NewShared returns what the Outputs of one server share, with the server's
// limits. A test that cannot wait for those, or queue as much, replaces them.
func NewShared(limits ...Limit) *Shared {
	shared := &Shared{
		limits: ledger.Limits{
			HighWater:        outputHighWater,
			WriteThrough:     outputWriteThrough,
			Chunk:            outputWriteChunk,
			PushLimit:        outputPushLimit,
			ReplicaPushLimit: outputReplicaPushLimit,
		},
		pushBudget:   outputPushBudget,
		stallTimeout: outputReplicaStallTimeout,
		pushers:      make(map[*Output]struct{}),
	}
	for _, limit := range limits {
		limit(shared)
	}
	return shared
}

// enforceBudget closes push-receiving connections, the one with the most
// waiting first, until their total is back within the budget. It holds no
// Output's lock: it reads what each one last reported.
func (s *Shared) enforceBudget() {
	for s.total.Load() > s.pushBudget {
		var victim *Output
		var most int64
		s.mu.Lock()
		for o := range s.pushers {
			if waiting := o.budgeted.Load(); waiting > most {
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

// ErrWouldBlock is what the Event loop's socket reports when it has taken part
// of what it was given and can take no more now (Service).
var ErrWouldBlock = errors.New("output: socket write would block")

// stallPhase is where the Replica stall rule stands for an Output.
type stallPhase uint8

const (
	// stallIdle is the rule at rest: nothing is on offer to a Replica's socket.
	stallIdle stallPhase = iota
	// stallCounting has a timer due to look at lastProgress (checkStall).
	stallCounting
	// stallDue is the stall timeout passed with nothing taken, on a connection
	// whose Event loop was asked to try the socket once more (checkStall). What
	// that try takes is the verdict (write).
	stallDue
)

// Output is the Client output of one connection: its Replies and the Pushed
// messages it receives (pub/sub messages, MONITOR events, and for a Replica
// its stream), in the order they were produced. Safe for concurrent use.
//
// Who calls decides what may wait on the peer's socket. The connection's own
// executor may: Reply, Flush and Drain, in the goroutine mode. Push,
// HoldPushes, MarkReplica, EndStream, Close and Err are for anyone and never
// wait. Service and MayReadRequests are the Event loop's and never wait.
//
// Lock order: the lock of whoever pushes (a registry's, or the replication
// Master's for a Replica's stream), then Output.mu, then the shared budget's.
// Output.mu is never held across a socket write, and nothing called under it
// takes a pusher's lock.
type Output struct {
	mu sync.Mutex
	// ledger counts the connection's output and decides what depends on a
	// count: whether its requests may be read, whether its Replies are out,
	// whether a Push fits. bytes are the bytes themselves. The two change
	// together, under one hold of mu.
	ledger ledger.Ledger
	bytes  chain
	// writing is set while a goroutine of the goroutine mode owns the socket:
	// the connection's own, writing its Replies (flush), or the pump, which
	// runs only while Pushed messages wait and nobody is writing.
	writing bool
	cause   error
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
	// closed, and after every write that leaves the connection's requests
	// readable (ledger.Ledger.Gate), the one that writes the last byte of its
	// Replies included.
	drained *sync.Cond
	// budgeted is what the connection last reported to the Push budget: its
	// Ledger's Budgeted, as of the last event (accountLocked). The budget reads
	// it without the lock (Shared.enforceBudget).
	budgeted atomic.Int64

	// wire is the socket of a connection served by its own goroutine, written
	// with blocking writes. It is nil for a connection the Event loop serves:
	// the loop writes everything itself (Service), and attention, when there
	// is one, tells it that the connection needs a Service. attention is
	// called with mu held or not and must not block.
	wire      io.Writer
	attention func()
	shutOnce  sync.Once
	// pump is what the pump goroutine runs, built on its first start so that
	// starting it again allocates nothing. Guarded by mu.
	pump func()

	shared *Shared
	logger *slog.Logger
}

// NewBlocking returns the Client output of a connection served by its
// own goroutine, which writes its Replies to wire (Flush). A pump goroutine
// writes what other connections push. Closing the Output closes wire when it
// is an io.Closer.
func NewBlocking(shared *Shared, wire io.Writer, logger *slog.Logger) *Output {
	o := &Output{ledger: ledger.New(shared.limits), wire: wire, shared: shared, logger: logger}
	o.drained = sync.NewCond(&o.mu)
	return o
}

// NewPolled returns the Client output of a connection the Event loop
// serves. The loop writes it (Service). attention tells the loop to call
// Service for the connection: bytes were queued from elsewhere, the stream is
// to end, the stall rule wants the socket tried, or the connection was closed.
// With a nil attention the Output is written only when its owner chooses to,
// as a ConnMachine built without an Event loop is.
func NewPolled(shared *Shared, attention func(), logger *slog.Logger) *Output {
	o := &Output{ledger: ledger.New(shared.limits), attention: attention, shared: shared, logger: logger}
	o.drained = sync.NewCond(&o.mu)
	return o
}

// New returns the Client output of a connection served by its own
// goroutine, writing to w. Closing the Output closes w when it is an io.Closer.
// The server builds each connection's Output itself; this is for code that
// drives a Client state without a server.
func New(w io.Writer) *Output {
	return NewBlocking(NewShared(), w, discardLogger)
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
	if o.ledger.Closed() {
		err := o.errLocked()
		o.mu.Unlock()
		return err
	}
	added := len(whole)
	if whole != nil {
		o.bytes.whole(whole)
	} else {
		var err error
		if added, err = o.bytes.reply(values); err != nil {
			o.mu.Unlock()
			return err
		}
	}
	// The Pushed messages held since HoldPushes follow the Reply.
	released := len(o.bytes.held) > 0
	o.bytes.release()
	o.ledger.Reply(int64(added))
	o.accountLocked()
	if released {
		// The connection's goroutine must not be the one that writes a stream
		// of Pushed messages to a peer that may have stopped reading.
		o.scheduleLocked()
	}
	writeThrough := o.wire != nil && o.ledger.Full()
	o.mu.Unlock()

	if writeThrough {
		return o.flush(false)
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

// sealBatchLocked makes everything queued now the batch of a writer that
// writes nothing queued after it began (Flush), and returns how many buffers
// that is. Behind whole buffers, what is in queued becomes the last of them:
// left where it is, it would grow by what is pushed while the buffers ahead of
// it are written. With no whole buffer the batch is queued as it is, which the
// writer takes in one piece under its next hold of the lock: a Push that lands
// in that instant rides with the batch. It is one batch either way, which is
// what bounds the writer.
func (o *Output) sealBatchLocked() int {
	if len(o.bytes.ahead) == 0 {
		return 1
	}
	o.bytes.seal()
	return len(o.bytes.ahead)
}

// Push queues a frame another connection produced. It never waits. It returns
// false if the connection is closed, or if this Push closed it for passing its
// Push limit or the Push budget.
func (o *Output) Push(frame []byte) bool {
	if o == nil {
		return false
	}

	o.mu.Lock()
	// The Ledger holds a subscriber to its limit for everything waiting, and a
	// Replica for its stream alone (ledger.Ledger.Push).
	class := o.ledger.Class()
	accepted, effects := o.ledger.Push(int64(len(frame)))
	if !accepted {
		waiting := o.ledger.Waiting()
		o.mu.Unlock()
		if effects.Close != nil {
			o.Close(fmt.Errorf("%w: %d bytes were waiting and %d more were pushed", ErrPushLimitExceeded, waiting, len(frame)))
		}
		return false
	}

	if o.ledger.Class() != class {
		// Its first Pushed message made the connection a subscriber.
		o.shared.mu.Lock()
		o.shared.pushers[o] = struct{}{}
		o.shared.mu.Unlock()
	}
	if o.ledger.Holding() {
		o.bytes.hold(frame)
	} else {
		o.bytes.push(frame)
		o.scheduleLocked()
	}
	overBudget := o.accountLocked() > 0 && o.shared.total.Load() > o.shared.pushBudget
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
	o.ledger.Hold()
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
	return o.flush(false)
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
// closing connection itself (ConnMachine.Service), so there Drain does nothing.
func (o *Output) Drain() {
	// Why the connection is closed, if it is, changes nothing for a caller
	// that is about to close it.
	_ = o.flush(true)
}

// Close closes the connection from any goroutine without waiting for a write in
// progress, and drops what was waiting for it. In the goroutine mode it closes
// the socket. The Event loop owns its sockets, so there Close asks for the
// loop's attention, and the loop closes the connection when its next Service
// reports why (Status.Closed). It never touches registries; teardown abandons
// the Client state.
func (o *Output) Close(cause error) {
	if o == nil {
		return
	}

	o.mu.Lock()
	if o.ledger.Closed() {
		o.mu.Unlock()
		return
	}
	waiting := o.ledger.Waiting()
	o.ledger.Close()
	o.cause = cause
	o.accountLocked()
	// A writer in the middle of a write keeps the bytes it is writing until
	// the write returns, and no longer.
	o.bytes.clear()
	if o.stallWatch != nil {
		o.stallWatch.Store(nil)
	}
	if o.ledger.Class() != ledger.Plain {
		o.shared.mu.Lock()
		delete(o.shared.pushers, o)
		o.shared.mu.Unlock()
	}
	o.drained.Broadcast()
	o.mu.Unlock()

	if errors.Is(cause, ErrPushLimitExceeded) || errors.Is(cause, errPushBudgetExceeded) || errors.Is(cause, ErrReplicaStalled) {
		o.logger.Warn("closing a connection that is not keeping up with what is pushed to it", "pending_bytes", waiting, "error", cause)
	}
	if o.wire == nil {
		if o.attention != nil {
			o.attention()
		}
		return
	}
	o.shutOnce.Do(func() {
		if closer, ok := o.wire.(io.Closer); ok {
			_ = closer.Close()
		}
	})
}

// EndStream ends the stream to the peer once everything queued has been
// written: the write side is shut and the connection stays open for the peer to
// close. Shutdown ends a Replica's stream this way
// (replication.Master.EndStreams). The Event loop shuts the write side itself,
// when a Service reports the end of the stream (Status.EndOfStream).
func (o *Output) EndStream() {
	if o == nil {
		return
	}

	o.mu.Lock()
	if o.ledger.Closed() || o.ending {
		o.mu.Unlock()
		return
	}
	o.ending = true
	// A writer of the goroutine mode shuts the write side when it has written
	// everything (write). With none at work and nothing to write, nobody would.
	shut := o.wire != nil && !o.writing && !o.ledger.Writable()
	if !shut {
		o.scheduleLocked()
	}
	o.mu.Unlock()

	if shut {
		o.closeWrite()
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

	o.ledger.MarkReplica(unclocked)
	o.accountLocked()
	if o.offeredLocked() {
		o.countStallLocked()
	}
}

// Backlog reports how many bytes of Client output wait for the connection.
func (o *Output) Backlog() int {
	if o == nil {
		return 0
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	return int(o.ledger.Waiting())
}

// MayReadRequests reports whether the connection's own requests may be read
// and run: whether at most outputHighWater bytes are left to write before its
// latest Reply is out. What was pushed after that Reply does not count, so a
// Replica whose stream waits still has its acknowledgements read; the Push
// limit bounds that output instead. Both networking modes hold a connection's
// requests back by this one rule, the Ledger's Gate.
func (o *Output) MayReadRequests() bool {
	if o == nil {
		return true
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.ledger.Gate()
}

// Err reports why the connection was closed, or nil while it is open. A nil
// Output, which refuses every Push, is closed.
func (o *Output) Err() error {
	if o == nil {
		return errOutputClosed
	}

	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.ledger.Closed() {
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
	conn, ok := o.wire.(net.Conn)
	return ok && peerClosed(conn)
}

func (o *Output) isClosed() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.ledger.Closed()
}

// IsReplica reports whether the connection is a Replica's (MarkReplica).
func (o *Output) IsReplica() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.isReplicaLocked()
}

func (o *Output) isReplicaLocked() bool {
	return o.ledger.Class() == ledger.Replica
}

func (o *Output) errLocked() error {
	if o.cause != nil {
		return o.cause
	}
	return errOutputClosed
}

// unclocked is the time the Ledger is told while the stall rule is still this
// file's timer (checkStall): none passes, so the Ledger gives no verdict.
const unclocked ledger.Instant = 1

// accountLocked reports what the connection counts toward the Push budget
// now, the Ledger's Budgeted, and returns it. It follows every event that
// changes what waits for the connection or what kind of connection it is, so
// the budget's total is derived from the Ledgers and cannot drift from them.
func (o *Output) accountLocked() int64 {
	budgeted := o.ledger.Budgeted()
	if delta := budgeted - o.budgeted.Load(); delta != 0 {
		o.budgeted.Store(budgeted)
		o.shared.total.Add(delta)
	}
	return budgeted
}

// offeredLocked reports whether the socket is being offered a batch that it has
// not taken all of.
func (o *Output) offeredLocked() bool {
	return o.bytes.offered()
}

// countStallLocked starts the Replica stall rule's count for the batch on
// offer: the socket has the stall timeout from now to take some of it. A closed
// Output's socket is offered nothing, whatever its writer left unsent, so no
// count starts for it. Its timer would hold the Output, and that batch, for
// the stall timeout (armStallLocked): the attach cut of a PSYNC marks a
// connection a Replica's even when it has closed meanwhile.
func (o *Output) countStallLocked() {
	if o.ledger.Closed() {
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
// time, so the loop is asked to try it now (attention), and its Service closes
// the Replica if that try takes nothing (write). An Output with nobody to ask
// is closed here.
func (o *Output) checkStall() {
	o.mu.Lock()
	o.stall = stallIdle
	if o.ledger.Closed() || !o.offeredLocked() {
		o.mu.Unlock()
		return
	}
	if idle := time.Since(o.lastProgress); idle < o.shared.stallTimeout {
		o.armStallLocked(o.shared.stallTimeout - idle)
		o.mu.Unlock()
		return
	}
	if o.wire == nil && o.attention != nil {
		o.stall = stallDue
		o.attention()
		o.mu.Unlock()
		return
	}
	o.mu.Unlock()

	o.Close(o.stalledErr())
}

func (o *Output) stalledErr() error {
	return fmt.Errorf("%w of %v", ErrReplicaStalled, o.shared.stallTimeout)
}

// Status is where a connection the Event loop serves stands after a Service.
// The loop decides from it and compares no byte counts of its own.
type Status struct {
	// Closed is why the Output is closed, or nil while it is open. The loop
	// closes the connection.
	Closed error
	// MayRead is MayReadRequests: read and run the connection's requests.
	MayRead bool
	// WantWrite reports that output waits which the socket did not take: ask
	// the poller for writable. Pushed messages held behind a Reply that has
	// not come are not waiting for the socket.
	WantWrite bool
	// EndOfStream reports that EndStream was called and everything is
	// written: shut the write side.
	EndOfStream bool
}

func (o *Output) statusLocked() Status {
	if o.ledger.Closed() {
		return Status{Closed: o.errLocked()}
	}
	writable := o.ledger.Writable()
	return Status{
		MayRead:     o.ledger.Gate(),
		WantWrite:   writable,
		EndOfStream: o.ending && !writable,
	}
}

// Service writes what is queued to the socket until nothing is left or the
// socket takes no more, and reports where the connection stands. Only the Event
// loop calls it, with the connection's socket. A socket that accepts part of a
// chunk, and reports that it would block (ErrWouldBlock) or reports nothing
// (io.ErrShortWrite), leaves the rest for the next Service. Any other failure
// closes the Output, as does a try the stall rule asked for (checkStall) that
// takes nothing.
func (o *Output) Service(socket io.Writer) Status {
	status, err := o.write(socket, everyBuffer)
	if status.Closed != nil || err == nil || errors.Is(err, ErrWouldBlock) || errors.Is(err, io.ErrShortWrite) {
		return status
	}
	o.Close(err)
	return Status{Closed: o.Err()}
}

// everyBuffer, as the number of buffers write is to write, has it go on until
// nothing is queued.
const everyBuffer = -1

// errMoreQueued is what write returns, still holding the socket (writing stays
// set), when it has written the buffers it was asked for and more is queued.
var errMoreQueued = errors.New("output: more output was queued during the write")

// write writes what is queued to w until nothing is left or w fails, and
// returns w's error and, for Service, where the Output stood when it stopped.
// The caller is the only writer: the goroutine that set writing, or the Event
// loop. It takes no more than buffers buffers of what is queued: the batch that
// sealBatchLocked counted for a writer when it took the socket, or everyBuffer.
// If more is queued once those are written, it returns errMoreQueued and
// leaves writing set, for the caller to hand the socket on: what was queued
// meanwhile was pushed by other connections. When the stall rule asked for
// this try (checkStall) and w takes nothing, the error is ErrReplicaStalled,
// for the caller to close the connection with.
func (o *Output) write(w io.Writer, buffers int) (Status, error) {
	for {
		o.mu.Lock()
		if o.ledger.Closed() {
			status := o.statusLocked()
			o.stopWritingLocked()
			o.mu.Unlock()
			return status, status.Closed
		}
		if !o.bytes.offered() {
			switch {
			case !o.ledger.Writable():
				status := o.statusLocked()
				o.stopWritingLocked()
				o.mu.Unlock()
				if status.EndOfStream && o.wire != nil {
					o.closeWrite()
				}
				return status, nil
			case buffers == 0:
				o.mu.Unlock()
				return Status{}, errMoreQueued
			}
			o.bytes.offer()
			if buffers > 0 {
				buffers--
			}
			if o.isReplicaLocked() {
				o.countStallLocked()
			}
		}
		chunk := o.bytes.inflight[o.bytes.sent:]
		if len(chunk) > outputWriteChunk {
			chunk = chunk[:outputWriteChunk]
		}
		o.mu.Unlock()

		n, err := w.Write(chunk)

		o.mu.Lock()
		if o.ledger.Closed() {
			// Close dropped what was being written with everything else.
			status := o.statusLocked()
			o.stopWritingLocked()
			o.mu.Unlock()
			return status, status.Closed
		}
		o.bytes.drop(n)
		o.ledger.Took(int64(n), unclocked)
		if n > 0 {
			if o.isReplicaLocked() {
				o.lastProgress = time.Now()
			}
			o.accountLocked()
			if o.ledger.Gate() {
				o.drained.Broadcast()
			}
		}
		if o.stall == stallDue {
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
			status := o.statusLocked()
			o.stopWritingLocked()
			o.mu.Unlock()
			return status, err
		}
		o.mu.Unlock()
	}
}

func (o *Output) stopWritingLocked() {
	o.writing = false
	o.drained.Broadcast()
}

// scheduleLocked is called when bytes were queued that the connection's own
// executor will not flush, a Pushed message or the ones a Reply released, and
// when the stream is to end. In the goroutine mode it starts the pump unless a
// goroutine has the socket already. The Event loop is asked for its attention.
func (o *Output) scheduleLocked() {
	if o.wire == nil {
		if o.attention != nil {
			o.attention()
		}
		return
	}
	if o.writing {
		return
	}
	o.writing = true
	o.startPumpLocked()
}

// startPumpLocked starts the pump goroutine, which writes whatever is queued
// and exits when nothing is left. The caller has set writing.
func (o *Output) startPumpLocked() {
	if o.pump == nil {
		// A failed write closes the connection (writeOut), which is all there
		// is to do about it here.
		o.pump = func() { _ = o.writeOut(everyBuffer) }
	}
	go o.pump()
}

// flush is Flush, which waits for another writer while the connection's
// requests may not be read (ledger.Ledger.Gate), or Drain, which waits for it
// until the Replies are out (ledger.Ledger.RepliesOut). The Event loop writes
// its connections itself, so there it does nothing.
func (o *Output) flush(drain bool) error {
	if o.wire == nil {
		return nil
	}

	o.mu.Lock()
	// Another goroutine may have the socket, and then writes the Replies queued
	// here with everything else. This one waits for it until the Ledger says
	// it may go on, and no longer. For Flush that is only backpressure on the
	// connection's own requests. What is pushed after its latest Reply never
	// makes it wait: its goroutine reads the peer's requests, which for a
	// Replica that has fallen behind are the acknowledgements that show how
	// far.
	for o.writing && !o.ledger.Closed() && !o.mayGoOnLocked(drain) {
		o.drained.Wait()
	}
	if o.ledger.Closed() {
		err := o.errLocked()
		o.mu.Unlock()
		return err
	}
	if o.writing || !o.ledger.Writable() {
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

	return o.writeOut(batch)
}

// mayGoOnLocked reports whether the connection's goroutine need not wait for
// another writer any longer: for a Drain once its Replies are out, otherwise
// once its requests may be read.
func (o *Output) mayGoOnLocked(drain bool) bool {
	if drain {
		return o.ledger.RepliesOut()
	}
	return o.ledger.Gate()
}

// writeOut is write to the socket of the goroutine mode, by the goroutine that
// set writing. It hands the socket to the pump when more was queued than it
// was to write, and closes the connection when the write fails.
func (o *Output) writeOut(buffers int) error {
	_, err := o.write(o.wire, buffers)
	if errors.Is(err, errMoreQueued) {
		o.mu.Lock()
		if o.ledger.Closed() {
			o.stopWritingLocked()
		} else {
			o.startPumpLocked()
		}
		o.mu.Unlock()
		return nil
	}
	if err != nil {
		o.Close(err)
	}
	return err
}

// writeCloser is a socket whose write side can be shut on its own, as a TCP
// connection's can.
type writeCloser interface {
	CloseWrite() error
}

// closeWrite ends the stream to the peer of the goroutine mode, after
// everything queued was written (EndStream).
func (o *Output) closeWrite() {
	if wire, ok := o.wire.(writeCloser); ok {
		// One that fails is closed with every other connection afterwards.
		_ = wire.CloseWrite()
	}
}

var discardLogger = slog.New(slog.NewTextHandler(io.Discard, nil))
