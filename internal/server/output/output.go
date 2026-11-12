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
	"os"
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
// the output it is offered before the Replica is dropped. Output that no
// writer has offered to the socket does not count, since the Replica is not
// failing to take it: Replies that are not flushed yet, and Pushed messages
// held behind a Reply that has not come. Only a try of the socket can say that
// it takes nothing (ledger.Ledger.Took): a blocking write that the timeout
// ends, or the Event loop's try once the timeout has passed, however long the
// loop was away from the socket.
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

// outputReplyLimit bounds what may be left to write before the latest Reply of
// a connection the Event loop serves is out. A Reply that leaves more closes
// the connection: the loop cannot wait on a socket, so it has to hold a Reply
// until the peer reads it, and a client that asks for more than this without
// reading is disconnected instead of growing the server's memory. What is
// pushed behind the Reply does not count, since the Push limits bound that.
// A connection served by its own goroutine has no such limit: its Reply
// waits on the socket (outputWriteThrough).
//
// The limit holds the largest value a reply carries, a bulk string of
// protocol.BulkStringLimit, behind the outputHighWater that may wait when its
// request is run, with as much again for its framing. A master serves a
// snapshot of up to that bulk string limit as one such reply, so no snapshot
// it accepts is closed here.
const outputReplyLimit = protocol.BulkStringLimit + 2*outputHighWater

// outputWriteChunk is the most bytes handed to a socket in one write, so that
// the Output's lock is taken, and a Replica's stall count renewed, at least
// that often.
const outputWriteChunk = 1 << 20

// ErrPushLimitExceeded reports that more Client output is waiting for a
// connection that receives Pushed messages, or a Replica's stream, than it may
// hold.
var ErrPushLimitExceeded = errors.New("output: more output waiting than the connection's push limit")

// errPushBudgetExceeded reports that a connection was closed to bring the
// output waiting for all push-receiving connections back within the budget.
var errPushBudgetExceeded = errors.New("output: closed to stay within the budget for pushed output")

// ErrReplicaStalled reports that a Replica's socket took nothing of the output
// it was offered for outputReplicaStallTimeout.
var ErrReplicaStalled = errors.New("output: replica accepted no output within the stall timeout")

// errReplyLimitExceeded reports that a Reply on the Event loop left more to
// write before it is out than outputReplyLimit.
var errReplyLimitExceeded = errors.New("output: a reply left more to write than the connection's write-buffer limit")

// errLastRepliesNotTaken reports that the peer of a connection that had run
// its last request took none of its last Replies for
// outputReplicaStallTimeout.
var errLastRepliesNotTaken = errors.New("output: peer took none of its last replies within the stall timeout")

// errOutputClosed is what a closed Output reports when it was closed without a
// cause, as an ordinary teardown closes it.
var errOutputClosed = fmt.Errorf("output: client output closed: %w", net.ErrClosed)

// Shared holds what every connection's Output of one server shares: the
// limits, and the Push budget's running total. Its lock comes after any
// Output's.
type Shared struct {
	// limits are the thresholds of every connection's Ledger, which alone
	// compares a byte count with one.
	limits     ledger.Limits
	pushBudget int64
	// epoch is when the Shared was built. The Ledgers are told the time as
	// nanoseconds since then (now).
	epoch time.Time

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

// ReplyLimit bounds what may be left to write before the latest Reply of a
// connection the Event loop serves is out.
func ReplyLimit(bytes int64) Limit {
	return func(s *Shared) { s.limits.ReplyLimit = bytes }
}

// StallTimeout is how long the socket of a Replica, or of a connection that
// has run its last request, may take nothing of what it is offered.
func StallTimeout(timeout time.Duration) Limit {
	return func(s *Shared) { s.limits.StallTimeout = ledger.Instant(timeout) }
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
			StallTimeout:     ledger.Instant(outputReplicaStallTimeout),
			ReplyLimit:       outputReplyLimit,
		},
		pushBudget: outputPushBudget,
		epoch:      time.Now(),
		pushers:    make(map[*Output]struct{}),
	}
	for _, limit := range limits {
		limit(shared)
	}
	return shared
}

// now is the time a Ledger is told: on the monotonic clock, in nanoseconds
// since the Shared was built, and above zero, which a Ledger reads as none.
func (s *Shared) now() ledger.Instant {
	return ledger.Instant(time.Since(s.epoch)) + 1
}

// timeOf is the time of an Instant a Ledger answered with.
func (s *Shared) timeOf(at ledger.Instant) time.Time {
	return s.epoch.Add(time.Duration(at - 1))
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

// Output is the Client output of one connection: its Replies and the Pushed
// messages it receives (pub/sub messages, MONITOR events, and for a Replica
// its stream), in the order they were produced. Safe for concurrent use.
//
// It has three parts. The Ledger (package ledger) decides everything that
// depends on a count: it is told each event and answers with a decision and
// the Effects to carry out. The chain holds the bytes. The Output holds the
// lock, tells the Ledger, and carries out its Effects in one function
// (unlock). A method here that compares byte counts is in the wrong file.
//
// Who calls decides what may wait on the peer's socket. The connection's own
// executor may: Reply, Flush and Finish, in the goroutine mode. Push,
// HoldPushes, MarkReplica, EndStream, Close and Err are for anyone and never
// wait. Service and MayReadRequests are the Event loop's and never wait.
//
// Lock order: the lock of whoever pushes (a registry's, or the replication
// Master's for a Replica's stream), then Output.mu, then the shared budget's.
// Output.mu is never held across a socket write, and nothing called under it
// takes a pusher's lock.
type Output struct {
	mu sync.Mutex
	// ledger counts the connection's output and decides, and bytes are the
	// bytes themselves. The two change together, under one hold of mu.
	ledger ledger.Ledger
	bytes  chain
	cause  error

	// waiters wait for the Ledger to open the gate or to report the Replies
	// out (Flush, Finish), and are woken when it says so (Effects.Signal).
	waiters *sync.Cond
	// budgeted is what the connection last reported to the Push budget, and
	// gateShut whether its requests may not be read: the Ledger's Budgeted and
	// Gate as of the last event (unlock). The budget and the Event loop read
	// them without the lock.
	budgeted atomic.Int64
	gateShut atomic.Bool

	// wire is the socket of a connection served by its own goroutine, written
	// with blocking writes. It is nil for a connection the Event loop serves:
	// the loop writes everything itself (Service), and attention, when there
	// is one, tells it that the connection needs a Service. attention is
	// called with mu held or not and must not block.
	wire      io.Writer
	attention func()
	shutOnce  sync.Once
	// deadlines is wire, when its writes can be given a deadline, as a
	// net.Conn's can. The stall rule ends a blocking write through it
	// (Effects.Deadline). A wire without deadlines is held to the rule only
	// when a write to it returns.
	deadlines writeDeadliner
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
	limits := shared.limits
	// Its Reply waits on the socket, so no Reply is too much to hold.
	limits.ReplyLimit = 0
	o := &Output{ledger: ledger.New(limits), wire: wire, shared: shared, logger: logger}
	o.waiters = sync.NewCond(&o.mu)
	o.deadlines, _ = wire.(writeDeadliner)
	return o
}

// NewPolled returns the Client output of a connection the Event loop
// serves. The loop writes it (Service). attention tells the loop to call
// Service for the connection: bytes were queued from elsewhere, the stream
// has ended, or the connection was closed. The loop also calls Service at the
// time a Service asked for (Status.TryAgainAt). With a nil attention the Output is written only when its owner
// chooses to, as a ConnMachine built without an Event loop is.
func NewPolled(shared *Shared, attention func(), logger *slog.Logger) *Output {
	o := &Output{ledger: ledger.New(shared.limits), attention: attention, shared: shared, logger: logger}
	o.waiters = sync.NewCond(&o.mu)
	return o
}

// New returns the Client output of a connection served by its own
// goroutine, writing to w. Closing the Output closes w when it is an io.Closer.
// The server builds each connection's Output itself; this is for code that
// drives a Client state without a server.
func New(w io.Writer) *Output {
	return NewBlocking(NewShared(), w, discardLogger)
}

// unlock carries out what the Ledger answered to an event and releases mu.
// Every event ends here, so that none can skip an effect: the wakeup, the
// writer to start, the end of the stream, the deadline of a blocking write, the
// close, and the reports of what the connection counts toward the Push budget
// and of its gate. The Event loop has no write to bound: it is told a deadline
// as the time to call Service again (Status.TryAgainAt).
func (o *Output) unlock(effects ledger.Effects) {
	if effects.Signal {
		o.waiters.Broadcast()
	}
	if effects.StartPusher {
		o.startPusherLocked()
	}
	if effects.ShutWrite && o.wire == nil && o.attention != nil {
		// The loop shuts the write side, when its Service reports the end.
		o.attention()
	}
	if budgeted, reported := o.ledger.Budgeted(), o.budgeted.Load(); budgeted != reported {
		o.shared.total.Add(budgeted - reported)
		o.budgeted.Store(budgeted)
	}
	if shut := !o.ledger.Gate(); shut != o.gateShut.Load() {
		o.gateShut.Store(shut)
	}
	if effects.Deadline != 0 && o.deadlines != nil {
		// It also ends a write that is in progress. A deadline that is not
		// replaced stays on the socket: the Ledger gives one with every write
		// of a connection it has given one before.
		_ = o.deadlines.SetWriteDeadline(o.shared.timeOf(effects.Deadline))
	}
	var cause error
	if effects.Close != nil {
		cause = o.causeLocked(effects.Close)
	}
	o.mu.Unlock()

	if effects.ShutWrite && o.wire != nil {
		o.closeWrite()
	}
	if cause != nil {
		o.Close(cause)
	}
}

// causeLocked is the Ledger's reason to close the connection as this package
// reports it.
func (o *Output) causeLocked(reason error) error {
	if errors.Is(reason, ledger.ErrPushLimit) {
		return fmt.Errorf("%w: %d bytes were waiting", ErrPushLimitExceeded, o.ledger.Waiting())
	}
	if errors.Is(reason, ledger.ErrStalled) {
		stalled := errLastRepliesNotTaken
		if o.isReplicaLocked() {
			stalled = ErrReplicaStalled
		}
		return fmt.Errorf("%w of %v", stalled, time.Duration(o.shared.limits.StallTimeout))
	}
	if errors.Is(reason, ledger.ErrReplyLimit) {
		return fmt.Errorf("%w of %d bytes", errReplyLimitExceeded, o.shared.limits.ReplyLimit)
	}
	return reason
}

// startPusherLocked gives the socket to the writer that does not read
// requests, as the Ledger asked: bytes were queued that the connection's own
// executor will not write. In the goroutine mode that is the pump goroutine,
// which writes whatever is queued and exits when nothing is left. The Event
// loop is asked for its attention.
func (o *Output) startPusherLocked() {
	if o.wire == nil {
		if o.attention != nil {
			o.attention()
		}
		return
	}
	if o.pump == nil {
		o.pump = func() {
			if _, err := o.writeAs(ledger.Pusher, o.wire); err != nil {
				o.Close(err)
			}
		}
	}
	go o.pump()
}

// Reply queues the Replies to the request the connection just ran (none is
// allowed) and releases the Pushed messages held since HoldPushes. Only the
// connection's own executor calls it. Replies leave together when a
// connection's goroutine next waits for input (Flush), unless outputWriteThrough
// is left to write before they are out: Reply then writes them out as Flush
// does, and waits as Flush does. On the Event loop it never waits, and a Reply
// that leaves more than outputReplyLimit to write before it is out closes the
// connection, which Reply reports. A Push to the connection takes the lock
// Reply holds, so a large Reply is encoded before that lock is taken and
// queued whole (isLargeReply).
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
	// The Pushed messages held since HoldPushes follow the Reply. The Ledger
	// starts the pump for them: the connection's goroutine must not be the one
	// that writes a stream of Pushed messages to a peer that may have stopped
	// reading.
	o.bytes.release()
	effects := o.ledger.Reply(int64(added))
	writeThrough := o.wire != nil && o.ledger.Full()
	o.unlock(effects)

	if effects.Close != nil {
		return o.Err()
	}
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
		o.unlock(effects)
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
	}
	o.unlock(effects)

	if o.budgeted.Load() > 0 && o.shared.total.Load() > o.shared.pushBudget {
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

// Flush writes out the connection's Replies, for the connection's own
// goroutine: before it waits for the next request and before a command blocks.
// It writes what was queued when it took the socket and nothing queued after:
// what other connections push meanwhile is the pump's to write. When another
// goroutine is already writing it leaves the writing to that one, and waits
// only while the connection's requests may not be read (ledger.Ledger.Gate): a
// client that does not read what it is sent stops having its requests run. It
// reports why the connection is closed, if it is. The Event loop writes its
// connections itself, so there Flush does nothing.
func (o *Output) Flush() error {
	if o == nil {
		return nil
	}
	return o.flush(false)
}

// Finish is the last call of a connection's own goroutine, when it has run its
// last request: it waits until every Reply queued so far has been written
// (ledger.Ledger.RepliesOut), and closes the connection, which drops what is
// still queued. It is the only way that goroutine closes its connection, so
// there is no wait and close to put in the wrong order. Like Flush it writes
// its Replies when nobody has the socket. When another goroutine has it,
// Finish waits until that one has written the latest Reply.
//
// It does not wait for what was pushed after that Reply, beyond the write that
// carries the Reply's last byte. The goroutine of a Replica that has been sent
// its full resync reply therefore leaves at once, however much of its stream
// waits.
//
// The wait is bounded. From Finish on the Ledger holds the socket to the
// stall rule (ledger.Ledger.Finish): a peer that takes none of its last
// Replies for outputReplicaStallTimeout is closed, where it used to keep the
// goroutine until the server shut down. One that keeps taking them, however
// slowly, is sent them all. The Event loop writes out a closing connection
// itself (ConnMachine.Service), so there Finish only closes.
func (o *Output) Finish() {
	if o == nil {
		return
	}
	o.drain()
	o.Close(nil)
}

// drain is Finish before it closes.
func (o *Output) drain() {
	if o.wire == nil {
		return
	}
	o.mu.Lock()
	o.unlock(o.ledger.Finish(o.shared.now()))
	// Why the connection is closed, if it is, changes nothing for a caller
	// that is about to close it.
	_ = o.flush(true)
}

// flush is Flush, which returns once the connection's requests may be read
// (ledger.Ledger.Gate), or the wait of Finish, which returns once its Replies
// are out (ledger.Ledger.RepliesOut). The Event loop writes its connections itself,
// so there it does nothing.
func (o *Output) flush(drain bool) error {
	if o.wire == nil {
		return nil
	}
	// The Ledger gives this goroutine the socket when nobody has it, for what
	// is queued now, which is its Replies: a push would have started the pump.
	status, err := o.writeAs(ledger.Requester, o.wire)
	switch {
	case err != nil:
		o.Close(err)
	case !drain && status.MayRead:
		// The gate was open when the write loop stopped: the usual case, and
		// the lock is not taken again for it.
		return nil
	}

	o.mu.Lock()
	defer o.mu.Unlock()
	// Another goroutine may have the socket, and then writes the Replies queued
	// here with everything else. This one waits for it until the Ledger says
	// it may go on, and no longer. For Flush that is only backpressure on the
	// connection's own requests. What is pushed after its latest Reply never
	// makes it wait: its goroutine reads the peer's requests, which for a
	// Replica that has fallen behind are the acknowledgements that show how
	// far.
	for !o.ledger.Closed() {
		if drain && o.ledger.RepliesOut() || !drain && o.ledger.Gate() {
			return nil
		}
		o.waiters.Wait()
	}
	return o.errLocked()
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
	effects := o.ledger.Close()
	o.cause = cause
	// A writer in the middle of a write keeps the bytes it is writing until
	// the write returns, and no longer.
	o.bytes.clear()
	if o.ledger.Class() != ledger.Plain {
		o.shared.mu.Lock()
		delete(o.shared.pushers, o)
		o.shared.mu.Unlock()
	}
	o.unlock(effects)

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
// (replication.Master.EndStreams). The Ledger says when the stream has ended
// (Effects.ShutWrite). The Event loop shuts the write side itself, when a
// Service reports the end of the stream (Status.EndOfStream).
func (o *Output) EndStream() {
	if o == nil {
		return
	}
	o.mu.Lock()
	o.unlock(o.ledger.EndStream())
}

// MarkReplica makes the connection a Replica's: held to the Replica Push limit
// and stall rule, and outside the Push budget. The attach cut calls it before
// the Replica's stream is pushed here (replication.Master.Attach).
func (o *Output) MarkReplica() {
	if o == nil {
		return
	}
	o.mu.Lock()
	effects := o.ledger.MarkReplica(o.shared.now())
	if effects.Deadline != 0 && o.wire == nil && o.attention != nil {
		// Its socket is being offered bytes: the loop learns from a Service
		// when to try it again.
		o.attention()
	}
	o.unlock(effects)
}

// MayReadRequests reports whether the connection's own requests may be read
// and run: whether at most outputHighWater bytes are left to write before its
// latest Reply is out. What was pushed after that Reply does not count, so a
// Replica whose stream waits still has its acknowledgements read; the Push
// limit bounds that output instead. Both networking modes hold a connection's
// requests back by this one rule, the Ledger's Gate. It is the Gate as of the
// connection's last event, read without the lock: the Event loop asks before
// every request.
func (o *Output) MayReadRequests() bool {
	return o == nil || !o.gateShut.Load()
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

// Status is where a connection the Event loop serves stands after a Service.
// Every field is a decision of the Output's Ledger. The loop decides from it
// and compares no byte counts of its own.
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
	// TryAgainAt, unless zero, is when to call Service again even if the
	// poller reports nothing: a Replica's socket has taken nothing of what it
	// is offered, and the try at that time decides whether it has stalled.
	TryAgainAt time.Time
}

// Service writes what is queued to the socket until nothing is left or the
// socket takes no more, and reports where the connection stands. Only the Event
// loop calls it, with the connection's socket. A socket that accepts part of a
// chunk, and reports that it would block (ErrWouldBlock) or reports nothing
// (io.ErrShortWrite), leaves the rest for the next Service. Any other failure
// closes the Output, as does a try of a Replica's socket that takes nothing
// once the stall timeout has passed (Status.TryAgainAt).
func (o *Output) Service(socket io.Writer) Status {
	status, err := o.writeAs(ledger.Pusher, socket)
	if err != nil && !errors.Is(err, ErrWouldBlock) && !errors.Is(err, io.ErrShortWrite) {
		o.Close(err)
		return Status{Closed: o.Err()}
	}
	return status
}

// nowLocked is the time to tell the Ledger with a Take or a Took. The clock is
// read only for a connection whose socket is held to the stall rule
// (ledger.Ledger.Stallable): the Ledger compares the times of no other, and
// every write of every connection passes here.
func (o *Output) nowLocked() ledger.Instant {
	if !o.ledger.Stallable() {
		return 1
	}
	return o.shared.now()
}

// Status reports where the connection stands without writing anything: a
// Service that offers the socket nothing.
func (o *Output) Status() Status {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.statusLocked(0)
}

// statusLocked is where the connection stands, as the Ledger has it. deadline
// is the one the Ledger answered the last write with.
func (o *Output) statusLocked(deadline ledger.Instant) Status {
	if o.ledger.Closed() {
		return Status{Closed: o.errLocked()}
	}
	status := Status{
		MayRead:     o.ledger.Gate(),
		WantWrite:   o.ledger.Writable(),
		EndOfStream: o.ledger.Ended(),
	}
	if deadline != 0 {
		status.TryAgainAt = o.shared.timeOf(deadline)
	}
	return status
}

// writeAs is the one loop that writes a connection's output, for the
// connection's goroutine (the Requester), the pump and the Event loop (the
// Pusher). It asks the Ledger for the socket and how much to offer it (Take),
// writes that, and reports what the socket took (Took), until the Ledger gives
// it nothing, because another writer has the socket, or says that it has
// written all it may (Effects.Released). A Requester is given what was queued
// when it took the socket and nothing queued after, and the Ledger hands the
// socket to the pump for the rest.
//
// The stall rule is the Ledger's answer to Took. A blocking write to a
// Replica ends at the deadline the Ledger gave (unlock) with whatever the
// socket took by then, which is no failure: Took closes the Replica if that
// was nothing, and the loop goes on if not. The Event loop's write returns at
// once, so the Status carries the deadline as the time to try again.
//
// It returns where the connection stood when it stopped, read under the lock
// its last event held, and the write's error, which for a socket that took
// part of a chunk without one is io.ErrShortWrite.
func (o *Output) writeAs(role ledger.Role, w io.Writer) (Status, error) {
	for {
		o.mu.Lock()
		n, effects := o.ledger.Take(role, o.nowLocked())
		if n == 0 {
			status := o.statusLocked(0)
			o.unlock(effects)
			return status, nil
		}
		chunk := o.bytes.next(n)
		o.unlock(effects)

		took, err := w.Write(chunk)

		o.mu.Lock()
		o.bytes.drop(took)
		effects = o.ledger.Took(int64(took), o.nowLocked())
		switch {
		case err == nil && took < len(chunk):
			err = io.ErrShortWrite
		case errors.Is(err, os.ErrDeadlineExceeded) && (effects.Deadline != 0 || effects.Close != nil):
			// The stall rule's deadline ended the write, and Took has given
			// the verdict: closed, or another deadline for what is left.
			err = nil
		}
		stop := err != nil || effects.Released
		var status Status
		if stop {
			status = o.statusLocked(effects.Deadline)
		}
		o.unlock(effects)
		if effects.Close != nil {
			return Status{Closed: o.Err()}, nil
		}
		if stop {
			return status, err
		}
	}
}

// writeDeadliner is a socket whose writes can be given a deadline, as a
// net.Conn's can.
type writeDeadliner interface {
	SetWriteDeadline(t time.Time) error
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
