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

// outputReplicaStallTimeout is how long a Replica may accept nothing while
// output waits for it before it is dropped.
const outputReplicaStallTimeout = 30 * time.Second

// outputWriteThrough is how much Client output a connection's goroutine lets
// wait before it writes it out without waiting for the next read, as a 4 KiB
// bufio.Writer would.
const outputWriteThrough = 4 << 10

// outputHighWater is how much Client output may wait before the connection's
// own requests stop being read and run until it drains, so that a client that
// pipelines without reading cannot turn small requests into unbounded Replies.
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

// errReplicaStalled reports that a Replica accepted nothing for
// outputReplicaStallTimeout while output waited for it.
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
	// flush is Output.Flush.
	flush(o *Output) error
	// shut closes the connection, once, without waiting for a write in
	// progress. It is called with no Output lock held.
	shut()
	// closeWrite ends the stream to the peer after everything queued was
	// written: Output.EndStream.
	closeWrite()
	// peerClosed reports whether the peer has closed its side, when the driver
	// can tell without reading.
	peerClosed() bool
}

// Output is the Client output of one connection: its Replies and the Pushed
// messages it receives (pub/sub messages, MONITOR events, and for a Replica
// its stream), in the order they were produced. No method but Flush waits on
// the peer's socket. Safe for concurrent use.
//
// Lock order: a registry's lock, then Output.mu, then the shared budget's.
// Output.mu is never held across a socket write.
type Output struct {
	mu sync.Mutex
	// queued is what waits to be written, appended under mu.
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
	// unflushed is set by a Reply and cleared by the Flush that follows it.
	unflushed bool
	holding   bool
	closed    bool
	cause     error
	// replica holds the connection to the Replica Push limit and stall rule,
	// and leaves it out of the Push budget.
	replica bool
	// counted is set while the connection is one of shared.pushers.
	counted bool
	// ending is set once the stream is to end when everything queued has been
	// written (EndStream).
	ending bool
	// lastProgress is when the socket last accepted a byte, or when output
	// began to wait; stallArmed is set while a timer is due to check it.
	lastProgress time.Time
	stallArmed   bool

	// drained is signalled when a writer finishes and when the backlog falls to
	// outputHighWater.
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
// connection's own executor calls it. It never waits.
func (o *Output) Reply(values ...protocol.Value) error {
	o.mu.Lock()
	if o.closed {
		err := o.errLocked()
		o.mu.Unlock()
		return err
	}
	before := len(o.queued)
	queued, err := protocol.AppendValues(o.queued, values)
	if err != nil {
		o.mu.Unlock()
		return err
	}
	o.queued = queued
	if added := len(queued) - before; added > 0 {
		o.growLocked(added)
		o.unflushed = true
	}

	if o.holding {
		o.holding = false
		if len(o.held) > 0 {
			o.queued = append(o.queued, o.held...)
			o.held = o.held[:0]
			if cap(o.held) > outputRetainedBuffer {
				o.held = nil
			}
			// The connection's goroutine must not be the one that writes a stream
			// of Pushed messages to a peer that may have stopped reading.
			o.driver.schedule(o)
		}
	}
	o.mu.Unlock()
	return nil
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
	limit := o.shared.pushLimit
	if o.replica {
		limit = o.shared.replicaPushLimit
	}
	if waiting := o.backlog.Load() + int64(len(frame)); waiting > limit {
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
// own connection (SUBSCRIBE, UNSUBSCRIBE, MONITOR, a Replica's attach) calls it
// at that change, so that what the change lets in follows the request's Reply.
func (o *Output) HoldPushes() {
	if o == nil {
		return
	}
	o.mu.Lock()
	o.holding = true
	o.mu.Unlock()
}

// Flush writes out what is queued, for the connection's own goroutine: before
// it waits for the next request, once outputWriteThrough is waiting, and before
// a command blocks. When another goroutine is already writing it leaves the
// writing to that one. It then waits only if Replies were queued since the last
// Flush and more than outputHighWater is waiting, until the backlog has fallen:
// a client that does not read its Replies stops having its requests run. It
// reports why the connection is closed, if it is. The Event loop writes its
// connections itself, so there Flush does nothing.
func (o *Output) Flush() error {
	if o == nil {
		return nil
	}
	return o.driver.flush(o)
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
	o.queued, o.held = nil, nil
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
// close. Shutdown ends a Replica's stream this way.
func (o *Output) EndStream() {
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
// and stall rule, and outside the Push budget.
func (o *Output) MarkReplica() {
	if o == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()

	o.replica = true
	o.lastProgress = time.Now()
	if o.counted {
		o.counted = false
		o.shared.total.Add(-o.backlog.Load())
		o.shared.mu.Lock()
		delete(o.shared.pushers, o)
		o.shared.mu.Unlock()
	}
	o.armStallLocked()
}

// Backlog reports how many bytes of Client output wait for the connection.
func (o *Output) Backlog() int {
	if o == nil {
		return 0
	}
	return int(o.backlog.Load())
}

// Err reports why the connection was closed, or nil while it is open.
func (o *Output) Err() error {
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

func (o *Output) errLocked() error {
	if o.cause != nil {
		return o.cause
	}
	return errOutputClosed
}

// pendingLocked is how many bytes can be written now: everything but what is
// held behind a Reply.
func (o *Output) pendingLocked() int {
	return len(o.queued) + len(o.inflight) - o.sent
}

// growLocked adds delta, which may be negative, to the backlog and to the Push
// budget's total when the connection counts toward it.
func (o *Output) growLocked(delta int) {
	if delta == 0 {
		return
	}
	if o.backlog.Add(int64(delta)) == int64(delta) && delta > 0 && o.replica {
		// Output began to wait: the stall rule counts from here.
		o.lastProgress = time.Now()
		o.armStallLocked()
	}
	if o.counted {
		o.shared.total.Add(int64(delta))
	}
}

// armStallLocked starts the timer that enforces the Replica stall rule, unless
// one is running or there is nothing to enforce.
func (o *Output) armStallLocked() {
	if !o.replica || o.stallArmed || o.closed || o.backlog.Load() == 0 {
		return
	}
	o.stallArmed = true
	wait := o.shared.stallTimeout - time.Since(o.lastProgress)
	time.AfterFunc(max(wait, time.Millisecond), o.checkStall)
}

// checkStall closes a Replica that has accepted nothing for the stall timeout
// while output waited, and otherwise looks again when that could next be true.
func (o *Output) checkStall() {
	o.mu.Lock()
	o.stallArmed = false
	if o.closed || o.backlog.Load() == 0 {
		o.mu.Unlock()
		return
	}
	idle := time.Since(o.lastProgress)
	if idle < o.shared.stallTimeout {
		o.armStallLocked()
		o.mu.Unlock()
		return
	}
	o.mu.Unlock()

	o.Close(fmt.Errorf("%w of %v", errReplicaStalled, o.shared.stallTimeout))
}

// writeTo writes what is queued to w until nothing is left or w fails, and
// returns w's error. The caller is the only writer: the goroutine that set
// writing, or the Event loop. A w that accepts part of a chunk, and reports that
// it would block as the Event loop's does or reports nothing (io.ErrShortWrite),
// leaves the rest for the next call.
func (o *Output) writeTo(w io.Writer) error {
	for {
		o.mu.Lock()
		if o.closed {
			err := o.errLocked()
			o.stopWritingLocked()
			o.mu.Unlock()
			return err
		}
		if o.sent == len(o.inflight) {
			if len(o.queued) == 0 {
				end := o.ending
				o.stopWritingLocked()
				o.mu.Unlock()
				if end {
					o.driver.closeWrite()
				}
				return nil
			}
			o.inflight, o.queued, o.spare, o.sent = o.queued, o.spare[:0], nil, 0
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
			if o.backlog.Load() <= outputHighWater {
				o.drained.Broadcast()
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
	if d.pump == nil {
		// A failed write closes the connection (write), which is all there is
		// to do about it here.
		d.pump = func() { _ = d.write(o) }
	}
	go d.pump()
}

func (d *goroutineOutputDriver) flush(o *Output) error {
	o.mu.Lock()
	// Another goroutine may have the socket, and then writes the Replies queued
	// here with everything else. Waiting for it is only backpressure on this
	// connection's own Replies. A connection that queued none never waits: its
	// goroutine reads the peer's requests, which for a Replica that has fallen
	// behind are the acknowledgements that show how far.
	replied := o.unflushed
	o.unflushed = false
	for replied && o.writing && !o.closed && o.backlog.Load() > outputHighWater {
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
	o.mu.Unlock()

	return d.write(o)
}

func (d *goroutineOutputDriver) write(o *Output) error {
	err := o.writeTo(d.wire)
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

func (d *goroutineOutputDriver) closeWrite() {
	if ender, ok := d.wire.(streamEnder); ok {
		// One that fails is closed with every other connection afterwards.
		_ = ender.CloseWrite()
	}
}

func (d *goroutineOutputDriver) peerClosed() bool {
	conn, ok := d.wire.(net.Conn)
	return ok && peerClosed(conn)
}

// unscheduledOutputDriver is the driver of an Output that only its owner
// writes, by calling writeTo: a ConnMachine built without an Event loop.
type unscheduledOutputDriver struct{}

func (unscheduledOutputDriver) schedule(*Output)    {}
func (unscheduledOutputDriver) flush(*Output) error { return nil }
func (unscheduledOutputDriver) shut()               {}
func (unscheduledOutputDriver) closeWrite()         {}
func (unscheduledOutputDriver) peerClosed() bool    { return false }

var discardLogger = slog.New(slog.NewTextHandler(io.Discard, nil))
