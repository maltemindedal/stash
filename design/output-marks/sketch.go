// Package output is the sketch of internal/server/output, the package that
// would replace internal/server/output.go. Bodies are not implemented. The
// pure core it describes is prototyped, cut to byte counts, in
// design/output-marks/proto, with the model checker that proves it.
//
// Files of the real package, by what each owns:
//
//	stream.go     the pure core: buffers, Marks, transitions. No lock, clock,
//	              socket or goroutine. Only Take writes Offered; only Close
//	              drops buffers.
//	rules.go      every measure and every rule, and nothing else.
//	output.go     the shell: one mutex around a stream, the clock, the stall
//	              timer, and the execution of a step's effects.
//	blocking.go   the goroutine mode's writer (the connection's goroutine and
//	              the pump). I/O only: it asks the stream what to write.
//	polled.go     the event loop's side: WriteSome and the hooks the loop
//	              implements. I/O only.
//	model_test.go the exhaustive checker against a byte-tagged reference.
//
// internal/server keeps the event loop, the handler and the registries, and
// sees two types: *Output, which the connection's own side holds, and Port,
// which everyone else gets from ClientState.Output.
//
//nolint:unused // A sketch: the unexported core has signatures and no callers yet.
package output

import (
	"io"
	"log/slog"
	"time"

	"github.com/maltemindedal/stash/internal/protocol"
)

// ---- rules.go: measures and rules ----------------------------------------

// Pos is a position in the bytes a connection is sent, counted from its first
// byte. A byte held behind a Reply that has not come has none yet.
type Pos int64

// Marks are where a connection's output stands. Written <= Offered <= Queued;
// ReplyEnd <= Queued. A closed stream has Written == Offered == Queued,
// ReplyEnd <= Written, Held == 0.
type Marks struct {
	Written  Pos   // the socket has taken [0, Written)
	Offered  Pos   // a writer holds the batch [Written, Offered)
	Queued   Pos   // [Offered, Queued) can be written now
	ReplyEnd Pos   // the end of the latest Reply that had bytes
	Held     int64 // bytes behind a Reply that has not come
}

// UntilReply is what the peer must still read to have the latest Reply.
func (m Marks) UntilReply() int64 { panic("not implemented") }

// AfterReply is what waits behind the latest Reply, held bytes included: a
// Replica's stream.
func (m Marks) AfterReply() int64 { panic("not implemented") }

// Unwritten is every byte that waits, held bytes included.
func (m Marks) Unwritten() int64 { panic("not implemented") }

// OnOffer is what a writer offered the socket and it has not taken.
func (m Marks) OnOffer() int64 { panic("not implemented") }

// Role is what the connection is held to. It changes once, at MarkReplica.
type Role uint8

const (
	// Client is any connection that is not a Replica's, subscribers and
	// MONITOR clients included.
	Client Role = iota
	// Replica is a connection that carries a replication stream.
	Replica
)

// Limits are the constants the rules compare with. The shell builds one set
// (DefaultLimits); tests shrink them.
type Limits struct {
	HighWater        int64         // 1 MiB: MayReadNext
	PushLimit        int64         // 32 MiB: a Client that is pushed to
	ReplicaPushLimit int64         // 256 MiB: a Replica's stream
	PushBudget       int64         // 128 MiB: every Client that is pushed to
	WriteBuffer      int64         // 512 MiB: the event loop's ConnMachine bound
	StallTimeout     time.Duration // 30 s
	WriteThrough     int64         // 4 KiB: Flush without waiting for a read
}

// The rules. Each compares one measure with one limit. Neither networking mode
// compares Marks itself: an agent that wants a new rule adds it here, where
// the model test requires a reference definition for it.

// MayReadNext reports whether the connection's next request may be read and
// run: the goroutine mode's Flush waits while it is false, the event loop
// drops read interest. Nothing pushed after the latest Reply counts, so a
// Replica's acknowledgements are read however much of its stream waits.
func MayReadNext(m Marks, l Limits) bool { panic("not implemented") }

// Drained reports whether every Reply queued so far is written: when Finish
// may close the connection.
func Drained(m Marks) bool { panic("not implemented") }

// OverPushLimit reports whether a Push of frame bytes takes the connection
// past its limit: a Client's counts Unwritten, a Replica's AfterReply.
func OverPushLimit(m Marks, r Role, frame int, l Limits) bool { panic("not implemented") }

// BudgetShare is what the connection counts toward the Push budget: a Client
// that has been pushed to counts Unwritten, a Replica nothing.
func BudgetShare(m Marks, r Role, pushedTo bool) int64 { panic("not implemented") }

// WriteThroughDue reports whether the connection's goroutine should write
// what it queued without waiting for its next read: bytes no writer holds
// have reached WriteThrough.
func WriteThroughDue(m Marks, l Limits) bool { panic("not implemented") }

// OverWriteBuffer is the event loop's bound on one connection's output
// (ConnMachine.bufferResponses today, which compares Backlog in place).
func OverWriteBuffer(m Marks, l Limits) bool { panic("not implemented") }

// ---- stream.go: the pure core ---------------------------------------------

// WriterKind is how a writer behaves while it holds a batch.
type WriterKind uint8

const (
	// Blocking stays inside one write while the batch is on offer: the
	// connection's goroutine, or the pump.
	Blocking WriterKind = iota + 1
	// Polled returns at once and comes back when the socket has room: the
	// event loop.
	Polled
)

// Batch is what a writer was handed: the bytes at [From, To). To is fixed when
// the batch is taken, so nothing queued later joins it.
type Batch struct {
	From, To Pos
	chunks   [][]byte // aliases the stream's buffers; valid until Wrote
}

// Chunks are the bytes of the batch not yet written, in order.
func (b *Batch) Chunks() [][]byte { panic("not implemented") }

// Effects are what a step asks the shell to do after it releases the lock.
// The core never does any of it.
type Effects struct {
	ScheduleWriter bool          // bytes can be written and nobody holds a batch
	WakeWaiters    bool          // a Flush or Finish may return
	ArmStallTimer  time.Duration // > 0: run Tick after this long
	AskAttempt     bool          // a Polled writer must try the socket now
	CloseWrite     bool          // EndStream: everything queued is written
	Close          error         // close the connection with this cause
}

// stream is one connection's output. Every method is a pure transition: the
// caller holds the Output's lock and passes the time in. Push and Reply take
// no time, so they cannot start or judge the stall rule.
type stream struct {
	lim      Limits
	marks    Marks
	buffers  [][]byte // [Written, Queued), whole buffers in order; nil once closed
	held     []byte
	holding  bool
	role     Role
	pushedTo bool
	ending   bool
	closed   error // nil while open
	writer   WriterKind
	progress time.Time // the socket last took a byte, or was offered the batch
}

func (s *stream) push(frame []byte) Effects         { panic("not implemented") }
func (s *stream) holdPushes()                       { panic("not implemented") }
func (s *stream) reply(encoded []byte) Effects      { panic("not implemented") }
func (s *stream) markReplica(now time.Time) Effects { panic("not implemented") }
func (s *stream) take(w WriterKind, now time.Time) (Batch, bool) {
	panic("not implemented")
}
func (s *stream) wrote(b *Batch, n int, now time.Time) Effects { panic("not implemented") }
func (s *stream) tick(now time.Time) Effects                   { panic("not implemented") }
func (s *stream) endStream() Effects                           { panic("not implemented") }

// close drops every buffer, the held ones included, and sets the marks so
// that nothing waits. Whatever still refers to a closed stream (a timer, a
// loop's list) keeps a struct alive, not a snapshot.
func (s *stream) close(cause error) Effects { panic("not implemented") }

// flushStep is what the connection's goroutine does next in Flush or Finish:
// write a batch, wait for the writer that holds one, or return.
func (s *stream) flushStep(goal Goal) (Step, Batch) { panic("not implemented") }

// Goal is what a wait of the connection's goroutine is for.
type Goal uint8

const (
	// UntilMayRead is Flush: return once MayReadNext.
	UntilMayRead Goal = iota
	// UntilDrained is Finish: return once Drained.
	UntilDrained
)

// Step is flushStep's answer.
type Step uint8

const (
	Return       Step = iota // the goal is met, or the stream is closed
	WriteBatch               // this goroutine took a batch: write it
	WaitForOther             // another writer holds the batch: wait for WakeWaiters
)

// ---- output.go: the shell -------------------------------------------------

// Shared is what every connection's Output shares: the limits and the Push
// budget. One per server.
type Shared struct{}

// NewShared returns the state for one server's connections.
func NewShared(l Limits, logger *slog.Logger) *Shared { panic("not implemented") }

// DefaultLimits are the server's limits.
func DefaultLimits() Limits { panic("not implemented") }

// Output is a connection's output as the connection's own side holds it: the
// handler in the goroutine mode, the eventConn in the event loop. Nothing else
// gets one: ClientState stores and returns its Port.
type Output struct {
	// mu guards s. Lock order: a registry's or the Master's, then mu, then
	// the budget's. mu is never held across a socket write.
	s stream
}

// Port is the Output as everyone but the connection's own side reaches it:
// registries and the replication Master, under their locks. None of its
// methods waits on the socket. replication.Output is this interface's subset.
type Port interface {
	// Push queues a frame another connection produced. It returns false once
	// the connection is closed, by this Push or before.
	Push(frame []byte) bool
	// HoldPushes makes what is pushed from now on wait for the current
	// request's Reply. Called inside the cut that changes what reaches the
	// connection: SUBSCRIBE, MONITOR, a PSYNC's attach.
	HoldPushes()
	// MarkReplica holds the connection to the Replica rules.
	MarkReplica()
	// EndStream shuts the write side once everything queued is written.
	EndStream()
	// Abort closes the connection without waiting, dropping what waits.
	Abort(cause error)
	// Err is why the connection was closed, or nil.
	Err() error
	// Backlog is Unwritten, for INFO. No rule may read it: rules read Marks.
	Backlog() int
}

// NewBlocking returns the Output of a connection served by its own goroutine.
func (sh *Shared) NewBlocking(conn io.ReadWriteCloser, logger *slog.Logger) *Output {
	panic("not implemented")
}

// NewPolled returns the Output of an event-loop connection. loop is told when
// the connection needs writing and when to close it.
func (sh *Shared) NewPolled(loop LoopHooks, logger *slog.Logger) *Output {
	panic("not implemented")
}

// Port is the Output as the registries and the Master reach it.
func (o *Output) Port() Port { panic("not implemented") }

// Reply queues the Replies to the request that just ran, none included, and
// releases what HoldPushes held. It encodes values before it takes the lock,
// always, into the owner's scratch buffer; only the owner calls Reply, so the
// scratch is not shared. It never waits.
func (o *Output) Reply(values ...protocol.Value) error { panic("not implemented") }

// Flush writes what is queued, or waits for the writer that holds the socket,
// until MayReadNext (Goal UntilMayRead). The goroutine mode calls it before a
// read, before a command blocks, and once WriteThrough is waiting. The event
// loop never calls it.
func (o *Output) Flush() error { panic("not implemented") }

// FlushIfFull is Flush when WriteThroughDue, and nothing otherwise. The
// handler calls it after every Reply.
func (o *Output) FlushIfFull() error { panic("not implemented") }

// Finish is the connection goroutine's teardown: it waits until Drained (Goal
// UntilDrained) and then closes the connection. It is the only way the owner
// closes it, so the drain and the close cannot be split or given another mark.
func (o *Output) Finish() { panic("not implemented") }

// MayReadNext is the event loop's read gate: rules.MayReadNext on the marks.
func (o *Output) MayReadNext() bool { panic("not implemented") }

// WantsWrite reports whether the event loop should wait for writability:
// something is queued or on offer.
func (o *Output) WantsWrite() bool { panic("not implemented") }

// WriteSome is the event loop's one attempt at the socket: it takes a Polled
// batch if none is held, writes what w accepts, and records it with the time.
// An attempt that takes nothing once the stall timeout has passed is the
// stall verdict, returned as errReplicaStalled.
func (o *Output) WriteSome(w io.Writer) error { panic("not implemented") }

// apply runs a step's effects after the lock is released: the only place the
// shell starts a goroutine, arms a timer, wakes waiters or closes a socket.
func (o *Output) apply(e Effects) { panic("not implemented") }

// LoopHooks are what the event loop gives a Polled Output. Each is safe to
// call with the Output's lock released and must not block.
type LoopHooks interface {
	MarkDirty()    // the connection needs WriteSome soon
	RequestClose() // close the socket on the loop
	ShutWrite()    // EndStream reached its end
}

// stallCell is how a timer reaches an Output: Close empties it. A timer that
// held the Output would keep it reachable for the stall timeout (e3fce43);
// since close drops every buffer, what such a leak keeps is the struct.
type stallCell struct{}
