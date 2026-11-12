//go:build sketch

// Package output is a sketch of internal/server/output: everything one
// connection is sent, in both networking modes. Bodies are not implemented.
// The sketch build tag keeps its unused fields out of lint: check it with
// go build -tags sketch ./design/... and go vet -tags sketch ./design/...
//
// Three parts, each with one job:
//
//	ledger.Ledger  decides. Pure: positions and times in, decisions and
//	               Effects out. internal/server/output/internal/ledger, so
//	               only this package can import it.
//	chain          holds the bytes, in order. No decisions.
//	Output         holds the lock, calls the Ledger, and carries out its
//	               Effects in one function (carryOut).
//
// An Output method has the same three lines whatever it does: lock, tell the
// Ledger, carry out what it answered. A method that compares byte counts, reads
// a clock or starts a timer is in the wrong file: that belongs to the Ledger,
// where the model test checks it.
//
// What may wait on the peer's socket is decided by who calls, not by mode:
//
//	the connection's own executor   Reply, Flush, Drain   may wait
//	anyone                          Push, HoldPushes, MarkReplica,
//	                                EndStream, Close, Err never wait
//	the Event loop                  Service, Status       never wait
//
// Lock order is unchanged: a registry's lock or the replication Master's, then
// Output.mu, then the Push budget's. Output.mu is never held across a socket
// write.
package output

import (
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/maltemindedal/stash/design/output-ledger/ledger"
	"github.com/maltemindedal/stash/internal/protocol"
)

// Shared is what every connection's Output of one server shares: the limits,
// the clock, and the Push budget's running total.
type Shared struct {
	limits ledger.Limits
	// now is the clock every Output reads, replaced in tests.
	now func() time.Time
	// The budget: the output waiting for every Subscriber together. An Output
	// reports its Ledger's Waiting to it from carryOut and nowhere else, so the
	// total is derived from the Ledgers and cannot drift from them.
	budget int64
	mu     sync.Mutex
	total  int64
	most   map[*Output]int64
}

// NewShared returns the limits and budget of one server.
func NewShared() *Shared { panic("not implemented") }

// Wire is the socket of a connection served by its own goroutine. A net.Conn
// is one.
type Wire interface {
	io.Writer
	// SetWriteDeadline bounds a write in progress and the ones after it. The
	// stall rule is enforced through it: a Replica's write returns at the
	// Ledger's Deadline with what the socket took, and Ledger.Took decides.
	SetWriteDeadline(t time.Time) error
	Close() error
}

// Output is the Client output of one connection: its Replies and the Pushed
// messages it receives, in the order they were produced. Safe for concurrent
// use.
type Output struct {
	mu     sync.Mutex
	ledger ledger.Ledger
	bytes  chain
	// waiters wait for Ledger.Gate or Ledger.RepliesOut (Effects.Signal).
	waiters *sync.Cond
	cause   error

	// Exactly one of wire and attention is set: the networking mode.
	wire      Wire
	attention func()

	shared *Shared
	logger *slog.Logger
}

// chain is the bytes that wait, in the order of the stream: the buffers of
// today's Output (ahead, queued, inflight, spare, held) and nothing else. It
// appends, hands out the next n bytes, and drops what the socket took. It makes
// no decision and keeps no count the Ledger does not have: a test checks after
// every operation that its length is the Ledger's Waiting.
type chain struct {
	whole  [][]byte
	queued []byte
	held   []byte
	head   int
	spare  []byte
}

// NewBlocking returns the Output of a connection served by its own goroutine.
// That goroutine writes its Replies (Flush). A pump goroutine, started when the
// Ledger grants the socket to a Pusher, writes what other connections push.
func NewBlocking(shared *Shared, wire Wire, logger *slog.Logger) *Output {
	panic("not implemented")
}

// NewPolled returns the Output of a connection the Event loop serves. The loop
// writes it (Service). attention tells the loop to call Service for this
// connection: bytes were queued from elsewhere, the stream is to end, or the
// connection was closed. It may be called with any lock held and must not
// block.
func NewPolled(shared *Shared, attention func(), logger *slog.Logger) *Output {
	panic("not implemented")
}

// Reply queues the Replies to the request the connection just ran (none is
// allowed) and releases the Pushed messages held since HoldPushes. Only the
// connection's own executor calls it. Served by its own goroutine, a connection
// with 4 KiB of Replies waiting writes them here, as Flush does.
func (o *Output) Reply(values ...protocol.Value) error {
	// encode a large Reply before the lock (today's isLargeReply)
	// lock; bytes.appendReply; effects := ledger.Reply(n); unlock
	// carryOut(effects)
	// blocking mode and ledger.Full(): return o.Flush()
	panic("not implemented")
}

// Push queues a frame another connection produced. It never waits. It returns
// false if the connection is closed, or if this Push closed it.
func (o *Output) Push(frame []byte) bool {
	// lock; accepted, effects := ledger.Push(len(frame)); if accepted { bytes.appendPush or bytes.hold }; unlock
	// carryOut(effects)
	panic("not implemented")
}

// HoldPushes makes Pushed messages queued from now on wait until the current
// request has replied.
func (o *Output) HoldPushes() { panic("not implemented") }

// Flush is for the connection's own goroutine before it waits for its next
// request or blocks in a command. It writes the Replies that are queued, unless
// a Pusher has the socket, and returns once Ledger.Gate is open. On the Event
// loop it does nothing: the loop reads Status.MayRead.
func (o *Output) Flush() error {
	// for {
	//     o.writeAs(ledger.Requester)
	//     lock; wait on waiters while !ledger.Gate() and a Pusher has the socket; unlock
	//     if ledger.Gate() { return o.Err() }
	// }
	panic("not implemented")
}

// Drain is Flush for a connection that has run its last request: it returns
// once Ledger.RepliesOut is true.
func (o *Output) Drain() { panic("not implemented") }

// MarkReplica makes the connection a Replica's.
func (o *Output) MarkReplica() { panic("not implemented") }

// EndStream ends the stream to the peer once everything queued is written.
func (o *Output) EndStream() { panic("not implemented") }

// Close closes the connection from any goroutine without waiting for a write
// in progress, and drops what was waiting for it.
func (o *Output) Close(cause error) { panic("not implemented") }

// Err reports why the connection was closed, or nil while it is open.
func (o *Output) Err() error { panic("not implemented") }

// Waiting reports how many bytes wait for the connection, for INFO. No caller
// decides anything from it: a test lists the files that may call it.
func (o *Output) Waiting() int64 { panic("not implemented") }

// Status is what the Event loop needs to know about a connection after an
// event. Every field is a Ledger decision. The loop compares no byte counts.
type Status struct {
	// Closed is why the Output is closed, or nil. The loop closes the
	// connection: there is no separate list of close requests.
	Closed error
	// MayRead is Ledger.Gate: read and run the connection's requests.
	MayRead bool
	// WantWrite is Ledger.Writable: ask the poller for writable.
	WantWrite bool
	// RepliesOut is Ledger.RepliesOut: a closing connection may be closed.
	RepliesOut bool
	// EndOfStream: EndStream was called and everything is written. Shut the
	// write side.
	EndOfStream bool
	// TryAgainAt is the Ledger's Deadline, or zero. The loop calls Service
	// again at that time even if the poller reports nothing, and that try
	// decides whether the Replica has stalled.
	TryAgainAt time.Time
}

// Service writes what the socket takes and reports where the connection
// stands. Only the Event loop calls it.
func (o *Output) Service(socket io.Writer) Status {
	// o.writeAs(ledger.Pusher), with socket for the wire: stops when the socket would block
	// return o.Status()
	panic("not implemented")
}

// Status is Service without the write.
func (o *Output) Status() Status { panic("not implemented") }

// writeAs is the one write loop, for the connection's goroutine, the pump and
// the Event loop:
//
//	for {
//	    lock; n, effects := ledger.Take(role, now); chunk := bytes.next(n); unlock
//	    carryOut(effects)            // StartPusher, Deadline
//	    if n == 0 { return }
//	    took, err := socket.Write(chunk)
//	    lock; bytes.drop(took); effects = ledger.Took(took, now); unlock
//	    carryOut(effects)            // Signal, Close(ErrStalled), Deadline
//	    if err is not "would block" or "deadline": Close(err); return
//	    if err is "would block": return
//	}
//
// carryOut is the one place an Effects is acted on:
//
//	StartPusher  blocking: go pump.   polled: attention()
//	Signal       waiters.Broadcast()
//	Close        o.Close(cause)
//	Deadline     blocking: wire.SetWriteDeadline.   polled: kept for Status.TryAgainAt
//	and always   shared.account(o, ledger.Class(), ledger.Waiting())
var _ = ledger.Effects{}
