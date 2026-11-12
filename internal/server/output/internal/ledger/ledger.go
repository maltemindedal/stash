// Package ledger decides everything about one connection's output that is a
// matter of counting: whether its requests may be read, whether its last
// Replies are out, whether a Push fits, who may write, how much, and whether a
// Replica's socket has stalled.
//
// A Ledger holds no bytes, no lock, no clock, no socket and no timer. It is
// told what happened (Reply, Push, Hold, Take, Took, MarkReplica, EndStream,
// Finish, Close) with
// the sizes and the time, and answers with a decision and the Effects its
// caller must carry out. Its caller, the Output (package output), holds the
// lock and the bytes. The package is under output's internal directory, so
// only package output can import it: the server and its Event loop cannot make
// one of these decisions with a rule of their own.
//
// The connection's output is one stream of bytes, and every decision is a
// distance between positions in it:
//
//	   taken          replyEnd                    end
//	     |               |                         |
//	-----+---------------+-------------------------+ - - - - +
//	     | ahead of the  | pushed after the latest | held    |
//	     | latest Reply  | Reply                   |         |
//
//	decision                      counts                        method
//	may its requests be read      taken..replyEnd               Gate
//	are its last Replies out      taken..replyEnd is empty      RepliesOut
//	write through before reading  taken..replyEnd               Full
//	does a Push fit, subscriber   taken..end, and held          Push
//	does a Push fit, Replica      replyEnd..end, and held       Push
//	its share of the Push budget  taken..end, and held, for a   Budgeted
//	                              subscriber
//	may this writer write, what   taken..seal or taken..end     Take
//	has the socket stalled        a try that took nothing       Took
//	has the stream ended          taken..end is empty, after    Ended
//	                              EndStream
//	is a Reply too much to hold   taken..replyEnd               Reply
//
// No decision counts anything else, and there is no accessor for a raw
// position: a caller cannot make a decision of its own from one. To change
// what a decision counts, change it here and run this directory's tests.
// model_test.go walks every reachable state of a small Ledger in both
// networking modes and compares each decision with a plain restatement of the
// rule, so a wrong measure fails with the shortest sequence of events that
// shows it. It is not built under the race detector, where the walk takes
// about ten times as long and has nothing to find: a Ledger starts no
// goroutine and is used only under its caller's lock.
//
// Reply and Hold are the connection's own goroutine's, and so is the Requester
// role. That goroutine asks for the socket (Take) until it is given nothing or
// told that it has written all it may (Took), and calls neither Reply nor Hold
// in between. The model holds it to that.
package ledger

import "errors"

// Instant is a time on the caller's monotonic clock, in the unit of
// Limits.StallTimeout. The zero Instant means "none" in Effects.Deadline, so
// the caller's clock starts above zero.
type Instant int64

// Class is what kind of connection the output belongs to. It selects the Push
// limit, whether the connection counts toward the Push budget, and whether the
// stall rule applies. One value, so that a Replica cannot also be counted
// toward the budget.
type Class uint8

const (
	// Plain has received no Pushed message.
	Plain Class = iota
	// Subscriber has received one: a pub/sub or MONITOR connection.
	Subscriber
	// Replica receives the replication stream.
	Replica
)

// Role is who writes to the socket.
type Role uint8

const (
	// NoWriter means nobody has the socket.
	NoWriter Role = iota
	// Requester is the connection's own goroutine, writing its Replies before
	// it reads the next request. It writes what was queued when it took the
	// socket and nothing queued after.
	Requester
	// Pusher is the writer that does not read requests: the pump goroutine,
	// or the Event loop.
	Pusher
)

// Limits are the thresholds of one server's connections.
type Limits struct {
	// HighWater is how much may be left to write before the latest Reply is
	// out while the connection's requests are still read.
	HighWater int64
	// WriteThrough is how much may be left to write before the latest Reply
	// is out before the connection's goroutine writes it without waiting for
	// its next read.
	WriteThrough int64
	// Chunk is the most one socket write is given.
	Chunk int64
	// PushLimit bounds everything that waits for a Subscriber.
	PushLimit int64
	// ReplicaPushLimit bounds what waits for a Replica behind its latest Reply.
	ReplicaPushLimit int64
	// StallTimeout is how long the socket of a Replica, or of a connection
	// that is finishing, may take nothing of what it is offered.
	StallTimeout Instant
	// ReplyLimit is how much may be left to write before the latest Reply is
	// out. A Reply that leaves more closes the connection. Zero is no limit,
	// for a connection whose goroutine waits on the socket instead (Full).
	ReplyLimit int64
}

// ErrStalled is the Close cause when a try of a socket that is held to the
// stall rule took nothing, StallTimeout or more after the socket last took a
// byte or was first offered the bytes.
var ErrStalled = errors.New("ledger: socket accepted no output within the stall timeout")

// ErrReplyLimit is the Close cause when a Reply leaves more than ReplyLimit to
// write before it is out.
var ErrReplyLimit = errors.New("ledger: more left to write before the latest reply is out than the connection's reply limit")

// ErrPushLimit is the Close cause when a Push would leave more waiting than
// the connection's Push limit.
var ErrPushLimit = errors.New("ledger: more output waiting than the connection's push limit")

// Effects is what the caller must do after an event, all of it. The Output
// carries Effects out in one function, so no event can skip one.
type Effects struct {
	// StartPusher: the socket was granted to a Pusher. Start the pump, or list
	// the connection for the Event loop.
	StartPusher bool
	// Signal: wake whoever waits for Gate or RepliesOut.
	Signal bool
	// Close: close the connection with this cause.
	Close error
	// ShutWrite: the stream to the peer has ended. EndStream was called and
	// everything queued is written: shut the write side of the socket.
	ShutWrite bool
	// Released: the writer whose write this was has written all it may and no
	// longer has the socket. It does not ask for it again.
	Released bool
	// Deadline: a try of the socket must end at or after this Instant even if
	// the socket takes nothing, so that Took can give the stall verdict. A
	// blocking writer sets it as the write deadline. The Event loop wakes then
	// and tries the socket. Zero means none.
	Deadline Instant
}

// Ledger is the accounting of one connection's output. The zero Ledger with
// its Limits set is an open, Plain connection with nothing queued. It is a
// comparable value: the model test copies and compares it.
type Ledger struct {
	lim   Limits
	class Class

	closed  bool
	holding bool
	// finishing is set once the connection has run its last request (Finish).
	finishing bool
	// ending is set once the stream is to end when everything queued is
	// written (EndStream).
	ending bool

	// taken is how many bytes the socket has accepted, end how many were
	// queued for writing, replyEnd where the latest Reply ends. held bytes
	// wait behind a Reply that has not come and are not in the stream yet.
	taken, end, replyEnd int64
	held                 int64

	// writer has the socket. A Requester writes up to seal.
	writer Role
	seal   int64

	// offerEnd is the end of what the socket was last offered: bytes
	// taken..offerEnd are on offer. trying is set while a socket write is in
	// progress. progress is when the socket last took a byte of the offer, or
	// was first offered it.
	offerEnd int64
	trying   bool
	progress Instant
}

// New returns the Ledger of a new connection.
func New(limits Limits) Ledger {
	return Ledger{lim: limits}
}

// Reply records n bytes of Replies to the request the connection just ran,
// and releases what was held since Hold behind them. n may be zero. A Reply
// that leaves more than ReplyLimit to write before it is out closes the
// connection. What it released behind it does not count: a Push limit bounds
// that.
func (l *Ledger) Reply(n int64) Effects {
	if l.closed {
		return Effects{}
	}
	l.end += n
	if n > 0 {
		l.replyEnd = l.end
	}
	if l.holding {
		l.holding = false
		l.end += l.held
		l.held = 0
	}
	if l.lim.ReplyLimit > 0 && l.aheadOfReply() > l.lim.ReplyLimit {
		return Effects{Close: ErrReplyLimit}
	}
	return l.grantPusher()
}

// Push records a Pushed message of n bytes, or refuses it. A refusal that
// carries a Close cause closes the connection for passing its Push limit.
func (l *Ledger) Push(n int64) (accepted bool, effects Effects) {
	if l.closed {
		return false, Effects{}
	}
	limit, waiting := l.lim.PushLimit, l.Waiting()+n
	if l.class == Replica {
		// The snapshot a Replica is still being sent says nothing of how far
		// behind it is.
		limit, waiting = l.lim.ReplicaPushLimit, waiting-l.aheadOfReply()
	}
	if waiting > limit {
		return false, Effects{Close: ErrPushLimit}
	}
	if l.class == Plain {
		l.class = Subscriber
	}
	if l.holding {
		l.held += n
		return true, Effects{}
	}
	l.end += n
	return true, l.grantPusher()
}

// Hold makes what is pushed from now on wait for the current request's Reply.
func (l *Ledger) Hold() {
	if !l.closed {
		l.holding = true
	}
}

// Holding reports whether a Push accepted now waits for the current request's
// Reply. The Output asks it to know where the message's bytes go.
func (l *Ledger) Holding() bool { return l.holding }

// MarkReplica makes the connection a Replica's, whose socket is held to the
// stall rule. A socket that is being offered bytes at that moment, and was not
// held to the rule until now, has StallTimeout from now to take some.
func (l *Ledger) MarkReplica(now Instant) Effects {
	if l.closed {
		return Effects{}
	}
	l.startCount(now)
	l.class = Replica
	return Effects{Deadline: l.deadline()}
}

// Finish records that the connection has run its last request: its owner now
// only waits for its Replies to be out (RepliesOut) and then closes it. From
// here on its socket is held to the stall rule, as a Replica's is, so that a
// peer that takes none of its last Replies does not keep the connection, and
// whoever waits for it, until the server shuts down.
func (l *Ledger) Finish(now Instant) Effects {
	if l.closed || l.finishing {
		return Effects{}
	}
	l.startCount(now)
	l.finishing = true
	return Effects{Deadline: l.deadline()}
}

// startCount starts the stall count for bytes that were on offer before the
// socket was held to the stall rule.
func (l *Ledger) startCount(now Instant) {
	if !l.Stallable() && l.offerEnd > l.taken {
		l.progress = now
	}
}

// Stallable reports whether the socket is held to the stall rule: a Replica's,
// and that of a connection that has run its last request (Finish). Only then
// does the Ledger compare the times it is told with Take and Took, so its
// caller need not read a clock for any other connection: the model tells such
// a Ledger a time that is wrong and requires the same decisions.
func (l *Ledger) Stallable() bool {
	return l.class == Replica || l.finishing
}

// Take asks for the socket on behalf of role and returns how many bytes to
// offer it next. Zero means there is nothing for this writer: another has the
// socket, or this one has written all it may and has given the socket up.
//
// A Requester may write the bytes that were queued when it took the socket. The
// caller reports the result of the write with Took before it calls Take again,
// and does not call it again once Took says the socket is released.
func (l *Ledger) Take(role Role, now Instant) (n int64, effects Effects) {
	if l.closed || l.trying {
		return 0, Effects{}
	}
	if l.writer == NoWriter {
		l.writer = role
		l.seal = l.end
	}
	if l.writer != role {
		return 0, Effects{}
	}
	n = min(l.writerLimit()-l.taken, l.lim.Chunk)
	if n <= 0 {
		return 0, Effects{StartPusher: l.release()}
	}
	if l.offerEnd <= l.taken {
		// Nothing was on offer. What still is keeps the time it has had.
		l.progress = now
	}
	l.offerEnd = l.taken + n
	l.trying = true
	return n, Effects{Deadline: l.deadline()}
}

// Took records that the write Take allowed has returned, and that the socket
// accepted n bytes of it. This is the only place the stall rule is decided:
// only a try of the socket can say that it takes nothing. A writer that has
// now written all it may gives the socket up here (Released), so that it need
// not ask again to be told so.
func (l *Ledger) Took(n int64, now Instant) Effects {
	l.trying = false
	if l.closed {
		return Effects{}
	}
	l.taken += n
	if n == 0 {
		if l.Stallable() && now-l.progress >= l.lim.StallTimeout {
			return Effects{Close: ErrStalled}
		}
		return Effects{Deadline: l.deadline()}
	}
	l.progress = now
	released, startPusher := l.taken >= l.writerLimit(), false
	if released {
		startPusher = l.release()
	}
	// One literal: an Effects filled in field by field costs each caller a
	// stalled load, which the Output's benchmarks showed.
	return Effects{StartPusher: startPusher, Signal: true, ShutWrite: l.Ended(), Released: released, Deadline: l.deadline()}
}

// writerLimit is where what the writer that has the socket may write ends: for
// a Requester where the stream ended when it took the socket, for a Pusher
// where it ends now.
func (l *Ledger) writerLimit() int64 {
	if l.writer == Requester {
		return l.seal
	}
	return l.end
}

// release takes the socket from the writer that has written all it may. What
// is still queued then lies behind a Requester's seal, so other connections
// pushed it: the Requester queues nothing while it has the socket. It goes to
// a Pusher, which release reports that the caller must start.
func (l *Ledger) release() (startPusher bool) {
	l.writer = NoWriter
	if l.end > l.taken {
		l.writer = Pusher
		return true
	}
	return false
}

// EndStream records that the stream to the peer ends once everything queued is
// written. It starts no writer: whoever has the socket, or takes it for what
// is queued, writes the last byte, and Took reports the end then. With nothing
// to write the stream ends here.
func (l *Ledger) EndStream() Effects {
	if l.closed || l.ending {
		return Effects{}
	}
	l.ending = true
	return Effects{ShutWrite: l.Ended()}
}

// Ended reports whether the stream to the peer has ended: EndStream was called
// and nothing is left to write. Held bytes are not in the stream.
func (l *Ledger) Ended() bool {
	return l.ending && !l.closed && l.end == l.taken
}

// Close records that the connection is closed and drops what waited for it.
// A closed Ledger grants nothing and returns no Effects.
func (l *Ledger) Close() Effects {
	if l.closed {
		return Effects{}
	}
	l.closed = true
	l.holding = false
	l.held = 0
	l.end = l.taken
	l.replyEnd = min(l.replyEnd, l.taken)
	l.writer = NoWriter
	return Effects{Signal: true}
}

// Gate reports whether the connection's requests may be read and run: no more
// than HighWater is left to write before its latest Reply is out. What was
// pushed after that Reply never closes the gate, so a Replica's
// acknowledgements are read however much of its stream waits.
func (l *Ledger) Gate() bool {
	return l.closed || l.aheadOfReply() <= l.lim.HighWater
}

// RepliesOut reports whether every Reply queued so far has been written. A
// connection that has run its last request is closed once it is true.
func (l *Ledger) RepliesOut() bool {
	return l.closed || l.taken >= l.replyEnd
}

// Full reports whether the connection's goroutine should write its Replies
// now and not wait for its next read.
func (l *Ledger) Full() bool {
	return !l.closed && l.aheadOfReply() >= l.lim.WriteThrough
}

// Waiting is how many bytes wait for the connection, held ones included. It
// is for INFO and for the Push budget, which counts it for a Subscriber. No
// decision about one connection is made from it outside this package.
func (l *Ledger) Waiting() int64 {
	return l.end - l.taken + l.held
}

// Budgeted is how many bytes the connection counts toward the Push budget,
// which bounds what waits for every Subscriber together: everything that waits
// for a Subscriber, and nothing for any other connection. A Replica has its own
// limit, and a connection that receives no Pushed message holds only Replies.
func (l *Ledger) Budgeted() int64 {
	if l.class != Subscriber {
		return 0
	}
	return l.Waiting()
}

// Writable reports whether a writer has anything to write, for the Event
// loop's write interest. Held bytes are not writable.
func (l *Ledger) Writable() bool {
	return !l.closed && l.end > l.taken
}

// Class reports what kind of connection this is.
func (l *Ledger) Class() Class { return l.class }

// Closed reports whether Close was called.
func (l *Ledger) Closed() bool { return l.closed }

func (l *Ledger) aheadOfReply() int64 {
	return max(l.replyEnd-l.taken, 0)
}

// grantPusher gives the socket to a Pusher when nobody has it and something
// was pushed behind the latest Reply, which no Requester will come to write.
func (l *Ledger) grantPusher() Effects {
	if l.writer != NoWriter || l.end <= max(l.taken, l.replyEnd) {
		return Effects{}
	}
	l.writer = Pusher
	return Effects{StartPusher: true}
}

func (l *Ledger) deadline() Instant {
	if !l.Stallable() || l.closed || l.offerEnd <= l.taken {
		return 0
	}
	return l.progress + l.lim.StallTimeout
}
