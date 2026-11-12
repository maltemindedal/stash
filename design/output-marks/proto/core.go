// Package proto is the pure core of the Output design in design/output-marks, cut
// down to byte counts so that a model checker can enumerate it. It has no
// lock, no clock, no socket and no goroutine: the shell in internal/server
// would hold its lock, read the clock, and do what its results say.
//
// Every rule about a connection's output is a function of Marks, five numbers
// with one meaning each. The rules are in rules.go, next to nothing else.
package proto

// Pos is a position in the bytes a connection is sent, counted from the first
// byte since it opened. Only bytes the peer can be sent now have one: a Pushed
// message held behind a Reply that has not come gets its position when that
// Reply releases it.
type Pos int64

// Time is a reading of the shell's clock, passed in. Nothing in the core reads
// a clock, so only the methods that take a Time can start or judge the stall
// rule: Take, Wrote, Tick and MarkReplica. Push and Reply cannot.
type Time int64

// Limits are the constants the rules are judged against.
type Limits struct {
	// HighWater is the most that may be left to write before the latest Reply
	// is out while the connection's next request is read (MayReadNext).
	HighWater int64
	// StallTimeout is how long a Replica's socket may take nothing of a batch
	// it was offered.
	StallTimeout Time
}

// Writer is who holds the batch [Written, Offered).
type Writer uint8

const (
	// NoWriter: nothing is on offer to the socket.
	NoWriter Writer = iota
	// Blocking is a writer that stays inside its write for as long as a batch
	// is on offer: the connection's goroutine or the pump.
	Blocking
	// Polled is a writer whose writes return at once and that comes back when
	// the socket can take more: the event loop.
	Polled
)

// Marks are where a connection's output stands, as positions. Invariant:
// Written <= Offered <= Queued, and ReplyEnd <= Queued. A closed stream has
// Written == Offered == Queued, ReplyEnd <= Written and Held == 0: what was
// dropped is not waiting for anything.
type Marks struct {
	// Written is how many bytes the socket has taken.
	Written Pos
	// Offered is the end of the batch a writer holds; Offered == Written when
	// none does.
	Offered Pos
	// Queued is the end of what can be written now.
	Queued Pos
	// ReplyEnd is where the latest Reply that had bytes ends.
	ReplyEnd Pos
	// Held is how many bytes wait behind a Reply that has not come.
	Held int64
}

// Verdict is what a step of the stall rule decided.
type Verdict uint8

const (
	// Keep the connection.
	Keep Verdict = iota
	// Stalled: close the connection with errReplicaStalled.
	Stalled
)

// Stream is the state of one connection's output.
type Stream struct {
	lim          Limits
	m            Marks
	holding      bool
	closed       bool
	replica      bool
	writer       Writer
	lastProgress Time
}

// NewStream returns the output of a connection that has just opened.
func NewStream(lim Limits) *Stream { return &Stream{lim: lim} }

// Marks reports where the output stands.
func (s *Stream) Marks() Marks { return s.m }

// Closed reports whether the stream was closed.
func (s *Stream) Closed() bool { return s.closed }

// Push queues n bytes another connection produced.
func (s *Stream) Push(n int) {
	if s.closed {
		return
	}
	if s.holding {
		s.m.Held += int64(n)
		return
	}
	s.m.Queued += Pos(n)
}

// HoldPushes makes what is pushed from now on wait for the next Reply.
func (s *Stream) HoldPushes() { s.holding = true }

// Reply queues the n bytes of the Replies to the request that just ran, which
// may be none, and releases what was held behind them.
func (s *Stream) Reply(n int) {
	if s.closed {
		return
	}
	s.m.Queued += Pos(n)
	if n > 0 {
		s.m.ReplyEnd = s.m.Queued
	}
	if s.holding {
		s.holding = false
		s.m.Queued += Pos(s.m.Held)
		s.m.Held = 0
	}
}

// MarkReplica holds the connection to the Replica rules. A batch already on
// offer starts the stall count now.
func (s *Stream) MarkReplica(now Time) {
	s.replica = true
	if s.m.Offered > s.m.Written {
		s.lastProgress = now
	}
}

// Take hands w the batch [Written, Queued) and reports whether it got one. Its
// end is fixed here: what is queued later is not part of it.
func (s *Stream) Take(w Writer, now Time) bool {
	if s.closed || s.writer != NoWriter || s.m.Queued == s.m.Written {
		return false
	}
	s.writer = w
	s.m.Offered = s.m.Queued
	if s.replica {
		s.lastProgress = now
	}
	return true
}

// Wrote records that the socket took n bytes of the batch on one attempt at
// now. An attempt by a Polled writer that takes nothing once the stall timeout
// has passed since the last progress is the stall rule's verdict: the socket
// has had no room for all that time.
func (s *Stream) Wrote(n int, now Time) Verdict {
	if s.writer == NoWriter {
		return Keep
	}
	if rest := int(s.m.Offered - s.m.Written); n > rest {
		n = rest
	}
	s.m.Written += Pos(n)
	switch {
	case n > 0:
		s.lastProgress = now
	case s.writer == Polled && s.replica && now-s.lastProgress >= s.lim.StallTimeout:
		s.Close()
		return Stalled
	}
	if s.m.Written == s.m.Offered {
		s.writer = NoWriter
	}
	return Keep
}

// Tick is the stall rule's timer at now. Only a Blocking writer can be judged
// here: it has been inside one write since lastProgress. A Polled writer may
// have been away, so the shell asks it for an attempt instead, and Wrote gives
// the verdict.
func (s *Stream) Tick(now Time) Verdict {
	if !s.StallClockRuns() || now-s.lastProgress < s.lim.StallTimeout {
		return Keep
	}
	if s.writer == Blocking {
		s.Close()
		return Stalled
	}
	return Keep
}

// StallClockRuns reports whether the stall rule is counting: the connection is
// a Replica's and a batch is on offer. A closed stream offers nothing.
func (s *Stream) StallClockRuns() bool {
	return s.replica && s.m.Offered > s.m.Written
}

// LastProgress is when the socket last took a byte, or was offered the batch.
func (s *Stream) LastProgress() Time { return s.lastProgress }

// Close drops everything that waits. The marks then say nothing waits, so no
// rule can find work, a stall, or a Reply to wait for on a closed stream.
func (s *Stream) Close() {
	s.closed = true
	s.holding = false
	s.writer = NoWriter
	w := s.m.Written
	s.m = Marks{Written: w, Offered: w, Queued: w, ReplyEnd: min(s.m.ReplyEnd, w)}
}
