//go:build !race

package ledger

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// The model: a Ledger with limits of a few bytes, driven by every sequence of
// events a connection can see, in each networking mode. Beside it runs a plain
// restatement of the rules, the world, which keeps the waiting bytes as a
// string of tags ('r' a Reply's byte, 'p' a Pushed message's) and no
// positions. After every event the Ledger's decisions must equal what the
// tags say.
//
// The walk is not built under the race detector: it takes about ten times as
// long there, and the test uses one goroutine.

var modelLimits = Limits{
	HighWater:        2,
	WriteThrough:     2,
	Chunk:            2,
	PushLimit:        5,
	ReplicaPushLimit: 5,
	StallTimeout:     2,
}

// modelLimitsFor are the limits of a connection in one networking mode. Only
// the Event loop's has a Reply limit: a connection's own goroutine waits on
// the socket instead of queueing a Reply behind that much.
func modelLimitsFor(eventLoop bool) Limits {
	limits := modelLimits
	if eventLoop {
		limits.ReplyLimit = 6
	}
	return limits
}

// modelRepliesWaiting bounds the model: no Reply is queued above it.
const modelRepliesWaiting = 7

// machine is a Ledger, or a Ledger with one rule replaced by the rule a past
// commit had.
type machine interface {
	Reply(n int64) Effects
	Push(n int64) (bool, Effects)
	Hold()
	Holding() bool
	MarkReplica(now Instant) Effects
	Finish(now Instant) Effects
	Stallable() bool
	Take(role Role, now Instant) (int64, Effects)
	Took(n int64, now Instant) Effects
	EndStream() Effects
	Ended() bool
	Close() Effects
	Gate() bool
	RepliesOut() bool
	Waiting() int64
	Budgeted() int64
	Writable() bool
	Full() bool
	Class() Class
	Closed() bool
	base() *Ledger
	clone() machine
}

func (l *Ledger) base() *Ledger  { return l }
func (l *Ledger) clone() machine { c := *l; return &c }

// timed is a machine whose stall rule is a timer's, as it was before this
// design: the model fires the timer after every tick of the clock.
type timed interface {
	timerFired(now Instant) Effects
}

// clocked is a machine that reads the clock when output is queued.
type clocked interface {
	setNow(now Instant)
}

// keyed is a machine with state of its own.
type keyed interface {
	key(now Instant) string
}

type world struct {
	m   machine
	now Instant

	wire, held      string
	holding, closed bool
	// ending is set once the stream was to end.
	ending bool
	// replica is set once the connection was marked a Replica's, and
	// subscriber once a Pushed message was accepted for one that is not.
	replica, subscriber bool
	// finishing is set once the connection has run its last request.
	finishing bool

	// grant is the size of the write in progress, and grantRole who makes it.
	grant     int64
	grantRole Role
	// requesterHolds is set while the Requester has the socket, and sealLeft
	// is how much of what was queued when it took the socket is unwritten.
	requesterHolds bool
	sealLeft       int64
	// offerLeft is how much of what the socket was last offered it has not
	// taken, and since is when it last took a byte of that or was first
	// offered it.
	offerLeft int64
	since     Instant
	// deadline is the latest Effects.Deadline.
	deadline Instant
}

// told is the time the Ledger is told with a Take or a Took. The Output reads
// its clock only for a connection whose socket is held to the stall rule, so
// any other Ledger is told a time that is wrong, and must decide the same.
func (w world) told() Instant {
	if !w.m.Stallable() {
		return 1
	}
	return w.now
}

func (w world) copy() world {
	w.m = w.m.clone()
	return w
}

// stallRule reports whether the connection's socket is held to the stall rule:
// a Replica's, and that of a connection that is finishing.
func (w world) stallRule() bool {
	return w.replica || w.finishing
}

func (w world) aheadOfReply() int64 {
	return int64(strings.LastIndexByte(w.wire, 'r') + 1)
}

func (w world) key() string {
	l := w.m.base()
	clamp := func(v, low, high int64) int64 { return max(low, min(v, high)) }
	timeout := int64(l.lim.StallTimeout)
	deadline := int64(-2)
	if w.deadline != 0 {
		deadline = clamp(int64(w.deadline-w.now), -1, timeout)
	}
	extra := ""
	if k, ok := w.m.(keyed); ok {
		extra = k.key(w.now)
	}
	return fmt.Sprint(
		l.class, l.closed, l.holding, l.ending, w.ending, l.finishing, w.finishing, l.end-l.taken, max(l.replyEnd-l.taken, 0), l.held,
		l.writer, max(l.seal-l.taken, 0), max(l.offerEnd-l.taken, 0), l.trying, clamp(int64(w.now-l.progress), 0, timeout+1),
		"|", w.wire, "|", w.held, w.holding, w.closed, w.replica, w.subscriber, w.grant, w.grantRole, w.requesterHolds, w.sealLeft,
		w.offerLeft, clamp(int64(w.now-w.since), 0, timeout+1), deadline, extra)
}

type eventKind uint8

const (
	queued eventKind = iota
	tookBytes
	timerFired
)

// settle carries out the Effects of one event the way the Output does, and
// checks every rule against the world. before is the world the event found.
// It returns the first rule that does not hold, or "".
func (w *world) settle(before world, kind eventKind, took int64, effects Effects) string {
	if before.closed && effects != (Effects{}) {
		return fmt.Sprintf("a closed Ledger returned %+v: nothing may be started, armed or signalled for a connection that is closed", effects)
	}

	timeout := w.m.base().lim.StallTimeout
	onOffer := w.stallRule() && w.offerLeft > 0
	switch {
	case effects.Deadline != 0 && !onOffer:
		return fmt.Sprintf("the deadline %d was set with %d bytes on offer, the connection a Replica's = %v and finishing = %v: only the socket of a Replica or of a connection that is finishing can stall, while it is offered bytes", effects.Deadline, w.offerLeft, w.replica, w.finishing)
	case effects.Deadline != 0 && effects.Deadline != w.since+timeout:
		return fmt.Sprintf("the deadline %d was set, want %d: the stall timeout of %d after the socket last took a byte or was first offered one, at %d", effects.Deadline, w.since+timeout, timeout, w.since)
	case kind == tookBytes && onOffer && effects.Deadline == 0 && effects.Close == nil && !before.closed:
		return fmt.Sprintf("a write that left %d bytes on offer to a Replica returned with no deadline: nothing would try its socket again", w.offerLeft)
	}
	if effects.StartPusher && !strings.Contains(w.wire, "p") {
		return fmt.Sprintf("a Pusher was started with %q writable and no pushed message in it: the connection's goroutine writes its own Replies", w.wire)
	}

	stalledTry := kind == tookBytes && took == 0
	overdue := before.stallRule() && before.offerLeft > 0 && w.now-before.since >= timeout
	if errors.Is(effects.Close, ErrStalled) {
		tryInProgress := kind == timerFired && before.grant > 0
		switch {
		case !before.stallRule():
			return "a connection that is neither a Replica's nor finishing was closed as stalled"
		case !stalledTry && !tryInProgress:
			return "the Replica was closed as stalled with no try of its socket that took nothing: only a writer that tried the socket can say it takes nothing"
		case !overdue:
			return fmt.Sprintf("the Replica was closed as stalled %d after its socket last took a byte or was first offered one, before the stall timeout of %d", w.now-before.since, timeout)
		}
	} else if stalledTry && overdue && !before.closed {
		return "a try of a Replica's socket took nothing, the stall timeout after it last took a byte, and the Replica was not closed"
	}

	signalled := effects.Signal
	if effects.Close != nil {
		signalled = w.m.Close().Signal || signalled
		w.closed, w.wire, w.held, w.holding = true, "", "", false
	}

	ended := w.ending && !w.closed && w.wire == ""
	if wasEnded := before.ending && !before.closed && before.wire == ""; effects.ShutWrite != (ended && !wasEnded) {
		return fmt.Sprintf("ShutWrite = %v with the stream to end = %v and %q left to write, where %q was left before: the write side is shut once, when the last byte queued for a stream that is to end is written", effects.ShutWrite, w.ending, w.wire, before.wire)
	}
	if got := w.m.Ended(); got != ended {
		return fmt.Sprintf("Ended() = %v with the stream to end = %v and %q left to write", got, w.ending, w.wire)
	}
	if got := w.m.Stallable(); got != w.stallRule() {
		return fmt.Sprintf("Stallable() = %v for a connection that is a Replica's = %v and finishing = %v: the Output reads its clock for the sockets held to the stall rule and for no other", got, w.replica, w.finishing)
	}
	if got := w.m.Holding(); got != w.holding {
		return fmt.Sprintf("Holding() = %v, want %v: a Pushed message would go to the wrong side of the next Reply", got, w.holding)
	}
	if got := w.m.Closed(); got != w.closed {
		return fmt.Sprintf("Closed() = %v, want %v", got, w.closed)
	}
	class := Plain
	switch {
	case w.replica:
		class = Replica
	case w.subscriber:
		class = Subscriber
	}
	if got := w.m.Class(); got != class {
		return fmt.Sprintf("Class() = %d, want %d: marked a Replica's = %v, a Pushed message accepted before that = %v", got, class, w.replica, w.subscriber)
	}
	budgeted := int64(0)
	if w.subscriber && !w.closed {
		budgeted = int64(len(w.wire) + len(w.held))
	}
	if got := w.m.Budgeted(); got != budgeted {
		return fmt.Sprintf("Budgeted() = %d, want %d: the Push budget counts everything that waits for a subscriber, here %q queued and %q held, and nothing for a Replica, a connection that was pushed nothing, or a closed one", got, budgeted, w.wire, w.held)
	}
	if w.closed {
		if !w.m.Gate() || !w.m.RepliesOut() {
			return "a closed connection still makes its goroutine wait"
		}
		if w.m.Waiting() != 0 || w.m.Writable() || w.m.Full() {
			return fmt.Sprintf("a closed connection has Waiting() = %d, Writable() = %v, Full() = %v: what waited for it was dropped", w.m.Waiting(), w.m.Writable(), w.m.Full())
		}
		return ""
	}
	if got, want := w.m.Waiting(), int64(len(w.wire)+len(w.held)); got != want {
		return fmt.Sprintf("Waiting() = %d with %q queued and %q held", got, w.wire, w.held)
	}
	if got, want := w.m.Writable(), w.wire != ""; got != want {
		return fmt.Sprintf("Writable() = %v with %q queued and %q held", got, w.wire, w.held)
	}
	ahead := w.aheadOfReply()
	if got, want := w.m.Gate(), ahead <= w.m.base().lim.HighWater; got != want {
		return fmt.Sprintf("Gate() = %v with %q waiting: %d bytes are left to write before the latest reply is out, and the high-water mark is %d", got, w.wire, ahead, w.m.base().lim.HighWater)
	}
	if got, want := w.m.RepliesOut(), ahead == 0; got != want {
		return fmt.Sprintf("RepliesOut() = %v with %q waiting, of which %d bytes are left before the latest reply is out", got, w.wire, ahead)
	}
	if got, want := w.m.Full(), ahead >= w.m.base().lim.WriteThrough; got != want {
		return fmt.Sprintf("Full() = %v with %q waiting, of which %d bytes are left before the latest reply is out", got, w.wire, ahead)
	}
	if strings.Contains(w.wire, "p") && w.m.base().writer == NoWriter {
		return fmt.Sprintf("%q waits with a pushed message in it and nobody has the socket: no Requester comes to write a push", w.wire)
	}
	if !signalled && (!before.m.Gate() && w.m.Gate() || !before.m.RepliesOut() && w.m.RepliesOut()) {
		return "the gate opened, or the last reply went out, and nobody was signalled"
	}
	return ""
}

type step struct {
	label     string
	next      world
	violation string
}

// take is a writer asking for the socket.
func (w world) take(role Role, name string) step {
	next := w.copy()
	n, effects := next.m.Take(role, next.told())
	label := fmt.Sprintf("the %s asks for the socket and is given %d", name, n)
	if n == 0 {
		if role == Requester {
			next.requesterHolds = false
		}
		return step{label, next, next.settle(w, queued, 0, effects)}
	}
	if role == Requester {
		if !next.requesterHolds {
			next.requesterHolds, next.sealLeft = true, int64(len(w.wire))
		}
		if n > next.sealLeft {
			return step{label, next, fmt.Sprintf("the connection's goroutine was given %d bytes to write with %d left of what was queued when its flush began: it is writing what other connections pushed meanwhile", n, next.sealLeft)}
		}
	}
	if n > int64(len(w.wire)) {
		return step{label, next, fmt.Sprintf("a writer was given %d bytes with %d writable: what is held behind a reply was offered to the socket", n, len(w.wire))}
	}
	if next.offerLeft == 0 {
		next.since = next.now
	}
	next.grant, next.grantRole, next.offerLeft, next.deadline = n, role, n, effects.Deadline
	want := Instant(0)
	if next.stallRule() {
		want = next.since + modelLimits.StallTimeout
	}
	if effects.Deadline != want {
		return step{label, next, fmt.Sprintf("the write was given the deadline %d, want %d", effects.Deadline, want)}
	}
	return step{label, next, next.settle(w, queued, 0, effects)}
}

// steps returns every event that can happen next. eventLoop selects the
// networking mode: who writes, and whether a write can outlast a tick.
func (w world) steps(eventLoop bool) []step {
	var out []step
	writing := w.grant > 0

	// The connection's own goroutine replies, holds and writes as the
	// Requester, one after the other: while it has the socket it does neither
	// of the first two.
	// A connection that is finishing has run its last request.
	if !w.requesterHolds && !w.finishing {
		for _, n := range []int64{0, 1, 3} {
			if int64(len(w.wire)+len(w.held))+n > modelRepliesWaiting {
				continue
			}
			next := w.copy()
			effects := next.m.Reply(n)
			label := fmt.Sprintf("reply of %d", n)
			if !next.closed {
				// What the Reply releases follows it and does not count.
				ahead, limit := int64(len(w.wire)), w.m.base().lim.ReplyLimit
				if n > 0 {
					ahead += n
				} else {
					ahead = w.aheadOfReply()
				}
				if over := limit > 0 && ahead > limit; errors.Is(effects.Close, ErrReplyLimit) != over {
					out = append(out, step{label, next, fmt.Sprintf("the Reply closed the connection for its Reply limit = %v with %d bytes left to write before the latest reply is out, %d held behind it, and a limit of %d", !over, ahead, len(w.held), limit)})
					continue
				}
				next.wire += strings.Repeat("r", int(n)) + next.held
				next.held, next.holding = "", false
			}
			out = append(out, step{label, next, next.settle(w, queued, 0, effects)})
		}
		if !w.holding && !w.closed {
			next := w.copy()
			next.m.Hold()
			next.holding = true
			out = append(out, step{"hold pushes", next, next.settle(w, queued, 0, Effects{})})
		}
	}

	for _, n := range []int64{1, 3} {
		next := w.copy()
		accepted, effects := next.m.Push(n)
		label := fmt.Sprintf("push of %d", n)
		if !w.closed {
			waiting, limit := int64(len(w.wire)+len(w.held))+n, modelLimits.PushLimit
			if w.replica {
				waiting, limit = waiting-w.aheadOfReply(), modelLimits.ReplicaPushLimit
			}
			if accepted != (waiting <= limit) {
				out = append(out, step{label, next, fmt.Sprintf("Push accepted = %v with %d bytes counting against a limit of %d", accepted, waiting, limit)})
				continue
			}
			switch {
			case !accepted && !errors.Is(effects.Close, ErrPushLimit):
				out = append(out, step{label, next, "a Push past the limit did not close the connection"})
				continue
			case accepted && w.holding:
				next.held += strings.Repeat("p", int(n))
				next.subscriber = !next.replica
			case accepted:
				next.wire += strings.Repeat("p", int(n))
				next.subscriber = !next.replica
			}
		}
		out = append(out, step{label, next, next.settle(w, queued, 0, effects)})
	}

	if !w.replica {
		next := w.copy()
		effects := next.m.MarkReplica(next.now)
		if !next.closed {
			if next.offerLeft > 0 && !next.stallRule() {
				next.since = next.now
			}
			next.replica, next.subscriber = true, false
			next.deadline = effects.Deadline
		}
		violation := next.settle(w, queued, 0, effects)
		if violation == "" && !next.closed && next.offerLeft > 0 && effects.Deadline == 0 {
			violation = fmt.Sprintf("a connection with %d bytes on offer was marked a Replica's with no deadline: nothing would try its socket again", next.offerLeft)
		}
		out = append(out, step{"mark as a replica", next, violation})
	}

	// The connection has run its last request. Its goroutine is not in the
	// middle of a flush then.
	if !w.finishing && !w.requesterHolds {
		next := w.copy()
		effects := next.m.Finish(next.now)
		if !next.closed {
			if next.offerLeft > 0 && !next.stallRule() {
				next.since = next.now
			}
			next.finishing = true
			next.deadline = effects.Deadline
		}
		violation := next.settle(w, queued, 0, effects)
		if violation == "" && !next.closed && next.offerLeft > 0 && effects.Deadline == 0 {
			violation = fmt.Sprintf("a connection with %d bytes on offer began to finish with no deadline: a peer that takes none of them would keep it for good", next.offerLeft)
		}
		out = append(out, step{"finish", next, violation})
	}

	if !writing {
		if !eventLoop {
			out = append(out, w.take(Requester, "connection's goroutine"))
		}
		if eventLoop || w.m.base().writer == Pusher {
			name := "pump"
			if eventLoop {
				name = "event loop"
			}
			out = append(out, w.take(Pusher, name))
		}
	}

	if writing {
		// Whoever asks for the socket during a write is given nothing, and
		// asking changes nothing: the connection's goroutine finds the pump
		// writing, or a writer asks again before it has reported its write.
		for _, role := range []Role{Requester, Pusher} {
			if eventLoop && role == Requester {
				continue
			}
			next := w.copy()
			n, effects := next.m.Take(role, next.told())
			if n != 0 || effects != (Effects{}) || next.key() != w.key() {
				out = append(out, step{"the socket is asked for during a write", next, fmt.Sprintf("Take(%d) during a write of %d bytes by %d returned %d and %+v: the bytes on offer would be offered twice", role, w.grant, w.grantRole, n, effects)})
			}
		}

		// A blocking write returns short only at its deadline. The Event
		// loop's returns at once with whatever the socket took.
		least := w.grant
		if eventLoop || w.deadline != 0 && w.now >= w.deadline {
			least = 0
		}
		for took := least; took <= w.grant; took++ {
			next := w.copy()
			effects := next.m.Took(took, next.told())
			if !next.closed {
				next.wire = next.wire[took:]
			}
			next.offerLeft -= took
			if took > 0 {
				next.since = next.now
			}
			if next.grantRole == Requester {
				next.sealLeft -= took
			}
			next.grant, next.deadline = 0, effects.Deadline
			violation := next.settle(w, tookBytes, took, effects)
			// A writer that has written all it may is told so by the write that
			// did it, and no longer has the socket.
			done := !next.closed && took > 0 && (next.grantRole == Requester && next.sealLeft == 0 || next.grantRole == Pusher && next.wire == "")
			switch {
			case violation != "":
			case effects.Released && !done:
				violation = fmt.Sprintf("a writer was told that it has written all it may, with %q left to write and %d of that its own", next.wire, next.sealLeft)
			case !effects.Released && done && next.grantRole == Requester:
				violation = fmt.Sprintf("the connection's goroutine keeps the socket after it wrote all that was queued when its flush began, with %q queued since: it is writing what other connections pushed meanwhile", next.wire)
			case !effects.Released && done:
				violation = "a writer that has written everything was not told so: it keeps a socket it has nothing to write to"
			}
			if effects.Released && next.grantRole == Requester {
				next.requesterHolds = false
			}
			out = append(out, step{fmt.Sprintf("the socket takes %d of the %d it was offered", took, w.grant), next, violation})
		}
	}

	if !eventLoop || !writing {
		next := w.copy()
		next.now++
		label := "the clock advances by 1"
		violation := ""
		if c, ok := next.m.(clocked); ok {
			c.setNow(next.now)
		}
		if t, ok := next.m.(timed); ok {
			label += " and the stall timer fires"
			violation = next.settle(w, timerFired, 0, t.timerFired(next.now))
		}
		out = append(out, step{label, next, violation})
	}

	if !w.ending {
		next := w.copy()
		effects := next.m.EndStream()
		if !next.closed {
			next.ending = true
		}
		out = append(out, step{"end the stream", next, next.settle(w, queued, 0, effects)})
	}

	if !w.closed {
		next := w.copy()
		effects := next.m.Close()
		next.closed, next.wire, next.held, next.holding = true, "", "", false
		out = append(out, step{"close", next, next.settle(w, queued, 0, effects)})
	}
	return out
}

// explore walks every world reachable from a new connection, breadth first,
// and returns how many it found, and the shortest sequence of events that
// breaks a rule with the rule it breaks. With a bound above zero it stops
// once it has found that many.
func explore(start machine, eventLoop bool, bound int) (states int, trace []string, violation string) {
	type node struct {
		w      world
		parent int
		label  string
	}
	if c, ok := start.(clocked); ok {
		c.setNow(1)
	}
	nodes := []node{{w: world{m: start, now: 1}, parent: -1}}
	seen := map[string]bool{nodes[0].w.key(): true}
	for at := 0; at < len(nodes) && (bound <= 0 || len(nodes) < bound); at++ {
		for _, s := range nodes[at].w.steps(eventLoop) {
			if s.violation != "" {
				trace = []string{s.label}
				for i := at; nodes[i].parent >= 0; i = nodes[i].parent {
					trace = append([]string{nodes[i].label}, trace...)
				}
				return len(nodes), trace, s.violation
			}
			if c, ok := s.next.m.(clocked); ok {
				c.setNow(s.next.now)
			}
			key := s.next.key()
			if seen[key] {
				continue
			}
			seen[key] = true
			nodes = append(nodes, node{w: s.next, parent: at, label: s.label})
		}
	}
	return len(nodes), nil, ""
}

var networkingModes = []struct {
	name      string
	eventLoop bool
}{
	{name: "a goroutine for each connection"},
	{name: "the event loop", eventLoop: true},
}

func TestEveryReachableStateOfTheLedgerKeepsItsRules(t *testing.T) {
	for _, mode := range networkingModes {
		mode := mode
		t.Run(mode.name, func(t *testing.T) {
			ledger := New(modelLimitsFor(mode.eventLoop))
			states, trace, violation := explore(&ledger, mode.eventLoop, 0)
			if violation != "" {
				t.Fatalf("after\n  %s\n%s", strings.Join(trace, "\n  "), violation)
			}
			t.Logf("%d states, every rule holds in each", states)
		})
	}
}

// called is a Ledger that counts the calls of each of its exported methods.
type called struct {
	*Ledger
	calls map[string]int
}

func (c *called) clone() machine {
	l := *c.Ledger
	return &called{Ledger: &l, calls: c.calls}
}
func (c *called) Reply(n int64) Effects { c.calls["Reply"]++; return c.Ledger.Reply(n) }
func (c *called) Push(n int64) (bool, Effects) {
	c.calls["Push"]++
	return c.Ledger.Push(n)
}
func (c *called) Hold()           { c.calls["Hold"]++; c.Ledger.Hold() }
func (c *called) Holding() bool   { c.calls["Holding"]++; return c.Ledger.Holding() }
func (c *called) Stallable() bool { c.calls["Stallable"]++; return c.Ledger.Stallable() }
func (c *called) Finish(now Instant) Effects {
	c.calls["Finish"]++
	return c.Ledger.Finish(now)
}
func (c *called) MarkReplica(now Instant) Effects {
	c.calls["MarkReplica"]++
	return c.Ledger.MarkReplica(now)
}
func (c *called) Take(role Role, now Instant) (int64, Effects) {
	c.calls["Take"]++
	return c.Ledger.Take(role, now)
}
func (c *called) Took(n int64, now Instant) Effects {
	c.calls["Took"]++
	return c.Ledger.Took(n, now)
}
func (c *called) EndStream() Effects { c.calls["EndStream"]++; return c.Ledger.EndStream() }
func (c *called) Ended() bool        { c.calls["Ended"]++; return c.Ledger.Ended() }
func (c *called) Close() Effects     { c.calls["Close"]++; return c.Ledger.Close() }
func (c *called) Gate() bool         { c.calls["Gate"]++; return c.Ledger.Gate() }
func (c *called) RepliesOut() bool   { c.calls["RepliesOut"]++; return c.Ledger.RepliesOut() }
func (c *called) Full() bool         { c.calls["Full"]++; return c.Ledger.Full() }
func (c *called) Waiting() int64     { c.calls["Waiting"]++; return c.Ledger.Waiting() }
func (c *called) Budgeted() int64    { c.calls["Budgeted"]++; return c.Ledger.Budgeted() }
func (c *called) Writable() bool     { c.calls["Writable"]++; return c.Ledger.Writable() }
func (c *called) Class() Class       { c.calls["Class"]++; return c.Ledger.Class() }
func (c *called) Closed() bool       { c.calls["Closed"]++; return c.Ledger.Closed() }

// TestTheModelCallsEveryExportedMethodOfTheLedger fails when the Ledger has
// an exported method the model does not know: a decision added to ledger.go
// that nothing compares with the bytes. The method goes into the machine
// interface, a rule about it into settle or steps, and its count into called.
func TestTheModelCallsEveryExportedMethodOfTheLedger(t *testing.T) {
	ledger := New(modelLimits)
	counted := &called{Ledger: &ledger, calls: map[string]int{}}
	for _, mode := range networkingModes {
		if _, trace, violation := explore(counted, mode.eventLoop, 5000); violation != "" {
			t.Fatalf("after\n  %s\n%s", strings.Join(trace, "\n  "), violation)
		}
	}

	modelled := reflect.TypeOf((*machine)(nil)).Elem()
	exported := reflect.TypeOf(&Ledger{})
	if exported.NumMethod() == 0 {
		t.Fatal("reflection found no exported method on *Ledger")
	}
	for i := 0; i < exported.NumMethod(); i++ {
		name := exported.Method(i).Name
		if _, ok := modelled.MethodByName(name); !ok {
			t.Errorf("Ledger.%s is not in the model's machine interface: the model cannot check it", name)
			continue
		}
		if counted.calls[name] == 0 {
			t.Errorf("the model never called Ledger.%s in its first 5000 states of either networking mode, or the called machine does not count it", name)
		}
	}
}

// The machines below are the Ledger with one rule put back to what the parent
// of a fix commit had. Each must be rejected.

// gateCountsEverythingWaiting is Flush before ca505a9, and the Event loop at
// 93c4c76 (eventloop.go: PendingOutputBytes() > eventLoopOutputHighWater):
// requests are held back while more than the high-water mark waits, pushed
// output included.
type gateCountsEverythingWaiting struct{ Ledger }

func (m *gateCountsEverythingWaiting) clone() machine { c := *m; return &c }
func (m *gateCountsEverythingWaiting) Gate() bool {
	return m.closed || m.Waiting() <= m.lim.HighWater
}

// gateTakesWrittenBytesForReplyBytes is ca505a9, which 5a28666 replaced: a
// count of unwritten Reply bytes that every written byte is taken from.
type gateTakesWrittenBytesForReplyBytes struct {
	Ledger
	replyWaiting int64
}

func (m *gateTakesWrittenBytesForReplyBytes) clone() machine { c := *m; return &c }
func (m *gateTakesWrittenBytesForReplyBytes) key(Instant) string {
	return fmt.Sprint(m.replyWaiting)
}
func (m *gateTakesWrittenBytesForReplyBytes) Reply(n int64) Effects {
	if !m.closed {
		m.replyWaiting += n
	}
	return m.Ledger.Reply(n)
}
func (m *gateTakesWrittenBytesForReplyBytes) Took(n int64, now Instant) Effects {
	m.replyWaiting = max(m.replyWaiting-n, 0)
	return m.Ledger.Took(n, now)
}
func (m *gateTakesWrittenBytesForReplyBytes) Close() Effects {
	m.replyWaiting = 0
	return m.Ledger.Close()
}
func (m *gateTakesWrittenBytesForReplyBytes) Gate() bool {
	return m.closed || m.replyWaiting <= m.lim.HighWater
}

// drainLeavesAtTheHighWaterMark is the handler before 721a457, which ended
// with Flush: it left once no more than the high-water mark was unwritten.
type drainLeavesAtTheHighWaterMark struct{ Ledger }

func (m *drainLeavesAtTheHighWaterMark) clone() machine { c := *m; return &c }
func (m *drainLeavesAtTheHighWaterMark) RepliesOut() bool {
	return m.closed || m.aheadOfReply() <= m.lim.HighWater
}

// stallCountsFromTheFirstWaitingByte is the stall rule before 22d6ba9 and
// 1bc2076: a timer that counts from the first byte that waits, held or
// unflushed, and from every byte written.
type stallCountsFromTheFirstWaitingByte struct {
	Ledger
	now, lastProgress Instant
}

func (m *stallCountsFromTheFirstWaitingByte) clone() machine { c := *m; return &c }
func (m *stallCountsFromTheFirstWaitingByte) setNow(now Instant) {
	m.now = now
}
func (m *stallCountsFromTheFirstWaitingByte) key(now Instant) string {
	return fmt.Sprint(min(now-m.lastProgress, m.lim.StallTimeout+1))
}
func (m *stallCountsFromTheFirstWaitingByte) began(before int64) {
	if m.class == Replica && before == 0 && m.Waiting() > 0 {
		m.lastProgress = m.now
	}
}
func (m *stallCountsFromTheFirstWaitingByte) Reply(n int64) Effects {
	before := m.Waiting()
	effects := m.Ledger.Reply(n)
	m.began(before)
	return effects
}
func (m *stallCountsFromTheFirstWaitingByte) Push(n int64) (bool, Effects) {
	before := m.Waiting()
	accepted, effects := m.Ledger.Push(n)
	m.began(before)
	return accepted, effects
}
func (m *stallCountsFromTheFirstWaitingByte) MarkReplica(now Instant) Effects {
	m.lastProgress = now
	return m.Ledger.MarkReplica(now)
}
func (m *stallCountsFromTheFirstWaitingByte) Took(n int64, now Instant) Effects {
	if n > 0 {
		m.lastProgress = now
	}
	return m.Ledger.Took(n, now)
}
func (m *stallCountsFromTheFirstWaitingByte) timerFired(now Instant) Effects {
	if m.class == Replica && !m.closed && m.Waiting() > 0 && now-m.lastProgress >= m.lim.StallTimeout {
		return Effects{Close: ErrStalled}
	}
	return Effects{}
}

// stallTimerDoesNotAskTheWriter is the stall rule of 1bc2076, which c8a0832
// replaced: a timer that closes a Replica when a batch has been on offer for
// the stall timeout with no byte taken, whether or not anything tried the
// socket in that time.
type stallTimerDoesNotAskTheWriter struct{ Ledger }

func (m *stallTimerDoesNotAskTheWriter) clone() machine { c := *m; return &c }
func (m *stallTimerDoesNotAskTheWriter) timerFired(now Instant) Effects {
	if m.class == Replica && !m.closed && m.offerEnd > m.taken && now-m.progress >= m.lim.StallTimeout {
		return Effects{Close: ErrStalled}
	}
	return Effects{}
}

// flushWritesWhatWasPushedDuringIt is Flush before 878dcef: the connection's
// goroutine goes on to write what was queued after it took the socket. When a
// write returns, what it may write ends where the stream ends then.
type flushWritesWhatWasPushedDuringIt struct{ Ledger }

func (m *flushWritesWhatWasPushedDuringIt) clone() machine { c := *m; return &c }
func (m *flushWritesWhatWasPushedDuringIt) Took(n int64, now Instant) Effects {
	if m.writer == Requester {
		m.seal = m.end
	}
	return m.Ledger.Took(n, now)
}

// replyLimitCountsEverythingWaiting is ConnMachine.bufferResponses before the
// limit became the Ledger's: a Reply closes the connection when more than the
// limit waits for it in all, what was pushed behind the Reply included.
type replyLimitCountsEverythingWaiting struct{ Ledger }

func (m *replyLimitCountsEverythingWaiting) clone() machine { c := *m; return &c }
func (m *replyLimitCountsEverythingWaiting) Reply(n int64) Effects {
	limit := m.lim.ReplyLimit
	m.lim.ReplyLimit = 0
	effects := m.Ledger.Reply(n)
	m.lim.ReplyLimit = limit
	if !m.closed && m.Waiting() > limit {
		return Effects{Close: ErrReplyLimit}
	}
	return effects
}

// markReplicaCountsForAClosedOutput is MarkReplica before 145fa8b: it starts
// the stall count for the batch a closed connection's writer left unsent.
type markReplicaCountsForAClosedOutput struct{ Ledger }

func (m *markReplicaCountsForAClosedOutput) clone() machine { c := *m; return &c }
func (m *markReplicaCountsForAClosedOutput) MarkReplica(now Instant) Effects {
	if m.closed && m.offerEnd > m.taken {
		return Effects{Deadline: now + m.lim.StallTimeout}
	}
	return m.Ledger.MarkReplica(now)
}

func TestTheModelRejectsEachRuleAPastCommitHadToReplace(t *testing.T) {
	tests := []struct {
		name string
		// replacedBy is the commit that replaced the rule.
		replacedBy string
		eventLoop  bool
		machine    func() machine
		// breaks is part of the rule the model must report as broken.
		breaks string
	}{
		{
			name:       "requests are held back for everything that waits",
			replacedBy: "ca505a9, and still the event loop's rule at 93c4c76",
			machine:    func() machine { return &gateCountsEverythingWaiting{New(modelLimits)} },
			breaks:     "Gate() = false",
		},
		{
			name:       "requests are held back for everything that waits, on the event loop",
			replacedBy: "nothing yet: eventloop.go at 93c4c76",
			eventLoop:  true,
			machine:    func() machine { return &gateCountsEverythingWaiting{New(modelLimits)} },
			breaks:     "Gate() = false",
		},
		{
			name:       "every written byte counts as a byte of a reply",
			replacedBy: "5a28666",
			machine:    func() machine { return &gateTakesWrittenBytesForReplyBytes{Ledger: New(modelLimits)} },
			breaks:     "Gate() = true",
		},
		{
			name:       "the handler leaves once the high-water mark is reached",
			replacedBy: "721a457",
			machine:    func() machine { return &drainLeavesAtTheHighWaterMark{New(modelLimits)} },
			breaks:     "RepliesOut() = true",
		},
		{
			name:       "the stall rule counts from the first byte that waits",
			replacedBy: "22d6ba9 and 1bc2076",
			machine:    func() machine { return &stallCountsFromTheFirstWaitingByte{Ledger: New(modelLimits)} },
			breaks:     "no try of its socket that took nothing",
		},
		{
			name:       "the stall timer closes a replica the event loop did not try",
			replacedBy: "c8a0832",
			eventLoop:  true,
			machine:    func() machine { return &stallTimerDoesNotAskTheWriter{New(modelLimits)} },
			breaks:     "no try of its socket that took nothing",
		},
		{
			name:       "a flush writes what was pushed during it",
			replacedBy: "878dcef",
			machine:    func() machine { return &flushWritesWhatWasPushedDuringIt{New(modelLimits)} },
			breaks:     "writing what other connections pushed meanwhile",
		},
		{
			name:       "a closed connection marked a replica's starts a stall count",
			replacedBy: "145fa8b",
			machine:    func() machine { return &markReplicaCountsForAClosedOutput{New(modelLimits)} },
			breaks:     "a closed Ledger returned",
		},
		{
			name:       "the event loop's reply limit counts everything that waits",
			replacedBy: "the commit that made the limit the Ledger's (conn_machine.go before it)",
			eventLoop:  true,
			machine:    func() machine { return &replyLimitCountsEverythingWaiting{New(modelLimitsFor(true))} },
			breaks:     "the Reply closed the connection for its Reply limit = true",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			states, trace, violation := explore(tt.machine(), tt.eventLoop, 0)
			if violation == "" {
				t.Fatalf("the model found no broken rule in %d states for the rule that %s replaced", states, tt.replacedBy)
			}
			if !strings.Contains(violation, tt.breaks) {
				t.Fatalf("the model rejected the rule for another reason than %q, after\n  %s\n%s", tt.breaks, strings.Join(trace, "\n  "), violation)
			}
			t.Logf("replaced by %s; rejected after\n  %s\n%s", tt.replacedBy, strings.Join(trace, "\n  "), violation)
		})
	}
}
