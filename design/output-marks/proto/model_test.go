package proto

import (
	"fmt"
	"strings"
	"testing"
)

// The model checker. It enumerates every sequence of operations up to a depth,
// runs each on a reference model and on a candidate, and compares what they
// observe after every step. The reference keeps one tag per byte in the order
// the peer receives them and defines each decision over those bytes in the
// plainest way. The candidate is the design's core, or a rule reconstructed
// from the parent of a fix commit plugged into that core.

var lim = Limits{HighWater: 4, StallTimeout: 2}

type op uint8

const (
	opPush1 op = iota
	opPush5
	opHold
	opReply0
	opReply1
	opReply5
	opTake
	opWrite1
	opWriteAll
	opWrite0
	opTick
	opMarkReplica
	opClose
	opFlush
	opDrain
)

var opNames = [...]string{
	opPush1: "Push(1)", opPush5: "Push(5)", opHold: "HoldPushes",
	opReply0: "Reply(0)", opReply1: "Reply(1)", opReply5: "Reply(5)",
	opTake: "Take", opWrite1: "Wrote(1)", opWriteAll: "Wrote(all)", opWrite0: "Wrote(0)",
	opTick: "Tick", opMarkReplica: "MarkReplica", opClose: "Close",
	opFlush: "Flush?", opDrain: "Drain?",
}

func (o op) String() string { return opNames[o] }

// observation is everything the checker compares after one step.
type observation struct {
	flushWaits bool // at Flush?: the goroutine waits for another writer
	drainWaits bool // at Drain?: the closing goroutine waits for another writer
	stallRuns  bool // after every step: the stall rule is counting
	stalled    bool // this step closed the connection as stalled
	onOffer    int  // after every step: bytes a writer was offered and the socket has not taken
}

func (o observation) String() string {
	return fmt.Sprintf("{flushWaits:%v drainWaits:%v stallRuns:%v stalled:%v onOffer:%d}", o.flushWaits, o.drainWaits, o.stallRuns, o.stalled, o.onOffer)
}

type model interface {
	apply(op) observation
	clone() model
}

// ref is the reference: the bytes themselves.
type ref struct {
	mode         Writer
	wire         []bool // in the order the peer receives them; true for a Reply's byte
	held         int
	written      int
	offered      int
	writing      bool
	holding      bool
	closed       bool
	replica      bool
	now          Time
	lastProgress Time
}

func (r *ref) clone() model {
	c := *r
	c.wire = append([]bool(nil), r.wire...)
	return &c
}

// untilLastReplyByte counts the unwritten bytes up to and including the last
// byte of any Reply.
func (r *ref) untilLastReplyByte() int {
	last := -1
	for i, reply := range r.wire {
		if reply {
			last = i
		}
	}
	return max(last+1-r.written, 0)
}

func (r *ref) close() {
	r.closed, r.writing, r.holding = true, false, false
	r.wire = r.wire[:r.written]
	r.held, r.offered = 0, r.written
}

func (r *ref) write(n int) (stalled bool) {
	if !r.writing {
		return false
	}
	n = min(n, r.offered-r.written)
	r.written += n
	switch {
	case n > 0:
		r.lastProgress = r.now
	case r.mode == Polled && r.replica && r.now-r.lastProgress >= lim.StallTimeout:
		r.close()
		return true
	}
	if r.written == r.offered {
		r.writing = false
	}
	return false
}

func (r *ref) apply(o op) observation {
	var obs observation
	switch o {
	case opPush1, opPush5:
		n := map[op]int{opPush1: 1, opPush5: 5}[o]
		if r.closed {
			break
		}
		if r.holding {
			r.held += n
		} else {
			for i := 0; i < n; i++ {
				r.wire = append(r.wire, false)
			}
		}
	case opHold:
		r.holding = true
	case opReply0, opReply1, opReply5:
		n := map[op]int{opReply0: 0, opReply1: 1, opReply5: 5}[o]
		if r.closed {
			break
		}
		for i := 0; i < n; i++ {
			r.wire = append(r.wire, true)
		}
		if r.holding {
			r.holding = false
			for i := 0; i < r.held; i++ {
				r.wire = append(r.wire, false)
			}
			r.held = 0
		}
	case opTake:
		if r.closed || r.writing || len(r.wire) == r.written {
			break
		}
		r.writing, r.offered = true, len(r.wire)
		if r.replica {
			r.lastProgress = r.now
		}
	case opWrite1:
		obs.stalled = r.write(1)
	case opWriteAll:
		obs.stalled = r.write(r.offered - r.written)
	case opWrite0:
		obs.stalled = r.write(0)
	case opTick:
		r.now++
		if r.replica && r.writing && r.mode == Blocking && r.now-r.lastProgress >= lim.StallTimeout {
			r.close()
			obs.stalled = true
		}
	case opMarkReplica:
		r.replica = true
		if r.writing {
			r.lastProgress = r.now
		}
	case opClose:
		r.close()
	case opFlush:
		obs.flushWaits = !r.closed && r.untilLastReplyByte() > int(lim.HighWater)
	case opDrain:
		obs.drainWaits = !r.closed && r.untilLastReplyByte() > 0
	}
	obs.stallRuns = r.replica && r.writing && r.offered > r.written
	obs.onOffer = r.offered - r.written
	return obs
}

// coreModel runs the design's core.
type coreModel struct {
	mode Writer
	s    Stream
	now  Time
}

func newCore(mode Writer) *coreModel { return &coreModel{mode: mode, s: *NewStream(lim)} }

func (c *coreModel) clone() model { d := *c; return &d }

func (c *coreModel) apply(o op) observation {
	var obs observation
	switch o {
	case opPush1:
		c.s.Push(1)
	case opPush5:
		c.s.Push(5)
	case opHold:
		c.s.HoldPushes()
	case opReply0:
		c.s.Reply(0)
	case opReply1:
		c.s.Reply(1)
	case opReply5:
		c.s.Reply(5)
	case opTake:
		c.s.Take(c.mode, c.now)
	case opWrite1:
		obs.stalled = c.s.Wrote(1, c.now) == Stalled
	case opWriteAll:
		obs.stalled = c.s.Wrote(int(c.s.Marks().OnOffer()), c.now) == Stalled
	case opWrite0:
		obs.stalled = c.s.Wrote(0, c.now) == Stalled
	case opTick:
		c.now++
		obs.stalled = c.s.Tick(c.now) == Stalled
	case opMarkReplica:
		c.s.MarkReplica(c.now)
	case opClose:
		c.s.Close()
	case opFlush:
		obs.flushWaits = !MayReadNext(c.s.Marks(), lim)
	case opDrain:
		obs.drainWaits = !Drained(c.s.Marks())
	}
	obs.stallRuns = c.s.StallClockRuns()
	obs.onOffer = int(c.s.Marks().OnOffer())
	return obs
}

// counterexample is the shortest sequence on which candidate and reference
// observe differently.
type counterexample struct {
	ops       []op
	want, got observation
}

func (c *counterexample) String() string {
	names := make([]string, len(c.ops))
	for i, o := range c.ops {
		names[i] = o.String()
	}
	return fmt.Sprintf("%s: reference %v, candidate %v", strings.Join(names, ", "), c.want, c.got)
}

// check returns the shortest counterexample up to depth, or nil, and how many
// sequences it ran.
func check(alphabet []op, depth int, newRef, newCandidate func() model) (*counterexample, int) {
	runs := 0
	var walk func(r, c model, path []op, left int) *counterexample
	walk = func(r, c model, path []op, left int) *counterexample {
		if left == 0 {
			runs++
			return nil
		}
		for _, o := range alphabet {
			rr, cc := r.clone(), c.clone()
			want, got := rr.apply(o), cc.apply(o)
			next := append(append([]op(nil), path...), o)
			if want != got {
				return &counterexample{ops: next, want: want, got: got}
			}
			if found := walk(rr, cc, next, left-1); found != nil {
				return found
			}
		}
		return nil
	}
	for d := 1; d <= depth; d++ {
		if found := walk(newRef(), newCandidate(), nil, d); found != nil {
			return found, runs
		}
	}
	return nil, runs
}

var (
	// queueAlphabet exercises ordering, holding and the two waits.
	queueAlphabet = []op{opPush1, opPush5, opHold, opReply0, opReply1, opReply5, opTake, opWrite1, opWriteAll, opClose, opFlush, opDrain}
	// stallAlphabet exercises the stall rule. opWrite0 is an attempt that took
	// nothing, which only a Polled writer makes.
	stallAlphabet = []op{opPush1, opHold, opReply1, opTake, opWrite1, opWrite0, opTick, opMarkReplica, opClose}
)

func alphabetFor(alphabet []op, mode Writer) []op {
	if mode == Polled {
		return alphabet
	}
	var out []op
	for _, o := range alphabet {
		if o != opWrite0 {
			out = append(out, o)
		}
	}
	return out
}

const (
	queueDepth = 6
	stallDepth = 7
)

func TestTheDesignCoreAgreesWithTheReferenceOnEverySequence(t *testing.T) {
	for _, tt := range []struct {
		name     string
		alphabet []op
		depth    int
		mode     Writer
	}{
		{name: "queue, blocking writer", alphabet: queueAlphabet, depth: queueDepth, mode: Blocking},
		{name: "queue, polled writer", alphabet: queueAlphabet, depth: queueDepth, mode: Polled},
		{name: "stall, blocking writer", alphabet: stallAlphabet, depth: stallDepth, mode: Blocking},
		{name: "stall, polled writer", alphabet: stallAlphabet, depth: stallDepth, mode: Polled},
	} {
		t.Run(tt.name, func(t *testing.T) {
			mode := tt.mode
			found, runs := check(alphabetFor(tt.alphabet, mode), tt.depth,
				func() model { return &ref{mode: mode} },
				func() model { return newCore(mode) })
			if found != nil {
				t.Fatalf("the core disagrees with the reference: %v", found)
			}
			t.Logf("no counterexample in %d sequences of up to %d operations", runs, tt.depth)
		})
	}
}
