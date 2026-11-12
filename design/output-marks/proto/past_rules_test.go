package proto

import "testing"

// Rules reconstructed from internal/server/output.go at the parent of each fix
// commit, each plugged into the design's core in place of the one decision the
// commit changed. The checker must find a counterexample for every one, and
// that counterexample is the bug the commit fixed.

// flushOnBacklog is the wait in Flush at ca505a9^:
//
//	replied := o.unflushed
//	o.unflushed = false
//	for replied && o.writing && !o.closed && o.backlog.Load() > outputHighWater {
//
// where backlog is "every byte in queued, held and inflight[sent:]".
type flushOnBacklog struct {
	coreModel
	unflushed bool
}

func (b *flushOnBacklog) clone() model { c := *b; return &c }

func (b *flushOnBacklog) apply(o op) observation {
	obs := b.coreModel.apply(o)
	switch o {
	case opReply1, opReply5:
		if !b.s.Closed() {
			b.unflushed = true
		}
	case opFlush:
		replied := b.unflushed
		b.unflushed = false
		obs.flushWaits = replied && !b.s.Closed() && b.s.Marks().Unwritten() > lim.HighWater
	}
	return obs
}

// flushOnReplyWaiting is the wait in Flush at 5a28666^: replyWaiting grows by
// each Reply's bytes and shrinks by every byte written, whatever it was.
//
//	o.replyWaiting += added                            // Reply
//	o.replyWaiting = max(o.replyWaiting-n, 0)          // write
//	for o.writing && !o.closed && o.replyWaiting > outputHighWater {
type flushOnReplyWaiting struct {
	coreModel
	replyWaiting int64
}

func (b *flushOnReplyWaiting) clone() model { c := *b; return &c }

func (b *flushOnReplyWaiting) apply(o op) observation {
	before := b.s.Marks().Written
	closed := b.s.Closed()
	obs := b.coreModel.apply(o)
	switch o {
	case opReply1:
		if !closed {
			b.replyWaiting++
		}
	case opReply5:
		if !closed {
			b.replyWaiting += 5
		}
	case opFlush:
		obs.flushWaits = !b.s.Closed() && b.replyWaiting > lim.HighWater
	}
	if n := int64(b.s.Marks().Written - before); n > 0 {
		b.replyWaiting = max(b.replyWaiting-n, 0)
	}
	return obs
}

// drainAtHighWater is the handler's teardown at 721a457^: `defer out.Flush()`,
// which waits only while more than outputHighWater is left before the latest
// Reply is out, and then the Close that drops the rest.
type drainAtHighWater struct{ coreModel }

func (b *drainAtHighWater) clone() model { c := *b; return &c }

func (b *drainAtHighWater) apply(o op) observation {
	obs := b.coreModel.apply(o)
	if o == opDrain {
		obs.drainWaits = !b.s.Closed() && b.s.Marks().UntilReply() > lim.HighWater
	}
	return obs
}

// eventLoopReadGate is the event loop's gate today (93c4c76), in connReadable
// and finishConnEvent:
//
//	conn.machine.PendingOutputBytes() <= eventLoopOutputHighWater
//
// where PendingOutputBytes is Output.Backlog, every byte that waits. It is the
// goroutine mode's rule before ca505a9, which ca505a9 left in place there.
type eventLoopReadGate struct{ coreModel }

func (b *eventLoopReadGate) clone() model { c := *b; return &c }

func (b *eventLoopReadGate) apply(o op) observation {
	obs := b.coreModel.apply(o)
	if o == opFlush {
		obs.flushWaits = !b.s.Closed() && b.s.Marks().Unwritten() > lim.HighWater
	}
	return obs
}

// batchGrowsWhileWriting is Flush's batch at 878dcef^: a Reply queued whole
// (Output.ahead) did not count as the flush's one batch, so once the writer had
// written it, it went on with the queue, which had grown by everything pushed
// meanwhile. Here the writer that finishes its batch takes what was queued
// since, instead of handing the socket on.
type batchGrowsWhileWriting struct{ coreModel }

func (b *batchGrowsWhileWriting) clone() model { c := *b; return &c }

func (b *batchGrowsWhileWriting) apply(o op) observation {
	writer := b.s.writer
	obs := b.coreModel.apply(o)
	if (o == opWrite1 || o == opWriteAll) && writer != NoWriter && b.s.writer == NoWriter && b.s.Take(writer, b.now) {
		obs.onOffer = int(b.s.Marks().OnOffer())
		obs.stallRuns = b.s.StallClockRuns()
	}
	return obs
}

// stallOnAnyWaiting is the stall count at 22d6ba9^: it ran from the first byte
// that waited for a Replica, held ones included.
type stallOnAnyWaiting struct{ coreModel }

func (b *stallOnAnyWaiting) clone() model { c := *b; return &c }

func (b *stallOnAnyWaiting) apply(o op) observation {
	obs := b.coreModel.apply(o)
	obs.stallRuns = b.s.replica && b.s.Marks().Unwritten() > 0
	return obs
}

// stallOnWritable is the stall count at 1bc2076^ (22d6ba9): it ran while
// output the socket could take waited, offered to it or not.
type stallOnWritable struct{ coreModel }

func (b *stallOnWritable) clone() model { c := *b; return &c }

func (b *stallOnWritable) apply(o op) observation {
	obs := b.coreModel.apply(o)
	m := b.s.Marks()
	obs.stallRuns = b.s.replica && m.Queued > m.Written
	return obs
}

// stallTimerJudges is checkStall at c8a0832^: the timer closes a Replica whose
// batch is partly written and that took nothing for the stall timeout, whoever
// its writer is.
type stallTimerJudges struct{ coreModel }

func (b *stallTimerJudges) clone() model { c := *b; return &c }

func (b *stallTimerJudges) apply(o op) observation {
	obs := b.coreModel.apply(o)
	if o == opTick && !obs.stalled && b.s.StallClockRuns() && b.now-b.s.LastProgress() >= lim.StallTimeout {
		b.s.Close()
		obs.stalled = true
		obs.stallRuns = false
	}
	return obs
}

// armOnClosed is MarkReplica at 145fa8b^: it started the stall count when a
// batch looked offered (o.sent < len(o.inflight)), without asking whether the
// Output was closed. Close at 145fa8b^ left inflight and sent as they were,
// so a batch a writer left partly sent still looked offered after Close.
type armOnClosed struct {
	coreModel
	// keepOfferOnClose models that Close. False runs the same rule on the
	// design's Marks, where Close leaves nothing on offer.
	keepOfferOnClose bool
	keptOffer        int64
	armedWhenClosed  bool
}

func (b *armOnClosed) clone() model { c := *b; return &c }

func (b *armOnClosed) apply(o op) observation {
	offer := b.s.Marks().OnOffer()
	wasClosed := b.s.Closed()
	obs := b.coreModel.apply(o)
	if b.keepOfferOnClose && !wasClosed && b.s.Closed() {
		b.keptOffer = offer
	}
	if o == opMarkReplica && b.s.Closed() {
		looksOffered := b.s.Marks().OnOffer()
		if b.keepOfferOnClose {
			looksOffered = b.keptOffer
		}
		if looksOffered > 0 {
			b.armedWhenClosed = true
		}
	}
	obs.stallRuns = obs.stallRuns || b.armedWhenClosed
	return obs
}

func TestTheCheckerRejectsTheRulesOfPastFixCommits(t *testing.T) {
	for _, tt := range []struct {
		name      string
		alphabet  []op
		depth     int
		mode      Writer
		candidate func(mode Writer) model
	}{
		{name: "ca505a9^ Flush waits on the whole backlog", alphabet: queueAlphabet, depth: queueDepth, mode: Blocking,
			candidate: func(m Writer) model { return &flushOnBacklog{coreModel: *newCore(m)} }},
		{name: "5a28666^ Flush counts every written byte against the Reply", alphabet: queueAlphabet, depth: queueDepth, mode: Blocking,
			candidate: func(m Writer) model { return &flushOnReplyWaiting{coreModel: *newCore(m)} }},
		{name: "721a457^ teardown waits only down to the high-water mark", alphabet: queueAlphabet, depth: queueDepth, mode: Blocking,
			candidate: func(m Writer) model { return &drainAtHighWater{coreModel: *newCore(m)} }},
		{name: "93c4c76 event loop gates reads on the whole backlog", alphabet: queueAlphabet, depth: queueDepth, mode: Polled,
			candidate: func(m Writer) model { return &eventLoopReadGate{coreModel: *newCore(m)} }},
		{name: "878dcef^ a flush's batch takes what was pushed while it wrote", alphabet: queueAlphabet, depth: queueDepth, mode: Blocking,
			candidate: func(m Writer) model { return &batchGrowsWhileWriting{coreModel: *newCore(m)} }},
		{name: "22d6ba9^ stall count runs while held bytes wait", alphabet: stallAlphabet, depth: stallDepth, mode: Blocking,
			candidate: func(m Writer) model { return &stallOnAnyWaiting{coreModel: *newCore(m)} }},
		{name: "1bc2076^ stall count runs for bytes no writer offered", alphabet: stallAlphabet, depth: stallDepth, mode: Blocking,
			candidate: func(m Writer) model { return &stallOnWritable{coreModel: *newCore(m)} }},
		{name: "c8a0832^ stall timer judges a polled writer that was away", alphabet: stallAlphabet, depth: stallDepth, mode: Polled,
			candidate: func(m Writer) model { return &stallTimerJudges{coreModel: *newCore(m)} }},
		{name: "145fa8b^ MarkReplica starts a count on a closed Output", alphabet: stallAlphabet, depth: stallDepth, mode: Blocking,
			candidate: func(m Writer) model { return &armOnClosed{coreModel: *newCore(m), keepOfferOnClose: true} }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			mode := tt.mode
			found, runs := check(alphabetFor(tt.alphabet, mode), tt.depth,
				func() model { return &ref{mode: mode} },
				func() model { return tt.candidate(mode) })
			if found == nil {
				t.Fatalf("no counterexample in %d sequences, want the checker to reject the rule", runs)
			}
			t.Logf("rejected: %v", found)
		})
	}
}

// TestTheDesignsMarksDisarmTheRuleOf145fa8bParent runs MarkReplica's rule from
// 145fa8b^, with no closed check, on the design's Marks. Close leaves nothing
// on offer there, so the missing check cannot start a count.
func TestTheDesignsMarksDisarmTheRuleOf145fa8bParent(t *testing.T) {
	found, runs := check(stallAlphabet, stallDepth,
		func() model { return &ref{mode: Blocking} },
		func() model { return &armOnClosed{coreModel: *newCore(Blocking)} })
	if found != nil {
		t.Fatalf("the rule disagrees with the reference on the design's Marks: %v", found)
	}
	t.Logf("no counterexample in %d sequences of up to %d operations", runs, stallDepth)
}
