package command

import (
	"context"
	"sync"

	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/server"
)

// sequencer orders the effects of requests against one another, which the sharded
// store alone does not do.
//
// Every request holds gate shared while it executes, and EXEC holds it exclusive,
// so a transaction runs with no other request in progress: nothing can change a
// watched key between EXEC's check and its queued commands, and nobody observes
// the transaction half way through.
//
// A request that writes also holds the stripe of each key it writes, from before
// it executes until its frames have reached the AOF and the replicas
// (ExecuteResult.Release). Two writers to the same key therefore append in the
// order they were applied. Without it a client could update the store, another
// client could update it and append first, and the log would replay the two in the
// opposite order to the one live state came from. Writers to different keys run
// in parallel, except that a write whose keys are not known holds every stripe.
//
// The stripes exist to order what reaches the log and the replicas, so a server
// with neither has nothing to order and skips them (needsOrder).
//
// Lock order is gate, then stripes by ascending index. Nothing takes gate shared
// while it already holds it, and nothing waits for another request while holding
// either (a blocking command waits with neither held). Handing a write's frames
// to the replicas then takes the replica registry's lock and a replica feed's
// (server.ReplicaRegistry.Propagate), which take neither gate nor stripe.
// Counting a frame's replication offset and queueing it for every replica happen
// under that one lock, so writers on different stripes, and WAIT's GETACK, reach
// every replica in offset order. While no replica is registered, a client write's
// frames are only counted, without the registry lock
// (server.ReplicaRegistry.PropagateOrdered): the attach cut below registers a
// replica only when no write sits between being applied and having its frames
// counted, so a write is counted either below the new replica's base, and is in
// its snapshot, or after the replica is registered. WAIT's GETACK, which holds
// neither gate nor stripe, and the expiry DELs, which a replica's full resync
// publishes holding neither, always take the lock.
//
// PSYNC attaches a replica at an attach cut (attachCut): it copies the snapshot
// it sends the replica and registers the replica while it holds gate shared and
// every stripe, so no write sits between being applied and being handed to the
// replicas, and each write is in exactly one of the snapshot and the replica's
// stream. Writes that skipped their stripes because nothing was recording are
// waited for first, by taking gate exclusively once, and the cut switches
// recording on while it holds gate, so every write after that takes its stripes.
// The store's shard locks (the copy) and then the registry's lock (registering)
// come after the stripes, and are not held together. Readers keep running during
// the copy; writers wait for it, as they already waited for the shard locks the
// copy takes. PSYNC orders itself, like BLPOP and WAIT, but waits only for
// locks, so it also runs under the event loop. It is refused inside MULTI: EXEC
// holds gate exclusively, which the cut would wait for forever.
type sequencer struct {
	gate    sync.RWMutex
	stripes [writeStripes]paddedMutex

	// needsOrder reports whether anything is currently recording writes: an
	// append-only file, or a replica attached or attaching. When it is nil,
	// writes are always ordered.
	needsOrder func() bool

	// The release functions are bound once, so that handing one to a result does
	// not allocate.
	releaseStripe    [writeStripes]func()
	releaseAll       func()
	releaseGateOnly  func()
	releaseExclusive func()
}

// writeStripes is how many independent locks writers to different keys are
// spread over. It also fits the bit set a multi-key write uses.
const writeStripes = 64

const allStripes = ^uint64(0)

// paddedMutex keeps each stripe on its own cache line, so that writers to
// different keys do not slow each other down by sharing one.
type paddedMutex struct {
	sync.Mutex
	_ [56]byte
}

func newSequencer() *sequencer {
	q := &sequencer{}
	for i := range q.releaseStripe {
		i := i // each release function unlocks its own stripe (go 1.21 shares the loop variable)
		q.releaseStripe[i] = func() {
			q.stripes[i].Unlock()
			q.gate.RUnlock()
		}
	}
	q.releaseAll = func() {
		q.unlockStripes(allStripes)
		q.gate.RUnlock()
	}
	q.releaseGateOnly = q.gate.RUnlock
	q.releaseExclusive = q.gate.Unlock
	return q
}

// keyShape says which arguments of a command are the keys it writes.
type keyShape int

const (
	// keysUnknown: the command is ordered against every other write. It is the
	// zero value, so a command added without a shape is ordered conservatively.
	keysUnknown keyShape = iota
	// keysFirstArg: the first argument is the only key.
	keysFirstArg
	// keysEveryArg: every argument is a key.
	keysEveryArg
)

func stripeOf(key []byte) int {
	hash := uint64(14695981039346656037)
	for _, b := range key {
		hash ^= uint64(b)
		hash *= 1099511628211
	}
	return int(hash % writeStripes)
}

func (q *sequencer) lockStripes(mask uint64) {
	for i := 0; i < writeStripes; i++ {
		if mask&(1<<uint(i)) != 0 {
			q.stripes[i].Lock()
		}
	}
}

func (q *sequencer) unlockStripes(mask uint64) {
	for i := writeStripes - 1; i >= 0; i-- {
		if mask&(1<<uint(i)) != 0 {
			q.stripes[i].Unlock()
		}
	}
}

// beginWrite takes the locks a write to the keys named by shape and args needs,
// and returns the function that gives them up.
func (q *sequencer) beginWrite(shape keyShape, args [][]byte) (release func()) {
	q.gate.RLock()
	if q.needsOrder != nil && !q.needsOrder() {
		return q.releaseGateOnly
	}

	switch shape {
	case keysFirstArg:
		if len(args) > 0 {
			i := stripeOf(args[0])
			q.stripes[i].Lock()
			return q.releaseStripe[i]
		}
	case keysEveryArg:
		var mask uint64
		for _, arg := range args {
			mask |= 1 << uint(stripeOf(arg))
		}
		if mask != 0 {
			q.lockStripes(mask)
			return func() {
				q.unlockStripes(mask)
				q.gate.RUnlock()
			}
		}
	}

	q.lockStripes(allStripes)
	return q.releaseAll
}

// attachCut takes the locks under which PSYNC copies a replica's snapshot and
// registers it, and returns the function that gives them up (see sequencer). In
// order, it:
//
//  1. takes gate exclusively, which waits for every write in progress, including
//     those that skipped their stripes because nothing was recording: they hold
//     gate shared from before they execute until their frames have been handed
//     to the replicas. It does so on every attach, since a replica detaching just
//     before can leave such a write in flight;
//  2. calls startRecording, after which needsOrder must report true, and releases
//     gate, so every write that begins after it takes its stripes;
//  3. takes gate shared and every stripe, as a write whose keys are unknown does,
//     whatever needsOrder reports: the cut is what makes the copy and the
//     registration one moment.
//
// The caller must hold neither gate nor a stripe, and must not be running inside
// EXEC, which holds gate exclusively.
func (q *sequencer) attachCut(startRecording func()) (release func()) {
	q.gate.Lock()
	startRecording()
	q.gate.Unlock()

	q.gate.RLock()
	q.lockStripes(allStripes)
	return q.releaseAll
}

// sequenceMode says how a request is ordered.
type sequenceMode int

const (
	// sequenceRead: holds gate shared while it executes.
	sequenceRead sequenceMode = iota
	// sequenceWrite: gate shared and writes, until the result is released.
	sequenceWrite
	// sequenceExclusive: gate exclusive, until the result is released.
	sequenceExclusive
	// sequenceSelf: the command orders itself, because it waits (BLPOP, WAIT)
	// or because it needs locks no other mode takes (PSYNC's attach cut).
	sequenceSelf
)

// sequenceModeFor classifies a request. A client that is queueing commands for
// EXEC executes nothing yet, so its requests are reads whatever they name.
func (e *Executor) sequenceModeFor(ctx context.Context, request *Request) sequenceMode {
	spec, ok := e.command(request.Name)
	if !ok {
		return sequenceRead
	}

	switch request.Name {
	case "EXEC":
		return sequenceExclusive
	case "BLPOP", "WAIT", "PSYNC":
		return sequenceSelf
	}
	if !spec.transactionControl {
		if state, ok := server.ClientStateFromContext(ctx); ok && state != nil && state.InTransactionActive() {
			return sequenceRead
		}
	}
	if spec.durable || spec.propagates {
		return sequenceWrite
	}
	return sequenceRead
}

// ExecuteSequenced executes a request like ExecuteDetailed and orders it against
// the other requests as described on sequencer. When the result has a Release
// function the caller must call it once it has applied the result's durability
// and propagation frames, and not before; nothing else can execute a write, or a
// transaction, until then.
func (e *Executor) ExecuteSequenced(ctx context.Context, value protocol.Value) (server.ExecuteResult, error) {
	request, err := DecodeRequest(value)
	if err != nil {
		return server.ExecuteResult{}, err
	}

	q := e.seq
	switch e.sequenceModeFor(ctx, request) {
	case sequenceExclusive:
		q.gate.Lock()
		result, err := e.executeRequestDetailed(ctx, request, true)
		if err != nil {
			q.gate.Unlock()
			return result, err
		}
		result.Release = q.releaseExclusive
		return result, nil
	case sequenceWrite:
		release := q.beginWrite(e.keyShapeOf(request.Name), request.Args)
		result, err := e.executeRequestDetailed(ctx, request, true)
		if err != nil {
			release()
			return result, err
		}
		result.Release = release
		return result, nil
	case sequenceSelf:
		return e.executeRequestDetailed(ctx, request, true)
	default:
		q.gate.RLock()
		defer q.gate.RUnlock()
		return e.executeRequestDetailed(ctx, request, true)
	}
}

// BeginBackgroundWrite orders work the server does on its own initiative (the
// expiry sweep, the snapshot a rewrite takes) against client requests, as a
// write is ordered. The caller calls the returned function when its frames have
// been applied.
func (e *Executor) BeginBackgroundWrite() (release func()) {
	return e.seq.beginWrite(keysUnknown, nil)
}

func (e *Executor) keyShapeOf(name string) keyShape {
	spec, _ := e.command(name)
	return spec.keys
}

// beginWrite orders one attempt of a command that orders itself (BLPOP) as a
// write. Inside a transaction, EXEC already holds the gate exclusively, so it
// returns a function that does nothing.
func (e *Executor) beginWrite(ctx context.Context, shape keyShape, args [][]byte) (release func()) {
	if inTransactionExecution(ctx) {
		return func() {}
	}
	return e.seq.beginWrite(shape, args)
}

// SetWriteOrdering tells the executor how to find out whether anything is
// recording writes at the moment (an append-only file, a replica). While nothing
// is, writes are not ordered against one another, which keeps concurrent writers
// to different keys parallel. The default is to always order them.
func (e *Executor) SetWriteOrdering(needed func() bool) {
	e.seq.needsOrder = needed
}

type transactionExecutionKey struct{}

// withTransactionExecution marks ctx as belonging to EXEC running its queued
// commands, which hold the sequencer exclusively and so must not wait for
// anything another request would have to provide.
func withTransactionExecution(ctx context.Context) context.Context {
	return context.WithValue(ctx, transactionExecutionKey{}, true)
}

func inTransactionExecution(ctx context.Context) bool {
	inside, _ := ctx.Value(transactionExecutionKey{}).(bool)
	return inside
}
