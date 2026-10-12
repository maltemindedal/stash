package command

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/record"
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
// Handle admits a request (Executor.admit) before it takes any of these locks.
// One refused for its Call, or because its client has not authenticated, takes
// none, so a client without the password can neither wait for another request
// nor hold one up.
//
// A request that writes also holds the stripe of each key it writes, from before
// it executes until Handle has recorded its command frames (Executor.record,
// record.Recorder), so a write's locks never leave Handle. Two writers to the
// same key therefore reach the AOF and the replicas in the order they were
// applied; writers on different stripes commute, so nothing orders them against
// each other (except under maxmemory, below), and the Recorder takes no lock of its own. Without it a client could update the store, another
// client could update it and append first, and the log would replay the two in the
// opposite order to the one live state came from. Writers to different keys run
// in parallel, except that a write whose keys are not known holds every stripe.
//
// While maxmemory is set, an Accounted write (a Growing write under maxmemory)
// also holds every stripe (writeShapeOf). Making room for it can cause a
// Memory-pressure eviction anywhere in the keyspace, and its sweep a TTL
// eviction, and their DELs are recorded with its own frames or through the
// expiration listener while it runs. So it no longer commutes with a write of
// an evicted key: held to its own stripe, such a write could be applied after
// the Eviction and still be recorded before its DEL, and replay would delete a
// live key. Shrinking writes (DEL, the pops, HDEL, SREM) never evict or sweep,
// so they keep their own stripes.
//
// The stripes exist to order what reaches the log and the replicas, so a server
// with neither has nothing to order and skips them (needsOrder).
//
// Lock order is gate, then stripes by ascending index, then what recording
// takes: the AOF writer's lock (aof.Writer.Append), and then the replica
// registry's lock and a replica feed's (server.ReplicaRegistry.Propagate),
// which take neither gate nor stripe, and are not held together. Nothing takes
// gate shared while it already holds it, and nothing waits for another request
// while holding either (a blocking command waits with neither held). Recording
// is never done under a shard lock.
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
	// sequenceWrite: gate shared and writes, until its write is recorded.
	sequenceWrite
	// sequenceExclusive: gate exclusive, until its write is recorded.
	sequenceExclusive
	// sequenceSelf: the command orders itself, because it waits (BLPOP, WAIT)
	// or because it needs locks no other mode takes (PSYNC's attach cut).
	sequenceSelf
)

// sequenceModeFor classifies a request. A client that is queueing commands for
// EXEC executes nothing yet, so its requests are reads whatever they name.
func (e *Executor) sequenceModeFor(request *Request) sequenceMode {
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
		if state := request.client; state != nil && state.InTransactionActive() {
			return sequenceRead
		}
	}
	if spec.durable || spec.propagates {
		return sequenceWrite
	}
	return sequenceRead
}

// Handle runs one request the server received, as call describes it, ordered
// against the other requests as described on sequencer. A write's command
// frames are recorded before its locks are released, so they never leave this
// function: nothing else can execute a write to the same keys, or a
// transaction, until its frames are in the AOF and on their way to the
// replicas.
func (e *Executor) Handle(ctx context.Context, call server.Call, value protocol.Value) (server.ExecuteResult, error) {
	request, err := decodeCall(call, value)
	if err != nil {
		return server.ExecuteResult{}, err
	}
	if err := e.admit(request); err != nil {
		return server.ExecuteResult{}, err
	}

	q := e.seq
	switch e.sequenceModeFor(request) {
	case sequenceExclusive:
		q.gate.Lock()
		defer q.gate.Unlock()
		return e.executeAndRecord(ctx, request)
	case sequenceWrite:
		release := q.beginWrite(e.writeShapeOf(request.Name), request.Args)
		defer release()
		return e.executeAndRecord(ctx, request)
	case sequenceSelf:
		// BLPOP records its pop inside the attempt that holds its stripe.
		return e.executeRequestDetailed(ctx, request, true)
	default:
		q.gate.RLock()
		defer q.gate.RUnlock()
		return e.executeAndRecord(ctx, request)
	}
}

// executeAndRecord executes request and records its write. The caller holds
// the request's ordering.
func (e *Executor) executeAndRecord(ctx context.Context, request *Request) (server.ExecuteResult, error) {
	result, err := e.executeRequestDetailed(ctx, request, true)
	if err != nil {
		return result, err
	}
	return e.record(request, result)
}

// record records the command frames request's execution produced, while the
// caller holds the request's ordering, and turns what the Recorder did into
// the request's reply. A write the Recorder withheld is answered ERR
// persistence failure. A failed append of the Master's command frames is
// returned, so that the link ends. Otherwise the end of the write's frames in
// the replication stream becomes the client's last write offset, which WAIT
// waits for.
func (e *Executor) record(request *Request, result server.ExecuteResult) (server.ExecuteResult, error) {
	outcome := e.recorder.Record(&request.write)
	switch {
	case outcome.Withheld && request.origin != record.OriginMaster:
		return server.ExecuteResult{}, ErrPersistenceFailureError()
	case outcome.Err != nil && request.origin == record.OriginMaster:
		return server.ExecuteResult{}, fmt.Errorf("%w: %w", server.ErrNotRecorded, outcome.Err)
	case errors.Is(outcome.Err, record.ErrZeroOrigin):
		return server.ExecuteResult{}, outcome.Err
	}
	if outcome.End > 0 && request.client != nil {
		request.client.SetLastWriteReplicationOffset(outcome.End)
	}
	return result, nil
}

// BeginBackgroundWrite orders work the server does on its own initiative (the
// expiry sweep, the snapshot a rewrite takes) against client requests, as a
// write is ordered. The caller calls the returned function when its frames have
// been applied.
func (e *Executor) BeginBackgroundWrite() (release func()) {
	return e.seq.beginWrite(keysUnknown, nil)
}

// writeShapeOf is the key shape a write named name is ordered by: the keys its
// spec declares, or every key for an Accounted write, whose Evictions may
// remove keys outside its own stripes (see sequencer). Maxmemory is configured before the server serves its first
// request, so the setting cannot change between this check and the write.
func (e *Executor) writeShapeOf(name string) keyShape {
	spec, _ := e.command(name)
	if spec.durable && !spec.shrinks && e.store.MaxMemory() > 0 {
		return keysUnknown
	}
	return spec.keys
}

// beginWrite orders one attempt of a command that orders itself (BLPOP) as a
// write. Inside a transaction, EXEC already holds the gate exclusively, so it
// returns a function that does nothing.
func (e *Executor) beginWrite(shape keyShape, request *Request) (release func()) {
	if request.inTransaction {
		return func() {}
	}
	return e.seq.beginWrite(shape, request.Args)
}
