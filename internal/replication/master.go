package replication

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/rdb"
	"github.com/maltemindedal/stash/internal/storage"
)

// Master is the component through which a Master serves its Replicas. It owns
// the replication offset, each Replica's base offset and acknowledgements,
// what is pushed to each Replica's Output, and attaching a Replica at one
// attach cut.
//
// Its lock comes after the command sequencer's gate and write stripes, and
// before the locks of a Replica's Output: lock order is gate, stripes, mu,
// Output. While holding mu the Master takes no gate, stripe or shard lock,
// waits on no socket (no method of an Output does), and calls no dropped
// handler. It never holds mu while it runs an attach cut: the cut takes mu
// under the gate (beginAttach) and Attach takes it again under every stripe
// (reserve), each time for a few instructions. Await waits holding nothing.
//
// A client write's frames skip mu while no Replica is registered (Propagate
// with ordered set): the attach cut orders them against reserve instead. Every
// other frame takes it.
type Master struct {
	replicationID string
	// offset is the replication offset: the bytes of the stream counted so far.
	offset atomic.Int64
	store  *storage.Store
	logger *slog.Logger

	// dropped is told, with mu not held and on the goroutine that propagated
	// the frame, of each Replica the Master detached because its Output refused
	// that frame. When it is nil nobody is told.
	dropped func(replica *Replica, cause error)

	// snapshotLimit is the largest full resync snapshot Attach serves, in bytes.
	snapshotLimit int

	mu       sync.RWMutex
	replicas map[uint64]*Replica
	// registered is len(replicas), stored under mu whenever it changes, so that
	// an ordered Propagate can find out without mu that no Replica is registered.
	registered atomic.Int64
	changed    chan struct{}
	// attaching counts the Replicas between beginAttach and their reserve or
	// endAttach, which Active counts as Replicas already.
	attaching int
	// stopping is set by StopPropagating once the server is shutting down.
	// Frames are then counted but pushed to no Replica: shutdown ends each
	// Replica's stream once what is queued for it has been written
	// (EndStreams), and nothing may be written past that end.
	stopping bool
}

// Replica is a Replica attached to the Master.
type Replica struct {
	ID            uint64
	ListeningPort int

	// ack is the latest replication offset the Replica acknowledged, in its own
	// count: the bytes of the stream it has processed since it attached. It is
	// written under the Master's lock but read without it, so it is atomic.
	ack atomic.Int64
	// base is the Master's replication offset at the Replica's attach cut. The
	// Replica is sent every frame counted after that and counts from zero, so
	// its acknowledged offset in the Master's count is base+ack. It is set
	// before the Replica is registered and never changes.
	base int64

	// out is the Replica's Client output, which every frame counted after base
	// is pushed to.
	out Output
}

// Output is the Master's port to a Replica's connection: the connection's
// Client output, as much of it as the Master uses. It queues the stream behind
// the full resync reply, writes it to the socket, and closes a Replica that
// does not take it in time. Its methods are safe for concurrent use and none
// waits on the Replica's socket, so the Master calls Push under its lock.
type Output interface {
	// MarkReplica holds the connection to the Replica Push limit and the
	// Replica stall rule.
	MarkReplica()
	// HoldPushes makes what is pushed from now on wait for the Reply to the
	// request the connection is running, which is the full resync reply.
	HoldPushes()
	// Push queues a frame of the stream, and keeps no reference to it. It
	// returns false once the connection is closed: because the Replica left, or
	// because its Output closed it for falling behind.
	Push(frame []byte) bool
	// Err reports why the connection was closed, or nil while it is open.
	Err() error
	// EndStream shuts the write side of the connection once everything queued
	// for it has been written.
	EndStream()
}

// Candidate is a client that asks to become a Replica.
type Candidate struct {
	ID uint64
	// ListeningPort is the port the client announced with REPLCONF
	// listening-port, or 0.
	ListeningPort int
	// Out is the Client output of the client's connection.
	Out Output
}

// Attachment is a Replica attached at its attach cut, whose full resync reply
// has not been sent yet.
type Attachment struct {
	replicationID string
	snapshot      []byte
}

// Reply is the full resync reply: FULLRESYNC <replid> 0, then the RDB snapshot
// copied at the attach cut.
func (a *Attachment) Reply() []protocol.Value {
	return []protocol.Value{
		protocol.SimpleString{Value: fmt.Sprintf("FULLRESYNC %s 0", a.replicationID)},
		protocol.BulkString{Data: a.snapshot},
	}
}

// SnapshotTooLargeError is why Attach refused a Candidate: the full resync
// snapshot of the keyspace would be Size bytes, more than the Limit a Replica
// accepts in one bulk string.
type SnapshotTooLargeError struct {
	Size  int
	Limit int
}

func (e *SnapshotTooLargeError) Error() string {
	return fmt.Sprintf("replication: full resync snapshot of %d bytes exceeds the %d byte limit of a bulk string", e.Size, e.Limit)
}

// ReplicaInfo describes one attached Replica for INFO replication.
type ReplicaInfo struct {
	ID            uint64
	ListeningPort int
	// AckedMasterOffset is how far the Replica has acknowledged the stream, in
	// the Master's count, as Redis reports a replica's offset. It is 0 until
	// the Replica acknowledges something.
	AckedMasterOffset int64
}

// MasterInfo is what INFO replication reports about a Master.
type MasterInfo struct {
	ReplicationID string
	Offset        int64
	Replicas      []ReplicaInfo
}

type propagationReport struct {
	attempted   int
	succeeded   int
	failed      int
	payloadSize int
	endOffset   int64
}

// refusal is a Replica detached because its Output refused a frame, kept until
// the Master's lock is released so that the dropped handler can be told.
type refusal struct {
	replica *Replica
	cause   error
}

// getAckPayload is the REPLCONF GETACK * that asks every Replica how far it
// has got.
var getAckPayload = []byte("*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n")

// NewMaster returns a Master with a new replication ID and no Replica. A full
// resync copies store.
//
// dropped is called for each Replica the Master detached because its Output
// refused a frame, with why that Output was closed. It runs on the goroutine
// that propagated the frame, with the Master's lock not held. It only reports:
// an Output refuses a frame once its connection is closed, so nothing is left
// to close, and the connection's own teardown follows. With nil nobody is
// told.
func NewMaster(store *storage.Store, dropped func(replica *Replica, cause error), logger *slog.Logger) *Master {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Master{
		replicationID: randomReplicationID(),
		store:         store,
		logger:        logger,
		dropped:       dropped,
		snapshotLimit: protocol.BulkStringLimit,
		replicas:      make(map[uint64]*Replica),
		changed:       make(chan struct{}),
	}
}

// SetSnapshotLimit sets the largest full resync snapshot Attach serves, in
// bytes, which is protocol.BulkStringLimit until it is called: the most a
// Replica accepts in one bulk string. Call it before the Master serves; tests
// use it to refuse a small snapshot.
func (m *Master) SetSnapshotLimit(bytes int) {
	m.snapshotLimit = bytes
}

// ReplicationID is the Master's replication ID, which FULLRESYNC and INFO
// report.
func (m *Master) ReplicationID() string {
	return m.replicationID
}

// Offset reports the Master's replication offset.
func (m *Master) Offset() int64 {
	return m.offset.Load()
}

// Active reports whether a Replica is attached or attaching: whether anything
// will send a write made now to a Replica.
func (m *Master) Active() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.attaching > 0 || len(m.replicas) > 0
}

// Attach makes c a Replica at one attach cut, so that every write is in
// exactly one of the snapshot it is sent and the stream that follows. cut is
// the command sequencer's: it switches recording on by calling startRecording,
// and returns holding the locks under which no write sits between being
// applied and being propagated, with the function that gives them up.
//
// A Replica attaches in two steps:
//
//  1. beginAttach, which cut calls as startRecording. From then on Active is
//     true, so every write that begins after it is ordered, and the cut can
//     wait for it.
//  2. reserve, inside the cut, right after the snapshot is copied: the
//     Replica's Output is told to hold what is pushed to it, and the Replica
//     is registered. Every write after the snapshot is pushed to that Output
//     and waits there.
//
// Attach returns the reply, which the connection sends as the Reply to the
// request that asked to attach. That Reply releases what the Output held, so
// the stream follows the snapshot. The Master's lock is not held while cut
// runs, and the snapshot is encoded after the cut is released, so writers wait
// only for the size pass and the copy. If the copy panics, the attach ends and Active reports
// what it did before.
//
// When the snapshot would be larger than the snapshot limit (SetSnapshotLimit),
// which a Replica could not read, Attach returns a *SnapshotTooLargeError
// instead. It sizes the snapshot inside the cut from the lengths of the keys
// and values, before it copies any of them and before reserve, so a refusal
// copies nothing, c is never registered and its Output is told nothing, and
// no snapshot is encoded. The Store sizes and copies under one hold of its
// locks, so the entries served are the ones that were sized. The attach ends and
// Active reports what it did before.
func (m *Master) Attach(c Candidate, cut func(startRecording func()) (release func())) (*Attachment, error) {
	entries, stats, tooLarge := m.cutAndReserve(c, cut)
	if tooLarge != nil {
		m.logger.Warn(
			"refusing full resync: snapshot exceeds the bulk string limit",
			"replica_id", c.ID,
			"snapshot_size_bytes", tooLarge.Size,
			"limit_bytes", tooLarge.Limit,
		)
		return nil, tooLarge
	}
	snapshot, written := rdb.BuildSnapshot(entries)
	if stats.SkippedUnsupportedKeys > 0 {
		// RDB support is scoped to DB 0 string keys, so a Master holding
		// collections transfers only its string keyspace on full resync.
		m.logger.Warn(
			"full resync snapshot skipped unsupported value types",
			"skipped_unsupported_keys", stats.SkippedUnsupportedKeys,
			"exported_keys", stats.ExportedKeys,
		)
	}
	m.logger.Info(
		"serving full resync to replica",
		"replica_id", c.ID,
		"master_replid", m.replicationID,
		"snapshot_size_bytes", len(snapshot),
		"snapshot_keys", written.WrittenKeys,
	)

	return &Attachment{replicationID: m.replicationID, snapshot: snapshot}, nil
}

func (m *Master) cutAndReserve(c Candidate, cut func(startRecording func()) (release func())) ([]storage.StringSnapshotEntry, storage.StringSnapshotStats, *SnapshotTooLargeError) {
	release := cut(m.beginAttach)
	defer release()
	// An attach that does not reach reserve, refused or panicking, must end, or
	// the Master would count an attach that never ends, and order every write.
	reserved := false
	defer func() {
		if !reserved {
			m.endAttach()
		}
	}()

	var sizer rdb.SnapshotSizer
	fits := func() bool { return sizer.Size() <= m.snapshotLimit }
	entries, stats, copied := m.store.SnapshotStringsIf(sizer.Add, fits)
	if !copied {
		return nil, stats, &SnapshotTooLargeError{Size: sizer.Size(), Limit: m.snapshotLimit}
	}
	m.reserve(c)
	reserved = true
	return entries, stats, nil
}

// beginAttach records that a Replica is attaching, so Active reports true
// until the matching reserve or endAttach.
func (m *Master) beginAttach() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.attaching++
}

// endAttach ends an attach that beginAttach began and that will not reach
// reserve.
func (m *Master) endAttach() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.attaching--
}

// reserve registers a Replica that beginAttach began attaching, and ends that
// attach. Every frame counted from now on is pushed to its Output, so its base
// offset is the replication offset now.
//
// The Output is marked a Replica's and told to hold before the Replica is
// registered, and so before Propagate can find it: the first frame pushed to
// it is already counted against a Replica's limit, not a subscriber's, and
// already waits behind the full resync reply.
func (m *Master) reserve(c Candidate) {
	c.Out.MarkReplica()
	c.Out.HoldPushes()
	m.register(c, true)
}

// Add registers a Replica outside any attach cut, for a Replica that has no
// snapshot to order its stream against, as in tests. Its Output holds nothing,
// since no reply would release it. The server attaches Replicas through
// Attach. Outside the cut nothing orders Add against an ordered Propagate,
// which may count a frame without the lock while no Replica is registered, so
// Add must not run while frames are propagated that way.
func (m *Master) Add(c Candidate) {
	c.Out.MarkReplica()
	m.register(c, false)
}

func (m *Master) register(c Candidate, endsAttach bool) {
	replica := &Replica{ID: c.ID, ListeningPort: c.ListeningPort, out: c.Out}

	m.mu.Lock()
	defer m.mu.Unlock()
	// Frames are counted and pushed under this lock, so the base splits the
	// stream exactly: no frame counted before it is sent to the Replica, and
	// every frame counted after it is. Read before the lock, a frame counted in
	// between would lie above the base without being sent, and the Replica would
	// fall that many bytes short of every later write; read after it, a frame
	// could be sent and still lie below the base, and WAIT would count the
	// Replica for a write it has not acknowledged.
	replica.base = m.offset.Load()
	m.replicas[c.ID] = replica
	m.registered.Store(int64(len(m.replicas)))
	if endsAttach {
		m.attaching--
	}
	m.notifyChangedLocked()
}

// Propagate counts payload into the replication offset and queues it for every
// Replica, as one step under the Master's lock, and returns the offset at
// which payload ends. Two calls therefore reach every Replica in the order
// their offsets were counted, so a Replica that acknowledges offset N has
// every frame that ends at or before N, which WAIT relies on.
//
// ordered says that the command sequencer orders the caller against the attach
// cut: it holds the sequencer's gate, and the stripes of its write whenever
// anything records writes, from before the write is applied until this
// returns. While no Replica is registered such a call only counts payload,
// without the lock. That is safe because a Replica is registered only inside
// the attach cut, which first waits, holding the gate exclusively, for every
// write that skipped its stripes, so that every later write takes them, and
// then holds every stripe. Such a call therefore either returned before the
// cut, so its frame lies at or below the Replica's base and its write is in
// the snapshot, or begins after the cut and finds the Replica registered,
// taking the lock.
//
// A frame the sequencer does not order that way takes the lock even when no
// Replica is registered: Await's REPLCONF GETACK, or a write the server makes
// on its own, the DELs of a Background unit, which the Recorder never passes
// as ordered. Counted without it, such a frame could lie
// above the base of a Replica registering at that moment and never be queued
// for it.
//
// Queueing only appends to each Replica's Output, so the lock is held for
// copies, never for a socket write. An Output refuses payload once its
// connection is closed: the Replica left, or the Output closed it for falling
// behind (the Replica Push limit, the Replica stall rule). That Replica is
// detached under the same hold of the lock, so it is pushed nothing after the
// frame it refused, and the dropped handler is told once the lock is released.
// Once shutdown has begun (StopPropagating), Propagate only counts payload. It
// keeps no reference to payload.
func (m *Master) Propagate(payload []byte, ordered bool) (end int64) {
	return m.propagate(payload, ordered).endOffset
}

func (m *Master) propagate(payload []byte, ordered bool) propagationReport {
	if len(payload) == 0 {
		return propagationReport{}
	}

	report := propagationReport{payloadSize: len(payload)}
	if ordered && m.registered.Load() == 0 {
		report.endOffset = m.offset.Add(int64(len(payload)))
		return report
	}
	report.endOffset, report.succeeded, report.failed = m.countAndQueue(payload)
	report.attempted = report.succeeded + report.failed
	if report.attempted > 0 {
		m.logger.Debug(
			"propagated command to replicas",
			"attempted", report.attempted,
			"succeeded", report.succeeded,
			"failed", report.failed,
			"payload_size", report.payloadSize,
			"end_offset", report.endOffset,
		)
	}

	if report.failed > 0 {
		log := m.logger.Warn
		message := "propagation to replicas was partially successful"
		if report.succeeded == 0 && report.attempted > 0 {
			log = m.logger.Error
			message = "propagation to replicas failed for every replica"
		}
		log(message,
			"attempted", report.attempted,
			"succeeded", report.succeeded,
			"failed", report.failed,
			"payload_size", report.payloadSize,
		)
	}

	return report
}

// countAndQueue counts payload into the offset and queues it for every
// Replica under the lock, detaching the Replicas whose Output refused it, and
// then tells the dropped handler of those. It returns the offset at which
// payload ends, how many Replicas it was queued for, and how many refused it.
func (m *Master) countAndQueue(payload []byte) (end int64, queued, refused int) {
	// Refusals are rare, so the slice is only allocated when one happens.
	var refusals []refusal

	m.mu.Lock()
	end = m.offset.Add(int64(len(payload)))
	if m.stopping {
		m.mu.Unlock()
		return end, 0, 0
	}
	for id, replica := range m.replicas {
		if replica.out.Push(payload) {
			queued++
			continue
		}
		// Detached before the lock is released: the next frame is counted
		// under it too, and must not reach a Replica that is missing this one.
		m.detachLocked(id)
		refusals = append(refusals, refusal{replica: replica, cause: replica.out.Err()})
	}
	m.mu.Unlock()

	if m.dropped != nil {
		for _, detached := range refusals {
			m.dropped(detached.replica, detached.cause)
		}
	}

	return end, queued, len(refusals)
}

// StopPropagating makes Propagate push nothing more to any Replica, and
// returns how many Replicas are registered. Frames are still counted. The
// server calls it when shutdown begins: a write made from then on reaches no
// Replica, which has to synchronise again from the start anyway, and nothing
// is written past the end of a Replica's stream (EndStreams).
func (m *Master) StopPropagating() (replicas int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.stopping = true
	return len(m.replicas)
}

// EndStreams stops propagating (StopPropagating) and ends the stream of every
// registered Replica: its Output writes what is queued for it and then shuts
// the write side of its connection. EndStreams then waits until deadline for
// those Replicas to close their own side, which detaches them, and returns how
// many had not.
//
// The server calls it on shutdown, before it closes the sockets, so that
// commands already propagated are not lost to a graceful stop. Closing a
// Replica's socket outright while the end of its stream is still in the
// kernel's send queue would let the Replica's next acknowledgement draw a
// reset that discards it. A Replica still waiting for its full resync reply
// may be cut off before it: it has to synchronise again from the start anyway.
func (m *Master) EndStreams(deadline time.Time) (remaining int) {
	m.StopPropagating()
	ended := m.snapshot()
	for _, replica := range ended {
		replica.out.EndStream()
	}

	for {
		m.mu.RLock()
		remaining = 0
		for _, replica := range ended {
			if m.replicas[replica.ID] == replica {
				remaining++
			}
		}
		changed := m.changed
		m.mu.RUnlock()

		wait := time.Until(deadline)
		if remaining == 0 || wait <= 0 {
			return remaining
		}
		timer := time.NewTimer(wait)
		select {
		case <-changed:
		case <-timer.C:
		}
		timer.Stop()
	}
}

// Ack records the latest replication offset a Replica acknowledged, in the
// Replica's own count from when it attached. It reports whether id is a
// registered Replica.
func (m *Master) Ack(id uint64, offset int64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	replica, ok := m.replicas[id]
	if !ok {
		return false
	}
	if offset > replica.ack.Load() {
		replica.ack.Store(offset)
		m.notifyChangedLocked()
	}

	return true
}

// Acked reports how many Replicas have acknowledged at least target, an
// offset in the Master's count: those whose base offset plus acknowledgement
// reaches it.
func (m *Master) Acked(target int64) int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.ackedLocked(target)
}

// ackedWithNotify returns what Acked does and a channel that is closed
// whenever the Replicas or their acknowledgements change.
func (m *Master) ackedWithNotify(target int64) (int, <-chan struct{}) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.ackedLocked(target), m.changed
}

func (m *Master) ackedLocked(target int64) int {
	count := 0
	for _, replica := range m.replicas {
		if replica.acknowledgedMasterOffset() >= target {
			count++
		}
	}

	return count
}

// Await asks every Replica to acknowledge its offset (REPLCONF GETACK *) and
// waits until want of them have acknowledged at least target, an offset in the
// Master's count, or until timeout has passed. It returns how many had, and
// whether it gave up waiting. It holds no lock while it waits, and returns
// ctx's error if ctx ends first.
//
// GETACK is part of the stream and counts toward the offset like a write, so
// it is counted and queued in the same step as writes are; a Replica whose
// Output refuses it is detached the way propagation detaches one. Nothing
// orders Await against an attach cut, so it takes the lock even with no
// Replica registered.
func (m *Master) Await(ctx context.Context, want int64, target int64, timeout time.Duration) (acked int, timedOut bool, err error) {
	_, queued, refused := m.countAndQueue(getAckPayload)
	m.logger.Debug("requested replica acknowledgements", "replica_count", queued+refused, "payload_size", len(getAckPayload))
	if refused > 0 {
		m.logger.Warn("failed to request replica ACK", "refused", refused, "requested", queued+refused)
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		acked, notified := m.ackedWithNotify(target)
		if int64(acked) >= want {
			return acked, false, nil
		}

		select {
		case <-ctx.Done():
			return 0, false, ctx.Err()
		case <-timer.C:
			return m.Acked(target), true, nil
		case <-notified:
		}
	}
}

// acknowledgedMasterOffset is how far the Replica has acknowledged the stream,
// in the Master's count: its base plus what it acknowledged. WAIT counts
// Replicas by it and INFO reports it, as Redis does with repl_ack_off. Until
// the Replica acknowledges something it is 0, as repl_ack_off starts, so it
// counts only for a target of 0; the base alone would count a Replica that may
// not have loaded the snapshot yet.
func (r *Replica) acknowledgedMasterOffset() int64 {
	acknowledged := r.ack.Load()
	if acknowledged <= 0 {
		return 0
	}
	// A Replica that claims more than the Master could have sent it is placed
	// at the largest offset instead of wrapping negative.
	if acknowledged > math.MaxInt64-r.base {
		return math.MaxInt64
	}

	return r.base + acknowledged
}

// Detach removes a Replica, which is pushed nothing from then on. It returns
// the Replica, or nil when id is not one. A Replica leaves once: when its
// connection is torn down, which calls Detach, or before that when its Output
// refuses a frame (Propagate). Only the first of the two finds it.
func (m *Master) Detach(id uint64) *Replica {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.detachLocked(id)
}

func (m *Master) detachLocked(id uint64) *Replica {
	replica := m.replicas[id]
	if replica == nil {
		return nil
	}
	delete(m.replicas, id)
	m.registered.Store(int64(len(m.replicas)))
	m.notifyChangedLocked()
	return replica
}

// snapshot returns the Replicas registered now.
func (m *Master) snapshot() []*Replica {
	m.mu.RLock()
	defer m.mu.RUnlock()

	replicas := make([]*Replica, 0, len(m.replicas))
	for _, replica := range m.replicas {
		replicas = append(replicas, replica)
	}

	return replicas
}

// Count reports how many Replicas are registered.
func (m *Master) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return len(m.replicas)
}

// Info is what INFO replication reports: the replication ID, the offset, and
// each Replica with the offset it has acknowledged in the Master's count.
func (m *Master) Info() MasterInfo {
	replicas := m.snapshot()
	info := MasterInfo{
		ReplicationID: m.replicationID,
		Offset:        m.offset.Load(),
		Replicas:      make([]ReplicaInfo, 0, len(replicas)),
	}
	for _, replica := range replicas {
		info.Replicas = append(info.Replicas, ReplicaInfo{
			ID:                replica.ID,
			ListeningPort:     replica.ListeningPort,
			AckedMasterOffset: replica.acknowledgedMasterOffset(),
		})
	}
	return info
}

func (m *Master) notifyChangedLocked() {
	close(m.changed)
	m.changed = make(chan struct{})
}

func randomReplicationID() string {
	var raw [20]byte
	if _, err := cryptorand.Read(raw[:]); err != nil {
		// crypto/rand only fails when the OS entropy source is broken. A
		// replication ID must be unpredictable, so fail loudly at startup rather
		// than fall back to a guessable time-derived value.
		panic(fmt.Sprintf("replication: read crypto/rand for replication ID: %v", err))
	}

	return hex.EncodeToString(raw[:])
}
