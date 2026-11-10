package replication

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/rdb"
	"github.com/maltemindedal/stash/internal/storage"
)

// Master is the component through which a Master serves its Replicas. It owns
// the replication offset, each Replica's base offset, feed and
// acknowledgements, and attaching a Replica at one attach cut.
//
// Its lock comes after the command sequencer's gate and write stripes, and
// before a replica feed's lock: lock order is gate, stripes, mu, feed. While
// holding mu the Master takes no gate, stripe or shard lock, and calls no
// drop handler, since the handler takes mu itself (Detach). It never holds mu
// while it runs an attach cut: the cut takes mu under the gate (beginAttach)
// and Attach takes it again under every stripe (reserve), each time for a few
// instructions. Await waits holding nothing.
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

	// drop is called, with mu not held, when a Replica can no longer be fed: on
	// the feed's own goroutine when a write to its socket fails, and on the
	// propagating goroutine when its feed refuses a frame. When it is nil the
	// Master detaches the Replica and closes its connection itself.
	drop func(id uint64, cause error)

	mu       sync.RWMutex
	replicas map[uint64]*Replica
	// feeds counts the feed goroutines Start started and that have not yet
	// returned from the drop handler (WaitFeeds).
	feeds sync.WaitGroup
	// registered is len(replicas), stored under mu whenever it changes, so that
	// an ordered Propagate can find out without mu that no Replica is registered.
	registered atomic.Int64
	changed    chan struct{}
	// attaching counts the Replicas between beginAttach and their reserve or
	// endAttach, which Active counts as Replicas already.
	attaching int
	// stopping is set by StopPropagating once the server is shutting down and
	// drains what each feed holds. Frames are then counted but queued for no
	// Replica: a feed closed for the drain would refuse them, and the refusal
	// would drop the Replica and close its socket in the middle of the drain.
	stopping bool
}

// Replica is a Replica attached to the Master.
type Replica struct {
	ID            uint64
	ListeningPort int
	// Conn is the Replica's connection. It is nil from the attach cut until the
	// Replica's feed starts (Master.Start), while its full resync reply is
	// being sent; Start sets it under the Master's lock while the Replica is
	// registered, so a Replica that Detach returned is read without the lock.
	Conn io.Closer

	// ack is the latest replication offset the Replica acknowledged, in its own
	// count: the bytes of the stream it has processed since it attached. It is
	// written under the Master's lock but read without it, so it is atomic.
	ack atomic.Int64
	// base is the Master's replication offset at the Replica's attach cut. The
	// Replica is sent every frame counted after that and counts from zero, so
	// its acknowledged offset in the Master's count is base+ack. It is set
	// before the Replica is registered and never changes.
	base int64

	out  Output
	feed *replicaFeed
}

// Output is where a Replica's feed hands its stream: the Replica's client
// connection, whose own output writes it to the socket and drops a Replica
// that does not take it in time. It keeps no reference to payload.
type Output interface {
	WriteEncoded(payload []byte) error
}

// Candidate is a client that asks to become a Replica.
type Candidate struct {
	ID uint64
	// ListeningPort is the port the client announced with REPLCONF
	// listening-port, or 0.
	ListeningPort int
	Out           Output
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

// replicaRefusal is a Replica that refused a frame, kept until the Master's
// lock is released so that it can be dropped.
type replicaRefusal struct {
	id  uint64
	err error
}

// getAckPayload is the REPLCONF GETACK * that asks every Replica how far it
// has got.
var getAckPayload = []byte("*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n")

// NewMaster returns a Master with a new replication ID and no Replica. A full
// resync copies store. drop is called when a Replica can no longer be fed, to
// detach it (Detach) and close its connection; with nil the Master does both
// itself.
func NewMaster(store *storage.Store, drop func(id uint64, cause error), logger *slog.Logger) *Master {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Master{
		replicationID: randomReplicationID(),
		store:         store,
		logger:        logger,
		drop:          drop,
		replicas:      make(map[uint64]*Replica),
		changed:       make(chan struct{}),
	}
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
// A Replica attaches in three steps:
//
//  1. beginAttach, which cut calls as startRecording. From then on Active is
//     true, so every write that begins after it is ordered, and the cut can
//     wait for it.
//  2. reserve, inside the cut, right after the snapshot is copied: the Replica
//     is registered with a feed that queues every write after the snapshot,
//     but does not write yet.
//  3. Start, once the reply is ahead of anything the feed writes on the
//     connection: the feed starts writing.
//
// Attach does the first two and returns the reply to send. The Master's lock
// is not held while cut runs, and the snapshot is encoded after the cut is
// released, so writers wait only for the copy. If the copy panics, the attach
// ends and Active reports what it did before.
func (m *Master) Attach(c Candidate, cut func(startRecording func()) (release func())) *Attachment {
	entries, stats := m.cutAndReserve(c, cut)
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

	return &Attachment{replicationID: m.replicationID, snapshot: snapshot}
}

func (m *Master) cutAndReserve(c Candidate, cut func(startRecording func()) (release func())) ([]storage.StringSnapshotEntry, storage.StringSnapshotStats) {
	release := cut(m.beginAttach)
	defer release()
	// Nothing below returns an error before reserve, but a panic would leave the
	// Master counting an attach that never ends, and every write ordered.
	reserved := false
	defer func() {
		if !reserved {
			m.endAttach()
		}
	}()

	entries, stats := m.store.SnapshotStrings()
	m.reserve(c)
	reserved = true
	return entries, stats
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
// attach. Its feed queues every frame counted from now on, so its base offset
// is the replication offset now, but writes nothing until Start.
func (m *Master) reserve(c Candidate) {
	m.register(c, true)
}

// Add registers a Replica and starts its feed at once, outside any attach cut:
// reserve and Start in one step, for a Replica that has no snapshot to order
// its stream against, as in tests. The server attaches Replicas through Attach
// and Start. Outside the cut nothing orders Add against an ordered Propagate,
// which may count a frame without the lock while no Replica is registered, so
// Add must not run while frames are propagated that way.
func (m *Master) Add(c Candidate, conn io.Closer) {
	m.register(c, false)
	m.Start(c.ID, conn)
}

func (m *Master) register(c Candidate, endsAttach bool) {
	replica := &Replica{ID: c.ID, ListeningPort: c.ListeningPort, out: c.Out, feed: newReplicaFeed(replicaFeedLimit)}

	m.mu.Lock()
	// Frames are counted and queued under this lock, so the base splits the
	// stream exactly: no frame counted before it is sent to the Replica, and
	// every frame counted after it is. Read before the lock, a frame counted in
	// between would lie above the base without being sent, and the Replica would
	// fall that many bytes short of every later write; read after it, a frame
	// could be sent and still lie below the base, and WAIT would count the
	// Replica for a write it has not acknowledged.
	replica.base = m.offset.Load()
	previous := m.replicas[c.ID]
	m.replicas[c.ID] = replica
	m.registered.Store(int64(len(m.replicas)))
	if endsAttach {
		m.attaching--
	}
	m.notifyChangedLocked()
	m.mu.Unlock()

	if previous != nil {
		previous.feed.close(0)
	}
}

// Start sets the connection of a Replica that Attach registered and starts its
// feed, which then writes what it queued since the attach cut and everything
// after. The caller calls it once the full resync reply is ahead of anything
// the feed writes on the connection. It reports false when the Replica is no
// longer registered, or its feed was closed, because it was dropped while its
// reply was sent; the caller then closes conn.
func (m *Master) Start(id uint64, conn io.Closer) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	replica := m.replicas[id]
	if replica == nil {
		return false
	}
	m.feeds.Add(1)
	stopped := func(err error) {
		defer m.feeds.Done()
		// With no drop handler a Replica whose socket failed stays registered
		// until its connection's teardown detaches it.
		if err != nil && m.drop != nil {
			m.drop(id, err)
		}
	}
	// Set before the feed starts, so that a drop caused by its first write
	// finds the connection to close.
	replica.Conn = conn
	if !replica.feed.start(replica.send, stopped) {
		m.feeds.Done()
		replica.Conn = nil
		return false
	}
	return true
}

// dropReplica drops a Replica that can no longer be fed, with mu not held:
// through the drop handler, or by detaching and closing it when none is set.
func (m *Master) dropReplica(id uint64, cause error) {
	if m.drop != nil {
		m.drop(id, cause)
		return
	}
	// With no handler there is nowhere to report a failed close; the server
	// always sets one, which logs it.
	_ = m.detachAndClose(id)
}

// enqueue queues a pre-encoded RESP payload for the Replica, which its feed
// writes to its socket in order. It does not wait for the socket, and fails
// only if the Replica is gone or has fallen too far behind.
func (r *Replica) enqueue(payload []byte) error {
	if r.out == nil {
		return fmt.Errorf("replica response writer unavailable")
	}

	return r.feed.enqueue(payload)
}

// send hands one stretch of the stream to the Replica's Output.
func (r *Replica) send(chunk []byte) error {
	return r.out.WriteEncoded(chunk)
}

// WaitFeeds waits until the goroutine of every feed Start started has ended,
// including the drop handler it called, so that nothing a feed does, such as
// logging why its Replica was dropped, comes after it returns. A feed ends
// once its Replica is detached or its connection closes, so the server calls
// it once every connection has been torn down, before ListenAndServe returns.
func (m *Master) WaitFeeds() {
	m.feeds.Wait()
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
// Queueing only appends to each Replica's feed, so the lock is held for
// copies, never for a socket write. A Replica whose feed refuses payload (it
// is closed, or too far behind) is sent nothing after it, and is dropped once
// the lock is released. Once shutdown has begun (StopPropagating), Propagate
// only counts payload. It keeps no reference to payload.
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
// Replica under the lock, then drops the Replicas that refused it. It returns
// the offset at which payload ends, how many Replicas it was queued for, and
// how many refused it.
func (m *Master) countAndQueue(payload []byte) (end int64, queued, refused int) {
	// Refusals are rare, so the slice is only allocated when one happens.
	var refusals []replicaRefusal

	m.mu.Lock()
	end = m.offset.Add(int64(len(payload)))
	if m.stopping {
		m.mu.Unlock()
		return end, 0, 0
	}
	for id, replica := range m.replicas {
		if err := replica.enqueue(payload); err != nil {
			// Close the feed so that it refuses every later frame as well. A
			// smaller frame could otherwise still fit under the backlog limit
			// before the Replica is dropped, and reach the Replica at a position
			// other than the offset counted for it.
			replica.feed.close(0)
			refusals = append(refusals, replicaRefusal{id: id, err: err})
			continue
		}
		queued++
	}
	m.mu.Unlock()

	// Dropping takes the lock, so it runs after the lock is released.
	for _, refusal := range refusals {
		m.dropReplica(refusal.id, refusal.err)
	}

	return end, queued, len(refusals)
}

// StopPropagating makes Propagate queue nothing more for any Replica, and
// returns how many Replicas are registered. Frames are still counted. The
// server calls it when shutdown begins: a write made from then on reaches no
// Replica, which has to synchronise again from the start anyway, and no
// Replica is dropped for it while its feed drains.
func (m *Master) StopPropagating() (replicas int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.stopping = true
	return len(m.replicas)
}

// StopFeeds stops propagating (StopPropagating), gives every Replica's feed up
// to grace to write what is queued for it and then stops them, returning how
// many had not finished. The server calls it on shutdown, before it closes the
// sockets, so that commands already propagated are not lost to a graceful
// stop. A feed that has not started, because its Replica is still being sent
// its full resync, is stopped without waiting: that Replica has to synchronise
// again from the start anyway.
func (m *Master) StopFeeds(grace time.Duration) (unfinished int) {
	m.StopPropagating()
	deadline := time.Now().Add(grace)
	for _, replica := range m.snapshot() {
		if !replica.feed.close(time.Until(deadline)) {
			unfinished++
		}
	}
	return unfinished
}

// StreamEnder is a Replica's connection whose write side can be shut on its
// own: a TCP connection in the default networking mode, the event loop's
// handle in the other.
type StreamEnder interface {
	CloseWrite() error
}

// EndStreams shuts the write side of every started Replica's connection, once
// StopFeeds has sent them what was queued, and waits until deadline for those
// Replicas to close their own side, which detaches them. It returns how many
// had not. Closing a Replica's socket outright while the end of its stream is
// still in the kernel's send queue would let the Replica's next
// acknowledgement draw a reset that discards it.
func (m *Master) EndStreams(deadline time.Time) (remaining int) {
	m.mu.RLock()
	enders := make(map[uint64]StreamEnder, len(m.replicas))
	for id, replica := range m.replicas {
		if ender, ok := replica.Conn.(StreamEnder); ok {
			enders[id] = ender
		}
	}
	m.mu.RUnlock()

	for id, ender := range enders {
		// One that fails is closed with every other connection afterwards.
		if err := ender.CloseWrite(); err != nil {
			delete(enders, id)
		}
	}

	for {
		m.mu.RLock()
		remaining = 0
		for id := range enders {
			if _, ok := m.replicas[id]; ok {
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
// it is counted and queued in the same step as writes are; a Replica that
// refuses it is dropped the way propagation drops one. Nothing orders Await
// against an attach cut, so it takes the lock even with no Replica registered.
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

// Detach removes a Replica and stops its feed. It returns the Replica, or nil
// when id is not one: every way a Replica leaves goes through it, so only the
// first finds it.
func (m *Master) Detach(id uint64) *Replica {
	m.mu.Lock()
	defer m.mu.Unlock()

	replica := m.replicas[id]
	delete(m.replicas, id)
	m.registered.Store(int64(len(m.replicas)))
	if replica != nil {
		m.notifyChangedLocked()
		replica.feed.close(0)
	}
	return replica
}

// detachAndClose detaches a Replica and closes its socket. A Replica whose
// feed has not started has no connection here yet; Start then reports that it
// was dropped, and its caller closes the connection.
func (m *Master) detachAndClose(id uint64) error {
	replica := m.Detach(id)
	if replica != nil && replica.Conn != nil {
		if err := replica.Conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			return err
		}
	}

	return nil
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
