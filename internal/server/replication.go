package server

import (
	"bufio"
	"bytes"
	"context"
	cryptorand "crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/rdb"
	"github.com/maltemindedal/stash/internal/storage"
)

// ExecuteResult describes the full set of RESP frames emitted by a command.
type ExecuteResult struct {
	Responses       []protocol.Value
	UpstreamReplies []protocol.Value
	Propagation     []protocol.Value
	Durability      []protocol.Value
	RegisterReplica bool
	// Release, when set, must be called by whoever applies Durability and
	// Propagation, after it has done so. It ends the exclusion that keeps other
	// writes (and transactions) from running until those frames are in the log
	// and on their way to replicas. It is set only by an executor's sequenced
	// entry point.
	Release func()
}

// SingleResponse wraps a standard single RESP value as an execution result.
func SingleResponse(value protocol.Value) ExecuteResult {
	return MultiResponse(value)
}

// MultiResponse wraps multiple RESP values to be written in order.
func MultiResponse(values ...protocol.Value) ExecuteResult {
	responses := make([]protocol.Value, len(values))
	copy(responses, values)
	return ExecuteResult{Responses: responses}
}

// DeleteFrame builds the `DEL` command frame for keys the server removed without
// a client command. The command executor uses it for memory-pressure evictions.
// The server uses it for active TTL evictions. Both callers propagate and
// persist the same frame.
func DeleteFrame(keys []string) protocol.Array {
	elements := make([]protocol.Value, 0, len(keys)+1)
	elements = append(elements, protocol.TextBulkString{Value: "DEL"})
	for _, key := range keys {
		elements = append(elements, protocol.BulkString{Data: []byte(key)})
	}

	return protocol.Array{Elements: elements}
}

type replicationOriginContextKey struct{}

// WithReplicationOrigin marks a command as originating from the upstream replication stream.
func WithReplicationOrigin(ctx context.Context) context.Context {
	return context.WithValue(ctx, replicationOriginContextKey{}, true)
}

// IsReplicationOrigin reports whether the current command came from the upstream replication stream.
func IsReplicationOrigin(ctx context.Context) bool {
	origin, _ := ctx.Value(replicationOriginContextKey{}).(bool)
	return origin
}

// ReplicationState stores process-wide replication metadata.
type ReplicationState struct {
	MasterReplicationID string
	masterOffset        atomic.Int64
	replicaOffset       atomic.Int64
}

// ReplicaPeer describes a replica connection attached to a master.
type ReplicaPeer struct {
	ID            uint64
	Conn          ClientConn
	ListeningPort int
	// AckOffset is the latest replication offset the replica acknowledged. It is
	// written under the registry lock but read through the peers Snapshot returns,
	// so it is atomic.
	AckOffset atomic.Int64

	writer encodedReplicaWriter
	feed   *replicaFeed
}

type encodedReplicaWriter interface {
	WriteEncoded([]byte) error
}

// deadlineReplicaWriter is a writer that can bound how long one write may take.
type deadlineReplicaWriter interface {
	WriteEncodedWithDeadline([]byte, time.Duration) error
}

type propagationReport struct {
	attempted   int
	succeeded   int
	failed      int
	payloadSize int
	endOffset   int64
}

// WriteEncoded queues a pre-encoded RESP payload for the replica, which the peer's
// feed writes to its socket in order. It does not wait for the socket, and fails
// only if the replica is gone or has fallen too far behind.
func (p *ReplicaPeer) WriteEncoded(payload []byte) error {
	if p == nil || p.writer == nil || p.feed == nil {
		return fmt.Errorf("replica response writer unavailable")
	}

	return p.feed.enqueue(payload)
}

// send writes one stretch of the stream to the replica's socket, giving up if the
// replica does not take it in time.
func (p *ReplicaPeer) send(chunk []byte) error {
	if writer, ok := p.writer.(deadlineReplicaWriter); ok {
		return writer.WriteEncodedWithDeadline(chunk, replicaFeedWriteTimeout)
	}
	return p.writer.WriteEncoded(chunk)
}

// ReplicaRegistry tracks replica peers connected to a master server.
//
// Its lock comes after the command sequencer's gate and write stripes, and before
// a replica feed's lock: lock order is gate, stripes, mu, feed. While holding mu
// the registry takes no gate, stripe or shard lock, and calls no handler, since
// the feed-error handler takes mu itself.
type ReplicaRegistry struct {
	mu       sync.RWMutex
	replicas map[uint64]*ReplicaPeer
	changed  chan struct{}

	// onFeedError is called, with mu not held, when a replica can no longer be
	// fed: on the feed's own goroutine when a write to its socket fails, and on
	// the propagating goroutine when its feed refuses a frame (Propagate).
	onFeedError func(id uint64, err error)
}

// replicaRefusal is a replica that refused a frame Propagate queued, kept until
// the registry lock is released so that it can be dropped.
type replicaRefusal struct {
	id  uint64
	err error
}

func newReplicationState() *ReplicationState {
	return &ReplicationState{MasterReplicationID: randomReplicationID()}
}

// MasterOffset reports the master's current logical replication offset.
func (s *ReplicationState) MasterOffset() int64 {
	if s == nil {
		return 0
	}

	return s.masterOffset.Load()
}

// AdvanceMasterOffset increments the master's logical replication offset.
func (s *ReplicationState) AdvanceMasterOffset(delta int64) int64 {
	if s == nil || delta <= 0 {
		return s.MasterOffset()
	}

	return s.masterOffset.Add(delta)
}

// ReplicaOffset reports the replica's processed upstream replication offset.
func (s *ReplicationState) ReplicaOffset() int64 {
	if s == nil {
		return 0
	}

	return s.replicaOffset.Load()
}

// ResetReplicaOffset starts the replica's processed offset again from zero, as it
// must when a full resynchronisation begins a new stream.
func (s *ReplicationState) ResetReplicaOffset() {
	if s == nil {
		return
	}

	s.replicaOffset.Store(0)
}

// AdvanceReplicaOffset increments the replica's processed upstream replication offset.
func (s *ReplicationState) AdvanceReplicaOffset(delta int64) int64 {
	if s == nil || delta <= 0 {
		return s.ReplicaOffset()
	}

	return s.replicaOffset.Add(delta)
}

// NewReplicaRegistry creates an empty registry of replica peers.
func NewReplicaRegistry() *ReplicaRegistry {
	return &ReplicaRegistry{replicas: make(map[uint64]*ReplicaPeer), changed: make(chan struct{})}
}

// Add stores or updates a replica peer.
func (r *ReplicaRegistry) Add(id uint64, conn ClientConn, listeningPort int, writer encodedReplicaWriter) {
	peer := &ReplicaPeer{ID: id, Conn: conn, ListeningPort: listeningPort, writer: writer, feed: newReplicaFeed(replicaFeedLimit)}

	r.mu.Lock()
	previous := r.replicas[id]
	r.replicas[id] = peer
	onFeedError := r.onFeedError
	r.notifyChangedLocked()
	r.mu.Unlock()

	if previous != nil {
		previous.feed.close(0)
	}
	go func() {
		if err := peer.feed.run(peer.send); err != nil && onFeedError != nil {
			onFeedError(id, err)
		}
	}()
}

// Propagate counts payload into the master's replication offset and queues it
// for every replica, as one step under the registry lock. Two calls therefore
// reach every replica in the order their offsets were counted, so a replica that
// acknowledges offset N has every frame that ends at or before N, which WAIT
// relies on. It returns the offset at which payload ends, how many replicas it
// was queued for, and how many refused it.
//
// The lock is taken even when no replica is attached: a frame counted without
// it could miss a replica registering at the same moment.
//
// Queueing only appends to each replica's feed, so the lock is held for copies,
// never for a socket write. A replica whose feed refuses payload (it is closed,
// or too far behind) is sent nothing after it, and is dropped once the lock is
// released, through the feed-error handler, or by RemoveAndClose when none is
// set.
func (r *ReplicaRegistry) Propagate(offsets *ReplicationState, payload []byte) (end int64, queued, refused int) {
	// Refusals are rare, so the slice is only allocated when one happens.
	var refusals []replicaRefusal

	r.mu.Lock()
	end = offsets.AdvanceMasterOffset(int64(len(payload)))
	for id, peer := range r.replicas {
		if err := peer.WriteEncoded(payload); err != nil {
			// Close the feed so that it refuses every later frame as well. A
			// smaller frame could otherwise still fit under the backlog limit
			// before the replica is dropped, and reach the replica at a position
			// other than the offset counted for it.
			if peer.feed != nil {
				peer.feed.close(0)
			}
			refusals = append(refusals, replicaRefusal{id: id, err: err})
			continue
		}
		queued++
	}
	onFeedError := r.onFeedError
	r.mu.Unlock()

	// Both drop paths take the registry lock, so they run after it is released.
	for _, refusal := range refusals {
		if onFeedError != nil {
			onFeedError(refusal.id, refusal.err)
			continue
		}
		// With no handler there is nowhere to report a failed close; the server
		// always sets one (dropReplica), which logs it.
		_ = r.RemoveAndClose(refusal.id)
	}

	return end, queued, len(refusals)
}

// SetFeedErrorHandler registers what happens when writing a replica's stream
// fails: the server drops the replica.
func (r *ReplicaRegistry) SetFeedErrorHandler(handler func(id uint64, err error)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.onFeedError = handler
}

// StopFeeds gives every replica's feed up to grace to write what is queued for it
// and then stops them, returning how many had not finished. The server calls it
// on shutdown, before it closes the sockets, so that commands already propagated
// are not lost to a graceful stop.
func (r *ReplicaRegistry) StopFeeds(grace time.Duration) (unfinished int) {
	peers := r.Snapshot()
	deadline := time.Now().Add(grace)
	for _, peer := range peers {
		if peer.feed != nil && !peer.feed.close(time.Until(deadline)) {
			unfinished++
		}
	}
	return unfinished
}

// UpdateAck records the latest processed replication offset for a replica peer.
func (r *ReplicaRegistry) UpdateAck(id uint64, offset int64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	peer, ok := r.replicas[id]
	if !ok {
		return false
	}
	if offset > peer.AckOffset.Load() {
		peer.AckOffset.Store(offset)
		r.notifyChangedLocked()
	}

	return true
}

// CountReplicasAtOrAbove reports how many replicas have acknowledged at least targetOffset.
func (r *ReplicaRegistry) CountReplicasAtOrAbove(targetOffset int64) int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.countReplicasAtOrAboveLocked(targetOffset)
}

// CountReplicasAtOrAboveWithNotify returns the current matching replica count and a
// notification channel that is closed whenever replica acknowledgement state changes.
func (r *ReplicaRegistry) CountReplicasAtOrAboveWithNotify(targetOffset int64) (int, <-chan struct{}) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.countReplicasAtOrAboveLocked(targetOffset), r.changed
}

func (r *ReplicaRegistry) countReplicasAtOrAboveLocked(targetOffset int64) int {
	count := 0
	for _, peer := range r.replicas {
		if peer.AckOffset.Load() >= targetOffset {
			count++
		}
	}

	return count
}

// Remove deletes a replica peer from the registry.
func (r *ReplicaRegistry) Remove(id uint64) *ReplicaPeer {
	r.mu.Lock()
	defer r.mu.Unlock()

	peer := r.replicas[id]
	delete(r.replicas, id)
	if peer != nil {
		r.notifyChangedLocked()
		if peer.feed != nil {
			peer.feed.close(0)
		}
	}
	return peer
}

// RemoveAndClose deletes a replica peer from the registry and closes its socket.
func (r *ReplicaRegistry) RemoveAndClose(id uint64) error {
	peer := r.Remove(id)
	if peer != nil {
		if err := peer.Conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			return err
		}
	}

	return nil
}

// Snapshot returns the currently tracked replica peers.
func (r *ReplicaRegistry) Snapshot() []*ReplicaPeer {
	r.mu.RLock()
	defer r.mu.RUnlock()

	peers := make([]*ReplicaPeer, 0, len(r.replicas))
	for _, peer := range r.replicas {
		peers = append(peers, peer)
	}

	return peers
}

// Count reports the number of tracked replica peers.
func (r *ReplicaRegistry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return len(r.replicas)
}

func (r *ReplicaRegistry) notifyChangedLocked() {
	close(r.changed)
	r.changed = make(chan struct{})
}

func randomReplicationID() string {
	var raw [20]byte
	if _, err := cryptorand.Read(raw[:]); err != nil {
		// crypto/rand only fails when the OS entropy source is broken. A
		// replication ID must be unpredictable, so fail loudly at startup rather
		// than fall back to a guessable time-derived value.
		panic(fmt.Sprintf("server: read crypto/rand for replication ID: %v", err))
	}

	return hex.EncodeToString(raw[:])
}

func (s *Server) registerReplicaPeer(clientID uint64, conn ClientConn) {
	state := s.getClientState(clientID)
	if state == nil || !state.IsReplica() {
		return
	}

	s.replicaPeers.Add(clientID, conn, state.ReplicaListeningPort(), state)
	s.logger.Info(
		"replica registered",
		"replica_id", clientID,
		"remote_addr", conn.RemoteAddr().String(),
		"listening_port", state.ReplicaListeningPort(),
		"master_offset", s.replication.MasterOffset(),
	)
}

func (s *Server) propagateToReplicas(values []protocol.Value) propagationReport {
	if len(values) == 0 {
		return propagationReport{}
	}

	payload, err := protocol.EncodeValues(values)
	if err != nil {
		s.logger.Warn("failed to encode propagated command", "error", err)
		return propagationReport{}
	}

	report := propagationReport{payloadSize: len(payload)}
	report.endOffset, report.succeeded, report.failed = s.replicaPeers.Propagate(s.replication, payload)
	report.attempted = report.succeeded + report.failed
	if report.attempted > 0 {
		s.logger.Debug(
			"propagated command to replicas",
			"attempted", report.attempted,
			"succeeded", report.succeeded,
			"failed", report.failed,
			"payload_size", report.payloadSize,
			"end_offset", report.endOffset,
		)
	}

	if report.failed > 0 {
		log := s.logger.Warn
		message := "propagation to replicas was partially successful"
		if report.succeeded == 0 && report.attempted > 0 {
			log = s.logger.Error
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

// dropReplica logs why a replica can no longer be fed and closes it. It has to
// synchronise again from the start. It is the registry's feed-error handler, so
// it is called with the registry lock not held, and takes it.
func (s *Server) dropReplica(id uint64, cause error) {
	peer := s.replicaPeers.Remove(id)
	if peer == nil {
		return
	}
	s.logger.Warn("dropping a replica: it can no longer be fed the propagated stream", "replica_id", id, "error", cause)
	if err := peer.Conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		s.logger.Debug("failed to close replica after propagation failure", "replica_id", id, "error", err)
	}
}

func (s *Server) recordClientWriteOffset(ctx context.Context, offset int64) {
	if offset <= 0 {
		return
	}

	state, ok := ClientStateFromContext(ctx)
	if !ok || state == nil {
		return
	}

	state.SetLastWriteReplicationOffset(offset)
}

func (s *Server) setUpstreamConn(conn net.Conn) {
	s.upstreamConnMu.Lock()
	defer s.upstreamConnMu.Unlock()

	s.upstreamConn = conn
}

func (s *Server) clearUpstreamConn(conn net.Conn) {
	s.upstreamConnMu.Lock()
	defer s.upstreamConnMu.Unlock()

	if s.upstreamConn == conn {
		s.upstreamConn = nil
	}
}

func (s *Server) closeUpstreamConn() {
	s.upstreamConnMu.Lock()
	conn := s.upstreamConn
	s.upstreamConn = nil
	s.upstreamConnMu.Unlock()

	if conn != nil {
		if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			s.logger.Debug("failed to close upstream connection", "error", err)
		}
	}
}

// How long a replica waits before trying its master again, and the most that
// wait grows to.
const (
	replicaReconnectInitial = time.Second
	replicaReconnectMax     = 30 * time.Second
)

// startReplicaLink keeps the replica attached to its master: it connects,
// synchronises, applies the stream, and when the link ends for any reason other
// than shutdown it waits and starts again from a full resynchronisation. The wait
// doubles up to replicaReconnectMax and starts over after a link that got as far
// as a completed handshake. Only an unusable configuration ends it.
func (s *Server) startReplicaLink(ctx context.Context, listenerAddr string) {
	defer s.handlerWG.Done()

	masterAddr, err := s.cfg.ReplicaAddress()
	if err != nil {
		s.logger.Error("replica mode configuration invalid", "error", err)
		return
	}

	listeningPort, err := parseListenerPort(listenerAddr)
	if err != nil {
		s.logger.Error("failed to determine replica listening port", "address", listenerAddr, "error", err)
		return
	}

	delay := replicaReconnectInitial
	for {
		synchronised := s.runReplicaLink(ctx, masterAddr, listeningPort)
		if ctx.Err() != nil {
			return
		}
		if synchronised {
			delay = replicaReconnectInitial
		}

		s.logger.Warn("the link to the master ended; retrying", "master_addr", masterAddr, "retry_in", delay)
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return
		}
		delay = min(delay*2, replicaReconnectMax)
	}
}

// runReplicaLink makes one attempt to attach to the master and applies its
// stream until the link ends. It reports whether the handshake completed.
func (s *Server) runReplicaLink(ctx context.Context, masterAddr string, listeningPort int) (synchronised bool) {
	dialer := net.Dialer{}
	conn, err := dialer.DialContext(ctx, "tcp", masterAddr)
	if err != nil {
		s.logger.Error("failed to connect to master", "master_addr", masterAddr, "error", err)
		return
	}
	s.setUpstreamConn(conn)
	defer s.clearUpstreamConn(conn)
	defer func() {
		if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			s.logger.Debug("failed to close replica upstream connection", "master_addr", masterAddr, "error", err)
		}
	}()

	stopClose := context.AfterFunc(ctx, func() {
		if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			s.logger.Debug("failed to close replica upstream connection after cancellation", "master_addr", masterAddr, "error", err)
		}
	})
	defer stopClose()

	parser := protocol.NewParser(conn)
	writer := bufio.NewWriter(conn)

	if err := writeReplicaCommand(writer, "PING"); err != nil {
		s.logger.Error("failed to send replica PING", "master_addr", masterAddr, "error", err)
		return
	}
	if err := expectSimpleString(parser, "PONG"); err != nil {
		s.logger.Error("invalid PING response from master", "master_addr", masterAddr, "error", err)
		return
	}

	if s.cfg.MasterAuth != "" {
		if err := writeReplicaCommand(writer, "AUTH", s.cfg.MasterAuth); err != nil {
			s.logger.Error("failed to send replica AUTH", "master_addr", masterAddr, "error", err)
			return
		}
		if err := expectReplicaAuthOK(parser); err != nil {
			s.logger.Error("replica AUTH failed", "master_addr", masterAddr, "error", err)
			return
		}
	}

	if err := writeReplicaCommand(writer, "REPLCONF", "listening-port", strconv.Itoa(listeningPort)); err != nil {
		s.logger.Error("failed to send replica REPLCONF", "master_addr", masterAddr, "error", err)
		return
	}
	if err := expectReplicaReplConfOK(parser); err != nil {
		s.logger.Error("replica REPLCONF failed", "master_addr", masterAddr, "error", err)
		return
	}

	if err := writeReplicaCommand(writer, "PSYNC", "?", "-1"); err != nil {
		s.logger.Error("failed to send replica PSYNC", "master_addr", masterAddr, "error", err)
		return
	}
	if err := s.consumeFullResync(parser); err != nil {
		s.logger.Error("failed to consume FULLRESYNC from master", "master_addr", masterAddr, "error", err)
		return
	}

	// The stream that follows a full resynchronisation counts from zero.
	s.replication.ResetReplicaOffset()
	s.logger.Info("replica handshake completed", "master_addr", masterAddr, "listening_port", listeningPort)
	synchronised = true

	replicationCtx := WithReplicationOrigin(ctx)

	for {
		value, err := parser.Parse()
		if err != nil {
			// A Master that goes away inside a frame is still a lost link: the Parser
			// reports a bulk payload cut short as io.ErrUnexpectedEOF, not io.EOF.
			if ctx.Err() != nil || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed) {
				return
			}

			s.logger.Warn("replication stream parse failed", "master_addr", masterAddr, "error", err)
			return
		}

		encodedLen, err := protocol.EncodedLen(value)
		if err != nil {
			s.logger.Warn("failed to size replication stream value", "master_addr", masterAddr, "error", err)
			return
		}
		replicaOffset := s.replication.AdvanceReplicaOffset(int64(encodedLen))
		s.logger.Debug(
			"replication stream command received",
			"master_addr", masterAddr,
			"command", replicationCommandName(value),
			"payload_size", encodedLen,
			"replica_offset", replicaOffset,
		)

		result, execErr := s.executeRequest(replicationCtx, value)
		if execErr != nil {
			if errors.Is(execErr, context.Canceled) || errors.Is(execErr, context.DeadlineExceeded) {
				return
			}

			s.logger.Warn("replication stream command failed", "master_addr", masterAddr, "error", execErr)
			return
		}
		persistErr := s.persistDurabilityFrames(result.Durability)
		if result.Release != nil {
			result.Release()
		}
		if persistErr != nil {
			s.logger.Error("failed to append replicated command to AOF", "master_addr", masterAddr, "error", persistErr)
			return
		}
		if len(result.UpstreamReplies) > 0 {
			if err := writeReplicaResponses(writer, result.UpstreamReplies); err != nil {
				s.logger.Warn("failed to write replication upstream reply", "master_addr", masterAddr, "error", err)
				return
			}
		}
	}
}

func (s *Server) consumeFullResync(parser *protocol.Parser) error {
	value, err := parser.Parse()
	if err != nil {
		return fmt.Errorf("read FULLRESYNC line: %w", err)
	}

	line, ok := value.(protocol.SimpleString)
	if !ok {
		return fmt.Errorf("unexpected FULLRESYNC response type %T", value)
	}
	parts := strings.Fields(line.Value)
	if len(parts) != 3 || !strings.EqualFold(parts[0], "FULLRESYNC") || parts[1] == "" || parts[2] != "0" {
		return fmt.Errorf("invalid FULLRESYNC response %q", line.Value)
	}

	snapshotValue, err := parser.Parse()
	if err != nil {
		return fmt.Errorf("read replication snapshot: %w", err)
	}

	snapshot, ok := snapshotValue.(protocol.BulkString)
	if !ok || snapshot.Null {
		return fmt.Errorf("unexpected replication snapshot type %T", snapshotValue)
	}

	snapshotStore := storage.NewStore()
	stats, err := rdb.LoadReader(bytes.NewReader(snapshot.Data), snapshotStore)
	if err != nil {
		return fmt.Errorf("load replication snapshot: %w", err)
	}
	s.store.ReplaceWith(snapshotStore)
	s.logger.Info(
		"applied full resync snapshot",
		"snapshot_size_bytes", len(snapshot.Data),
		"loaded_keys", stats.LoadedKeys,
		"skipped_expired_keys", stats.SkippedExpiredKeys,
	)

	return nil
}

func replicationCommandName(value protocol.Value) string {
	request, err := commandFromValue(value)
	if err != nil {
		return fmt.Sprintf("%T", value)
	}

	return request.Name
}

func commandFromValue(value protocol.Value) (*protocolCommand, error) {
	array, ok := value.(protocol.Array)
	if !ok || array.Null || len(array.Elements) == 0 {
		return nil, fmt.Errorf("unexpected replication value %T", value)
	}

	rawName, err := protocol.Bytes(array.Elements[0])
	if err != nil {
		return nil, err
	}

	return &protocolCommand{Name: strings.ToUpper(string(rawName))}, nil
}

type protocolCommand struct {
	Name string
}

func parseListenerPort(address string) (int, error) {
	_, port, err := net.SplitHostPort(address)
	if err != nil {
		return 0, err
	}

	parsed, err := strconv.Atoi(port)
	if err != nil {
		return 0, err
	}

	return parsed, nil
}

func writeReplicaCommand(writer *bufio.Writer, parts ...string) error {
	if err := protocol.WriteValue(writer, replicaCommand(parts...)); err != nil {
		return err
	}

	return writer.Flush()
}

func writeReplicaResponses(writer *bufio.Writer, values []protocol.Value) error {
	for _, value := range values {
		if err := protocol.WriteValue(writer, value); err != nil {
			return err
		}
	}

	return writer.Flush()
}

func replicaCommand(parts ...string) protocol.Array {
	elements := make([]protocol.Value, 0, len(parts))
	for _, part := range parts {
		elements = append(elements, protocol.BulkString{Data: []byte(part)})
	}

	return protocol.Array{Elements: elements}
}

func expectSimpleString(parser *protocol.Parser, expected string) error {
	value, err := parser.Parse()
	if err != nil {
		return err
	}

	switch typed := value.(type) {
	case protocol.SimpleString:
		if typed.Value != expected {
			return fmt.Errorf("unexpected response %q, want %q", typed.Value, expected)
		}
		return nil
	case protocol.ErrorValue:
		return fmt.Errorf("unexpected error response %q, want %q", typed.Message, expected)
	default:
		return fmt.Errorf("unexpected response type %T", value)
	}
}

func expectReplicaAuthOK(parser *protocol.Parser) error {
	value, err := parser.Parse()
	if err != nil {
		return err
	}

	switch typed := value.(type) {
	case protocol.SimpleString:
		if typed.Value != "OK" {
			return fmt.Errorf("unexpected AUTH response %q, want %q", typed.Value, "OK")
		}
		return nil
	case protocol.ErrorValue:
		switch respErrorCode(typed.Message) {
		case "WRONGPASS":
			return fmt.Errorf("replica AUTH rejected by master: %s", typed.Message)
		case "NOAUTH":
			return fmt.Errorf("master reported NOAUTH during replica AUTH: %s", typed.Message)
		default:
			return fmt.Errorf("replica AUTH failed with error response: %s", typed.Message)
		}
	default:
		return fmt.Errorf("unexpected AUTH response type %T", value)
	}
}

func expectReplicaReplConfOK(parser *protocol.Parser) error {
	value, err := parser.Parse()
	if err != nil {
		return err
	}

	switch typed := value.(type) {
	case protocol.SimpleString:
		if typed.Value != "OK" {
			return fmt.Errorf("unexpected REPLCONF response %q, want %q", typed.Value, "OK")
		}
		return nil
	case protocol.ErrorValue:
		switch respErrorCode(typed.Message) {
		case "NOAUTH":
			return fmt.Errorf("protected master requires replica authentication (--masterauth): %s", typed.Message)
		default:
			return fmt.Errorf("replica REPLCONF failed with error response: %s", typed.Message)
		}
	default:
		return fmt.Errorf("unexpected REPLCONF response type %T", value)
	}
}

func respErrorCode(message string) string {
	code, _, _ := strings.Cut(message, " ")
	return code
}
