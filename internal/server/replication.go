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
	AckOffset     int64

	writer encodedReplicaWriter
}

type encodedReplicaWriter interface {
	WriteEncoded([]byte) error
}

type propagationReport struct {
	attempted   int
	succeeded   int
	failed      int
	payloadSize int
	endOffset   int64
}

const (
	replicaFanoutAsyncThreshold = 8
	replicaFanoutMaxWorkers     = 32
)

// WriteEncoded writes a pre-encoded RESP payload to the replica socket.
func (p *ReplicaPeer) WriteEncoded(payload []byte) error {
	if p == nil || p.writer == nil {
		return fmt.Errorf("replica response writer unavailable")
	}

	return p.writer.WriteEncoded(payload)
}

// ReplicaRegistry tracks replica peers connected to a master server.
type ReplicaRegistry struct {
	mu       sync.RWMutex
	replicas map[uint64]*ReplicaPeer
	changed  chan struct{}
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
	r.mu.Lock()
	defer r.mu.Unlock()

	r.replicas[id] = &ReplicaPeer{ID: id, Conn: conn, ListeningPort: listeningPort, writer: writer}
	r.notifyChangedLocked()
}

// UpdateAck records the latest processed replication offset for a replica peer.
func (r *ReplicaRegistry) UpdateAck(id uint64, offset int64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	peer, ok := r.replicas[id]
	if !ok {
		return false
	}
	if offset > peer.AckOffset {
		peer.AckOffset = offset
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
		if peer.AckOffset >= targetOffset {
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

	peers := s.replicaPeers.Snapshot()
	report := propagationReport{attempted: len(peers), payloadSize: len(payload)}
	report.endOffset = s.replication.AdvanceMasterOffset(int64(len(payload)))

	report.succeeded, report.failed = s.writePropagationPayload(payload, peers)
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

func (s *Server) writePropagationPayload(payload []byte, peers []*ReplicaPeer) (int, int) {
	if len(peers) == 0 {
		return 0, 0
	}
	if len(peers) < replicaFanoutAsyncThreshold {
		return s.writePropagationPayloadSequential(payload, peers)
	}

	workerCount := min(len(peers), replicaFanoutMaxWorkers)
	jobs := make(chan *ReplicaPeer, len(peers))
	var succeeded atomic.Int64
	var failed atomic.Int64
	var wg sync.WaitGroup
	wg.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go func() {
			defer wg.Done()
			for peer := range jobs {
				if s.writePropagationPayloadToReplica(peer, payload) {
					succeeded.Add(1)
				} else {
					failed.Add(1)
				}
			}
		}()
	}

	for _, peer := range peers {
		jobs <- peer
	}
	close(jobs)
	wg.Wait()

	return int(succeeded.Load()), int(failed.Load())
}

func (s *Server) writePropagationPayloadSequential(payload []byte, peers []*ReplicaPeer) (int, int) {
	succeeded := 0
	failed := 0
	for _, peer := range peers {
		if s.writePropagationPayloadToReplica(peer, payload) {
			succeeded++
		} else {
			failed++
		}
	}

	return succeeded, failed
}

func (s *Server) writePropagationPayloadToReplica(peer *ReplicaPeer, payload []byte) bool {
	if peer == nil {
		return false
	}
	if err := peer.WriteEncoded(payload); err != nil {
		s.logger.Warn("failed to propagate command to replica", "replica_id", peer.ID, "error", err)
		if closeErr := s.replicaPeers.RemoveAndClose(peer.ID); closeErr != nil {
			s.logger.Debug("failed to close replica after propagation failure", "replica_id", peer.ID, "error", closeErr)
		}
		return false
	}

	return true
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

	s.logger.Info("replica handshake completed", "master_addr", masterAddr, "listening_port", listeningPort)

	replicationCtx := WithReplicationOrigin(ctx)

	for {
		value, err := parser.Parse()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
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

		result, execErr := s.executor.ExecuteDetailed(replicationCtx, value)
		if execErr != nil {
			if errors.Is(execErr, context.Canceled) || errors.Is(execErr, context.DeadlineExceeded) {
				return
			}

			s.logger.Warn("replication stream command failed", "master_addr", masterAddr, "error", execErr)
			return
		}
		if err := s.persistDurabilityFrames(result.Durability); err != nil {
			s.logger.Error("failed to append replicated command to AOF", "master_addr", masterAddr, "error", err)
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
