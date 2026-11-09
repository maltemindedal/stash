package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"

	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/rdb"
	"github.com/maltemindedal/stash/internal/record"
	"github.com/maltemindedal/stash/internal/replication"
	"github.com/maltemindedal/stash/internal/storage"
)

// ExecuteResult describes the full set of RESP frames emitted by a command.
type ExecuteResult struct {
	Responses []protocol.Value
	// RegisterReplica is set by PSYNC, which registered the client as a replica
	// at its attach cut. The connection then starts the replica's feed
	// (registerReplicaPeer) once Responses are ahead of anything it writes.
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

// replicaMarker is a connection whose limits depend on whether it is a replica:
// the event loop's, which holds a replica's waiting output to the replica limit.
type replicaMarker interface {
	markReplica()
}

// registerReplicaPeer starts the feed of the replica that PSYNC registered at its
// attach cut (replication.Master.Start), once the full resync reply is ahead of
// anything the feed writes on conn. The feed has queued every write since the
// snapshot. When the replica was dropped while its reply was sent (its feed
// refused a frame, past the backlog limit), it closes conn instead.
func (s *Server) registerReplicaPeer(clientID uint64, conn ClientConn) {
	// Before the feed starts, so that the first frame it pushes is held to the
	// replica's output limit, not a subscriber's.
	if marker, ok := conn.(replicaMarker); ok {
		marker.markReplica()
	}
	if !s.master.Start(clientID, conn) {
		s.logger.Info("closing a replica dropped while its full resync was sent", "replica_id", clientID)
		if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			s.logger.Debug("failed to close dropped replica", "replica_id", clientID, "error", err)
		}
		return
	}

	listeningPort := 0
	if state := s.getClientState(clientID); state != nil {
		listeningPort = state.ReplicaListeningPort()
	}
	s.logger.Info(
		"replica registered",
		"replica_id", clientID,
		"remote_addr", conn.RemoteAddr().String(),
		"listening_port", listeningPort,
		"master_offset", s.master.Offset(),
	)
}

// dropReplica removes a replica that can no longer be fed, logs why, and closes
// it. It has to synchronise again from the start. It is the Master's drop
// handler, so it is called with the Master's lock not held, and takes it. A replica dropped while its full resync reply is being sent has no
// connection here yet; its feed never starts, and registerReplicaPeer closes the
// connection once the reply is sent.
//
// Only a replica that fell behind (replicaFellBehind) is logged as dropped. Any
// other cause means its connection ended, because the replica left or the
// connection is being torn down, and it is logged as the connection's teardown
// logs it (teardownClient), which finds it removed already.
func (s *Server) dropReplica(id uint64, cause error) {
	peer := s.master.Detach(id)
	if peer == nil {
		return
	}
	if replicaFellBehind(cause) {
		s.logger.Warn("dropping a replica: it can no longer be fed the propagated stream", "replica_id", id, "error", cause)
	} else {
		s.logger.Info("replica disconnected", "replica_id", id, "listening_port", peer.ListeningPort, "error", cause)
	}
	if peer.Conn == nil {
		return
	}
	if err := peer.Conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		s.logger.Debug("failed to close replica after propagation failure", "replica_id", id, "error", err)
	}
}

// replicaFellBehind reports whether a replica's feed stopped because the replica
// did not keep up: its queue passed its limit (replication.ErrReplicaBacklog, or the event
// loop's push limit), or a write to it, or the wait for its connection's writer,
// took longer than the replica feed allows. Anything else means its connection
// ended.
func replicaFellBehind(cause error) bool {
	return errors.Is(cause, replication.ErrReplicaBacklog) ||
		errors.Is(cause, errPushLimitExceeded) ||
		errors.Is(cause, os.ErrDeadlineExceeded) ||
		errors.Is(cause, errResponseWriterBusy)
}

// startReplicaLink keeps the replica attached to its master (replication.Follow) until ctx
// is cancelled, which shutdown does. Only an unusable configuration ends it
// before that.
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
	s.link.ListeningPort = listeningPort
	s.link.Logger = s.logger.With("master_addr", masterAddr)

	dial := func(ctx context.Context) (net.Conn, error) {
		var dialer net.Dialer
		return dialer.DialContext(ctx, "tcp", masterAddr)
	}
	replication.Follow(ctx, dial, s.link, nil)
}

// replicaStore is the Store as the Replica's Link changes it: a full resync
// replaces the keyspace, and each command of the Master's stream runs through
// the command executor with the Master's Origin.
type replicaStore struct{ server *Server }

// Replace loads the RDB snapshot of a full resync and swaps it in for the
// whole keyspace, and has the append-only file follow: the file holds the
// history of the keyspace the Replica had before, so it is rewritten from the
// new one. Until that rewrite has replaced the file, a restart still replays
// the old history.
//
// The swap runs as a background write, ordered against client requests and
// against the copy an AOF rewrite takes, so a rewrite copies the keyspace from
// before the swap or from after it, never half of each, and RequestRewrite
// runs another after one that copied the old keyspace.
func (r replicaStore) Replace(snapshot []byte) error {
	s := r.server
	snapshotStore := storage.NewStore()
	stats, err := rdb.LoadReader(bytes.NewReader(snapshot), snapshotStore)
	if err != nil {
		return err
	}

	release := func() {}
	if sequencer, ok := s.executor.(backgroundWriteSequencer); ok {
		release = sequencer.BeginBackgroundWrite()
	}
	s.store.ReplaceWith(snapshotStore)
	release()

	s.logger.Info(
		"applied full resync snapshot",
		"snapshot_size_bytes", len(snapshot),
		"loaded_keys", stats.LoadedKeys,
		"skipped_expired_keys", stats.SkippedExpiredKeys,
	)

	if s.aofWriter != nil {
		runtimeCtx := s.runtimeCtx
		if runtimeCtx == nil {
			runtimeCtx = context.Background()
		}
		if err := s.aofWriter.RequestRewrite(runtimeCtx, s.store); err != nil {
			return fmt.Errorf("rewrite the append-only file from the full resync: %w", err)
		}
	}

	return nil
}

func (r replicaStore) Apply(ctx context.Context, command protocol.Value) error {
	_, err := r.server.executor.Handle(ctx, Call{Origin: record.OriginMaster}, command)
	if errors.Is(err, ErrNotRecorded) {
		r.server.logger.Error("failed to append replicated command to AOF", "error", err)
	}
	return err
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
