package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"

	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/rdb"
	"github.com/maltemindedal/stash/internal/record"
	"github.com/maltemindedal/stash/internal/replication"
	"github.com/maltemindedal/stash/internal/server/output"
	"github.com/maltemindedal/stash/internal/storage"
)

// ExecuteResult describes the full set of RESP frames emitted by a command.
type ExecuteResult struct {
	Responses []protocol.Value
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

// A connection's Output is the Master's port to it once PSYNC has made the
// connection a Replica's: a method renamed on either side is a compile error.
var _ replication.Output = (*output.Output)(nil)

// replicaDropped logs a Replica the Master detached because its Output refused
// a frame of the stream. It is the Master's dropped handler, so it is called
// with the Master's lock not held. It closes nothing: the Output refused
// because the connection is closed already, and the connection's teardown,
// which then finds the Replica detached, follows.
//
// Only a Replica that fell behind (replicaFellBehind) is logged as dropped. Any
// other cause means its connection ended, because the Replica left or the
// connection is being torn down, and it is logged as the connection's teardown
// logs one it detaches itself (teardownClient).
func (s *Server) replicaDropped(replica *replication.Replica, cause error) {
	if replicaFellBehind(cause) {
		s.logger.Warn("dropping a replica: it can no longer be fed the propagated stream", "replica_id", replica.ID, "error", cause)
		return
	}
	s.logger.Info("replica disconnected", "replica_id", replica.ID, "listening_port", replica.ListeningPort, "error", cause)
}

// replicaFellBehind reports whether a Replica's Output was closed because the
// Replica did not keep up: more of the stream waited for it than the Replica
// Push limit allows, or it accepted nothing for the stall timeout. Anything
// else means its connection ended.
func replicaFellBehind(cause error) bool {
	return errors.Is(cause, output.ErrPushLimitExceeded) || errors.Is(cause, output.ErrReplicaStalled)
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
// The swap runs as a Background unit, ordered against client requests and
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

	// The swap is a Background unit: no write is halfway to the log while the
	// keyspace changes under it, and the DEL of the keys the new keyspace
	// held past their TTL is recorded. The Recorder logs an append that fails.
	_ = s.executor.RunBackgroundUnit(func(changes storage.Changes) {
		s.store.ReplaceWith(changes, snapshotStore)
	})
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
