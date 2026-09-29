package command

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/rdb"
	"github.com/maltemindedal/stash/internal/server"
	"github.com/maltemindedal/stash/internal/storage"
)

const pubSubWriteTimeout = 100 * time.Millisecond

// maxWaitMillis is the longest WAIT timeout a time.Duration can represent, about
// 292 years. A longer request is clamped to it: converting it as given overflowed
// to a negative duration, so a WAIT asked to wait a very long time returned at once.
const maxWaitMillis = math.MaxInt64 / int64(time.Millisecond)

var cachedReplConfGetAckPayload = sync.OnceValues(func() ([]byte, error) {
	return protocol.Encode(propagationFrame(&Request{
		Name: "REPLCONF",
		Args: [][]byte{[]byte("GETACK"), []byte("*")},
	}))
})

var pubSubSubscriberSlicePool = sync.Pool{
	New: func() any {
		subscribers := make([]*server.ClientState, 0, 32)
		return &subscribers
	},
}

func getPooledPubSubSubscribers() *[]*server.ClientState {
	return pubSubSubscriberSlicePool.Get().(*[]*server.ClientState)
}

func putPooledPubSubSubscribers(subscribers *[]*server.ClientState) {
	if cap(*subscribers) > 1024 {
		return
	}

	*subscribers = (*subscribers)[:0]
	pubSubSubscriberSlicePool.Put(subscribers)
}

func (e *Executor) handleWatch(ctx context.Context, request *Request) (protocol.Value, error) {
	state, err := clientStateFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if state.InTransactionActive() {
		return nil, ErrWatchInsideMultiError()
	}

	keys := make([]string, 0, len(request.Args))
	for _, arg := range request.Args {
		keys = append(keys, string(arg))
	}
	state.WatchKeys(keys...)

	return protocol.SimpleString{Value: "OK"}, nil
}

func (e *Executor) handleSubscribe(ctx context.Context, request *Request) (server.ExecuteResult, error) {
	state, err := clientStateFromContext(ctx)
	if err != nil {
		return server.ExecuteResult{}, err
	}
	if state.InTransactionActive() {
		return server.ExecuteResult{}, ErrSubscribeInsideMultiError()
	}

	responses := make([]protocol.Value, 0, len(request.Args))
	for _, arg := range request.Args {
		channel := string(arg)
		state.SubscribeChannel(channel)
		responses = append(responses, pubSubAckResponse("subscribe", protocol.BulkString{Data: clone(arg)}, state.SubscriptionCount()))
	}

	return server.MultiResponse(responses...), nil
}

func (e *Executor) handleUnsubscribe(ctx context.Context, request *Request) (server.ExecuteResult, error) {
	state, err := clientStateFromContext(ctx)
	if err != nil {
		return server.ExecuteResult{}, err
	}
	if state.InTransactionActive() {
		return server.ExecuteResult{}, ErrSubscribeInsideMultiError()
	}

	channels := make([]string, 0, len(request.Args))
	if len(request.Args) == 0 {
		channels = state.SubscribedChannels()
		if len(channels) == 0 {
			return server.MultiResponse(pubSubAckResponse("unsubscribe", protocol.BulkString{Null: true}, 0)), nil
		}
	} else {
		for _, arg := range request.Args {
			channels = append(channels, string(arg))
		}
	}

	responses := make([]protocol.Value, 0, len(channels))
	for _, channel := range channels {
		state.UnsubscribeChannel(channel)
		responses = append(responses, pubSubAckResponse("unsubscribe", protocol.BulkString{Data: []byte(channel)}, state.SubscriptionCount()))
	}

	return server.MultiResponse(responses...), nil
}

func (e *Executor) handleMulti(ctx context.Context, request *Request) (protocol.Value, error) {
	if len(request.Args) != 0 {
		return nil, wrongNumberOfArgumentsError("MULTI")
	}

	state, err := clientStateFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if state.IsSubscribed() {
		return nil, ErrSubscribedModeOnlyError()
	}
	if !state.BeginTransaction() {
		return nil, ErrMultiNestedError()
	}

	return protocol.SimpleString{Value: "OK"}, nil
}

func (e *Executor) handleExec(ctx context.Context, request *Request) (server.ExecuteResult, error) {
	if len(request.Args) != 0 {
		return server.ExecuteResult{}, wrongNumberOfArgumentsError("EXEC")
	}

	state, err := clientStateFromContext(ctx)
	if err != nil {
		return server.ExecuteResult{}, err
	}
	if !state.InTransactionActive() {
		return server.ExecuteResult{}, ErrExecWithoutMultiError()
	}
	if state.TransactionDirty() {
		state.ResetTransaction()
		state.UnwatchAll()
		return server.ExecuteResult{}, ErrExecAbortError()
	}
	if state.TransactionFailed() {
		state.ResetTransaction()
		state.UnwatchAll()
		return server.SingleResponse(protocol.Array{Null: true}), nil
	}
	state.UnwatchAll()

	// The queued commands run with the sequencer held exclusively; none of them
	// may wait for another request (see withTransactionExecution).
	ctx = withTransactionExecution(ctx)
	queued := state.DrainTransaction()
	responses := make([]protocol.Value, 0, len(queued))
	propagation := make([]protocol.Value, 0, len(queued))
	durability := make([]protocol.Value, 0, len(queued))
	for _, queuedCommand := range queued {
		result, execErr := e.executeRequestDetailed(ctx, &Request{Name: queuedCommand.Name, Args: queuedCommand.Args}, false)
		if execErr != nil {
			if errors.Is(execErr, context.Canceled) || errors.Is(execErr, context.DeadlineExceeded) {
				return server.ExecuteResult{}, execErr
			}
			responses = append(responses, responseErrorValue(execErr))
			continue
		}
		if len(result.Responses) != 1 {
			responses = append(responses, protocol.ErrorValue{Message: "ERR queued command returned an invalid response"})
			continue
		}
		responses = append(responses, result.Responses[0])
		propagation = append(propagation, result.Propagation...)
		durability = append(durability, result.Durability...)
	}

	result := server.SingleResponse(protocol.Array{Elements: responses})
	result.Propagation = propagation
	result.Durability = durability
	return result, nil
}

func (e *Executor) handleBGRewriteAOF(ctx context.Context, request *Request) (protocol.Value, error) {
	if len(request.Args) != 0 {
		return nil, wrongNumberOfArgumentsError("BGREWRITEAOF")
	}
	if e.aofRewrite == nil {
		return nil, fmt.Errorf("append only file persistence is not enabled")
	}
	if err := e.aofRewrite(ctx); err != nil {
		return nil, err
	}

	return protocol.SimpleString{Value: "Background append only file rewriting started"}, nil
}

func (e *Executor) handleReplConf(ctx context.Context, request *Request) (server.ExecuteResult, error) {
	// validateReplConfRequest has already checked the arity, rejected unknown
	// subcommands, and confirmed each subcommand's argument parses. What remains
	// here is the context-dependent work it cannot do: applying the parsed value
	// and checking who is asking.
	subcommand := strings.ToUpper(string(request.Args[0]))

	switch subcommand {
	case "LISTENING-PORT":
		port, err := parseReplicationPortArgument(request.Args[1])
		if err != nil {
			return server.ExecuteResult{}, ErrSyntaxError()
		}

		if state, ok := server.ClientStateFromContext(ctx); ok && state != nil {
			state.SetReplicaListeningPort(port)
		}

		return server.SingleResponse(protocol.SimpleString{Value: "OK"}), nil
	case "GETACK":
		// The "*" argument is validated upstream; only the caller's origin,
		// which the validator has no context for, is checked here.
		if !server.IsReplicationOrigin(ctx) {
			return server.ExecuteResult{}, ErrSyntaxError()
		}

		ackOffset := int64(0)
		if e.replication != nil {
			ackOffset = e.replication.ReplicaOffset()
		}

		result := server.ExecuteResult{}
		result.UpstreamReplies = []protocol.Value{propagationFrame(&Request{
			Name: "REPLCONF",
			Args: [][]byte{[]byte("ACK"), []byte(strconv.FormatInt(ackOffset, 10))},
		})}
		return result, nil
	case "ACK":
		ackOffset, err := parseIntegerArgument(request.Args[1])
		if err != nil || ackOffset < 0 {
			return server.ExecuteResult{}, ErrSyntaxError()
		}

		if state, ok := server.ClientStateFromContext(ctx); ok && state != nil && state.IsReplica() && e.replicaPeers != nil {
			if updated := e.replicaPeers.UpdateAck(state.ID, ackOffset); updated {
				e.logger.Debug("replica acknowledged offset", "replica_id", state.ID, "ack_offset", ackOffset)
			}
		}

		return server.ExecuteResult{}, nil
	default:
		return server.ExecuteResult{}, ErrSyntaxError()
	}
}

func (e *Executor) handlePSync(ctx context.Context, request *Request) (server.ExecuteResult, error) {
	if len(request.Args) != 2 {
		return server.ExecuteResult{}, wrongNumberOfArgumentsError("PSYNC")
	}
	if string(request.Args[0]) != "?" || string(request.Args[1]) != "-1" {
		return server.ExecuteResult{}, ErrSyntaxError()
	}
	if e.replication == nil || e.replication.MasterReplicationID == "" {
		return server.ExecuteResult{}, fmt.Errorf("replication state unavailable")
	}

	entries, snapshotStats := e.store.SnapshotStrings()
	payload, writeStats := rdb.BuildSnapshot(entries)
	if snapshotStats.SkippedUnsupportedKeys > 0 {
		// RDB support is scoped to DB 0 string keys, so a master holding
		// collections transfers only its string keyspace on full resync.
		e.logger.Warn(
			"full resync snapshot skipped unsupported value types",
			"skipped_unsupported_keys", snapshotStats.SkippedUnsupportedKeys,
			"exported_keys", snapshotStats.ExportedKeys,
		)
	}

	if state, ok := server.ClientStateFromContext(ctx); ok && state != nil {
		state.PromoteToReplica()
		e.logger.Info(
			"serving full resync to replica",
			"replica_id", state.ID,
			"master_replid", e.replication.MasterReplicationID,
			"snapshot_size_bytes", len(payload),
			"snapshot_keys", writeStats.WrittenKeys,
		)
	}

	result := server.MultiResponse(
		protocol.SimpleString{Value: fmt.Sprintf("FULLRESYNC %s 0", e.replication.MasterReplicationID)},
		protocol.BulkString{Data: payload},
	)
	result.RegisterReplica = true
	return result, nil
}

func (e *Executor) handleWait(ctx context.Context, request *Request) (server.ExecuteResult, error) {
	replicas, timeoutMillis, err := parseWaitArguments(request)
	if err != nil {
		return server.ExecuteResult{}, err
	}

	targetOffset := e.waitTargetOffset(ctx)
	startedAt := time.Now()

	ackedReplicas := e.countReplicasAtOrAbove(targetOffset)
	e.logger.Debug(
		"WAIT requested",
		"target_replicas", replicas,
		"timeout_millis", timeoutMillis,
		"target_offset", targetOffset,
		"currently_acked", ackedReplicas,
	)
	// Inside a transaction the sequencer is held exclusively, which would keep the
	// replicas' acknowledgements from being processed, so it reports what is
	// acknowledged now instead of waiting, as in Redis.
	if int64(ackedReplicas) >= replicas || timeoutMillis == 0 || inTransactionExecution(ctx) {
		return e.finishWaitResult(replicas, ackedReplicas, targetOffset, startedAt, false), nil
	}

	// On the event loop, waiting would deadlock: the GETACK frames queued
	// below are flushed by the loop goroutine this command is blocking, and
	// incoming REPLCONF ACK frames are read by it too.
	if server.IsInlineExecution(ctx) {
		return server.ExecuteResult{}, blockingNotSupportedError("WAIT")
	}

	// Replies to the requests pipelined ahead of this one must not wait behind it.
	if err := server.FlushClientResponses(ctx); err != nil {
		return server.ExecuteResult{}, err
	}

	if err := e.requestReplicaAcknowledgements(); err != nil {
		return server.ExecuteResult{}, err
	}

	return e.waitForReplicaAcknowledgements(ctx, replicas, timeoutMillis, targetOffset, startedAt)
}

func parseWaitArguments(request *Request) (int64, int64, error) {
	if len(request.Args) != 2 {
		return 0, 0, wrongNumberOfArgumentsError("WAIT")
	}

	replicas, err := parseIntegerArgument(request.Args[0])
	if err != nil || replicas < 0 {
		return 0, 0, ErrValueNotIntegerError()
	}
	timeoutMillis, err := parseIntegerArgument(request.Args[1])
	if err != nil || timeoutMillis < 0 {
		return 0, 0, ErrValueNotIntegerError()
	}

	return replicas, timeoutMillis, nil
}

func (e *Executor) waitTargetOffset(ctx context.Context) int64 {
	if state, ok := server.ClientStateFromContext(ctx); ok && state != nil {
		return state.LastWriteReplicationOffset()
	}
	if e.replication != nil {
		return e.replication.MasterOffset()
	}

	return 0
}

func (e *Executor) waitForReplicaAcknowledgements(ctx context.Context, replicas int64, timeoutMillis int64, targetOffset int64, startedAt time.Time) (server.ExecuteResult, error) {
	timer := time.NewTimer(time.Duration(min(timeoutMillis, maxWaitMillis)) * time.Millisecond)
	defer timer.Stop()

	for {
		ackedReplicas, notified := e.countReplicasAtOrAboveWithNotify(targetOffset)
		if int64(ackedReplicas) >= replicas {
			return e.finishWaitResult(replicas, ackedReplicas, targetOffset, startedAt, false), nil
		}

		select {
		case <-ctx.Done():
			return server.ExecuteResult{}, ctx.Err()
		case <-timer.C:
			finalAcked := e.countReplicasAtOrAbove(targetOffset)
			return e.finishWaitResult(replicas, finalAcked, targetOffset, startedAt, true), nil
		case <-notified:
		}
	}
}

func (e *Executor) finishWaitResult(replicas int64, ackedReplicas int, targetOffset int64, startedAt time.Time, timedOut bool) server.ExecuteResult {
	e.logger.Info(
		"WAIT completed",
		"target_replicas", replicas,
		"acked_replicas", ackedReplicas,
		"target_offset", targetOffset,
		"timed_out", timedOut,
		"elapsed", time.Since(startedAt),
	)

	return server.SingleResponse(protocol.Integer{Value: int64(ackedReplicas)})
}

func (e *Executor) handleDiscard(ctx context.Context, request *Request) (protocol.Value, error) {
	if len(request.Args) != 0 {
		return nil, wrongNumberOfArgumentsError("DISCARD")
	}

	state, err := clientStateFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if !state.InTransactionActive() {
		return nil, ErrDiscardWithoutMultiError()
	}

	state.ResetTransaction()
	state.UnwatchAll()
	return protocol.SimpleString{Value: "OK"}, nil
}

func (e *Executor) handleAuth(ctx context.Context, request *Request) (protocol.Value, error) {
	if len(request.Args) != 1 {
		return nil, wrongNumberOfArgumentsError("AUTH")
	}
	if e.requirePass == "" {
		return nil, ErrAuthNotConfiguredError()
	}

	state, err := clientStateFromContext(ctx)
	if err != nil {
		return nil, err
	}

	if !secretsEqual(e.requirePass, request.Args[0]) {
		return nil, ErrWrongPassError()
	}

	state.SetAuthenticated(true)
	return protocol.SimpleString{Value: "OK"}, nil
}

func secretsEqual(expected string, actual []byte) bool {
	expectedDigest := sha256.Sum256([]byte(expected))
	actualDigest := sha256.Sum256(actual)

	return subtle.ConstantTimeCompare(expectedDigest[:], actualDigest[:]) == 1
}

func (e *Executor) handlePing(_ context.Context, request *Request) (protocol.Value, error) {
	if len(request.Args) == 1 {
		return protocol.BulkString{Data: clone(request.Args[0])}, nil
	}
	if len(request.Args) > 1 {
		return nil, wrongNumberOfArgumentsError("PING")
	}

	return protocol.SimpleString{Value: "PONG"}, nil
}

func (e *Executor) handleEcho(_ context.Context, request *Request) (protocol.Value, error) {
	if len(request.Args) != 1 {
		return nil, wrongNumberOfArgumentsError("ECHO")
	}

	return protocol.BulkString{Data: clone(request.Args[0])}, nil
}

func (e *Executor) handlePublish(_ context.Context, request *Request) (protocol.Value, error) {
	if e.pubSubRegistry == nil {
		return protocol.Integer{Value: 0}, nil
	}

	channel := string(request.Args[0])
	pooledSubscribers := getPooledPubSubSubscribers()
	subscribers := e.pubSubRegistry.AppendSubscribers(channel, (*pooledSubscribers)[:0])
	defer func() {
		*pooledSubscribers = subscribers
		putPooledPubSubSubscribers(pooledSubscribers)
	}()
	if len(subscribers) == 0 {
		return protocol.Integer{Value: 0}, nil
	}

	message := pubSubMessageResponse(request.Args[0], request.Args[1])
	encodedMessage, err := protocol.Encode(message)
	if err != nil {
		return nil, fmt.Errorf("encode pub/sub message: %w", err)
	}

	matched := e.deliverPubSubMessage(channel, encodedMessage, subscribers)

	return protocol.Integer{Value: matched}, nil
}

func (e *Executor) handleSet(ctx context.Context, request *Request) (protocol.Value, error) {
	expiresAt, err := storage.ParseExpiryMillis(request.Args[2:])
	if err != nil {
		switch {
		case errors.Is(err, storage.ErrSyntax):
			return nil, ErrSyntaxError()
		case errors.Is(err, storage.ErrInvalidExpireTime):
			return nil, ErrInvalidExpireTimeError()
		default:
			return nil, err
		}
	}

	key := string(request.Args[0])
	evicted, err := e.store.Set(key, request.Args[1], expiresAt)
	if err != nil {
		if errors.Is(err, storage.ErrMemoryLimitExceeded) {
			return nil, ErrOutOfMemoryError()
		}
		return nil, err
	}
	if effects := executionEffectsFromContext(ctx); effects != nil {
		effects.setExpiryMillis = expiresAt
	}
	e.touchWatchKeys(key)
	e.recordEvictedKeys(ctx, evicted)
	return protocol.SimpleString{Value: "OK"}, nil
}

func (e *Executor) handleGet(_ context.Context, request *Request) (protocol.Value, error) {
	if len(request.Args) != 1 {
		return nil, wrongNumberOfArgumentsError("GET")
	}

	key := string(request.Args[0])
	value, ok, err := e.store.Get(key)
	if err != nil {
		if errors.Is(err, storage.ErrWrongType) {
			return nil, ErrWrongTypeError()
		}
		return nil, err
	}
	if !ok {
		return protocol.BulkString{Null: true}, nil
	}

	return protocol.BulkString{Data: value}, nil
}

func (e *Executor) handleSetBit(ctx context.Context, request *Request) (protocol.Value, error) {
	offset, err := parseBitmapOffsetArgument(request.Args[1])
	if err != nil {
		return nil, err
	}
	bit, err := parseBitValueArgument(request.Args[2])
	if err != nil {
		return nil, err
	}

	key := string(request.Args[0])
	previous, evicted, err := e.store.SetBit(key, offset, bit)
	if err != nil {
		return nil, storageCommandError(err)
	}

	e.recordWriteEffects(ctx, key, evicted)
	return protocol.Integer{Value: previous}, nil
}

func (e *Executor) handleGetBit(_ context.Context, request *Request) (protocol.Value, error) {
	offset, err := parseBitmapOffsetArgument(request.Args[1])
	if err != nil {
		return nil, err
	}

	bit, _, err := e.store.GetBit(string(request.Args[0]), offset)
	if err != nil {
		return nil, storageCommandError(err)
	}

	return protocol.Integer{Value: bit}, nil
}

func (e *Executor) handleBitCount(_ context.Context, request *Request) (protocol.Value, error) {
	var start *int64
	var end *int64
	if len(request.Args) == 3 {
		parsedStart, err := parseIntegerArgument(request.Args[1])
		if err != nil {
			return nil, err
		}
		parsedEnd, err := parseIntegerArgument(request.Args[2])
		if err != nil {
			return nil, err
		}
		start = &parsedStart
		end = &parsedEnd
	}

	count, _, err := e.store.BitCount(string(request.Args[0]), start, end)
	if err != nil {
		return nil, storageCommandError(err)
	}

	return protocol.Integer{Value: count}, nil
}

func (e *Executor) handlePFAdd(ctx context.Context, request *Request) (protocol.Value, error) {
	if len(request.Args) < 1 {
		return nil, wrongNumberOfArgumentsError("PFADD")
	}

	key := string(request.Args[0])
	changed, evicted, err := e.store.PFAdd(key, request.Args[1:])
	if err != nil {
		return nil, storageCommandError(err)
	}

	e.recordWriteEffects(ctx, key, evicted)
	return protocol.Integer{Value: changed}, nil
}

func (e *Executor) handlePFCount(_ context.Context, request *Request) (protocol.Value, error) {
	if len(request.Args) < 1 {
		return nil, wrongNumberOfArgumentsError("PFCOUNT")
	}

	keys := make([]string, 0, len(request.Args))
	for _, arg := range request.Args {
		keys = append(keys, string(arg))
	}

	count, err := e.store.PFCount(keys)
	if err != nil {
		return nil, storageCommandError(err)
	}

	return protocol.Integer{Value: count}, nil
}

func (e *Executor) handleDel(_ context.Context, request *Request) (protocol.Value, error) {
	if len(request.Args) < 1 {
		return nil, wrongNumberOfArgumentsError("DEL")
	}

	removed := int64(0)
	touched := make([]string, 0, len(request.Args))
	if len(request.Args) == 1 {
		key := string(request.Args[0])
		if e.store.Delete(key) {
			removed = 1
			touched = append(touched, key)
		}
	} else {
		keys := make([]string, 0, len(request.Args))
		for _, arg := range request.Args {
			key := string(arg)
			keys = append(keys, key)
		}

		touched = e.store.DeleteMany(keys)
		removed = int64(len(touched))
	}
	if len(touched) > 0 {
		e.touchWatchKeys(touched...)
	}

	return protocol.Integer{Value: removed}, nil
}

func (e *Executor) handleIncr(ctx context.Context, request *Request) (protocol.Value, error) {
	if len(request.Args) != 1 {
		return nil, wrongNumberOfArgumentsError("INCR")
	}

	key := string(request.Args[0])
	value, evicted, err := e.store.Increment(key)
	if err != nil {
		return nil, storageCommandError(err)
	}

	e.recordWriteEffects(ctx, key, evicted)
	return protocol.Integer{Value: value}, nil
}

func (e *Executor) handleZAdd(ctx context.Context, request *Request) (protocol.Value, error) {
	entries := make([]storage.ZSetEntry, 0, (len(request.Args)-1)/2)
	for i := 1; i < len(request.Args); i += 2 {
		score, err := parseFloatArgument(request.Args[i])
		if err != nil {
			return nil, err
		}

		entries = append(entries, storage.ZSetEntry{
			Member: request.Args[i+1],
			Score:  score,
		})
	}

	key := string(request.Args[0])
	added, evicted, err := e.store.ZAdd(key, entries)
	if err != nil {
		return nil, storageCommandError(err)
	}

	e.recordWriteEffects(ctx, key, evicted)
	return protocol.Integer{Value: added}, nil
}

func (e *Executor) handleZRange(_ context.Context, request *Request) (protocol.Value, error) {
	withScores := false
	if len(request.Args) == 4 {
		if !strings.EqualFold(string(request.Args[3]), "WITHSCORES") {
			return nil, ErrSyntaxError()
		}
		withScores = true
	}

	start, stop, err := parseRangeBounds(request.Args[1:3])
	if err != nil {
		return nil, err
	}

	key := string(request.Args[0])
	entries, err := e.store.ZRange(key, start, stop)
	if err != nil {
		if errors.Is(err, storage.ErrWrongType) {
			return nil, ErrWrongTypeError()
		}
		return nil, err
	}

	return zsetResponse(entries, withScores), nil
}

func (e *Executor) handleXAdd(ctx context.Context, request *Request) (protocol.Value, error) {
	key := string(request.Args[0])
	id, evicted, err := e.store.XAdd(key, string(request.Args[1]), request.Args[2:])
	if err != nil {
		return nil, storageCommandError(err)
	}

	if effects := executionEffectsFromContext(ctx); effects != nil {
		effects.streamID = id
	}
	e.recordWriteEffects(ctx, key, evicted)
	return protocol.TextBulkString{Value: id}, nil
}

func (e *Executor) handleXRead(_ context.Context, request *Request) (protocol.Value, error) {
	if len(request.Args) == 0 {
		return nil, wrongNumberOfArgumentsError("XREAD")
	}
	if !strings.EqualFold(string(request.Args[0]), "STREAMS") {
		return nil, ErrSyntaxError()
	}
	if len(request.Args) != 3 {
		return nil, ErrSyntaxError()
	}

	key := string(request.Args[1])
	entries, err := e.store.XRead(key, string(request.Args[2]))
	if err != nil {
		switch {
		case errors.Is(err, storage.ErrWrongType):
			return nil, ErrWrongTypeError()
		case errors.Is(err, storage.ErrInvalidStreamID):
			return nil, ErrInvalidStreamIDError()
		default:
			return nil, err
		}
	}

	return streamReadResponse(request.Args[1], entries), nil
}

func listResponse(values [][]byte) protocol.Array {
	elements := make([]protocol.Value, 0, len(values))
	for _, value := range values {
		elements = append(elements, protocol.BulkString{Data: value})
	}

	return protocol.Array{Elements: elements}
}

func zsetResponse(entries []storage.ZSetRangeEntry, withScores bool) protocol.Array {
	elements := make([]protocol.Value, 0, len(entries))
	if withScores {
		elements = make([]protocol.Value, 0, len(entries)*2)
	}

	for _, entry := range entries {
		elements = append(elements, protocol.TextBulkString{Value: entry.Member})
		if withScores {
			elements = append(elements, protocol.TextBulkString{Value: formatFloatScore(entry.Score)})
		}
	}

	return protocol.Array{Elements: elements}
}

func streamReadResponse(key []byte, entries []storage.StreamEntry) protocol.Array {
	if len(entries) == 0 {
		return protocol.Array{Elements: []protocol.Value{}}
	}

	streamEntries := make([]protocol.Value, 0, len(entries))
	for _, entry := range entries {
		streamEntries = append(streamEntries, protocol.Array{Elements: []protocol.Value{
			protocol.TextBulkString{Value: entry.ID},
			listResponse(entry.Values),
		}})
	}

	return protocol.Array{Elements: []protocol.Value{
		protocol.Array{Elements: []protocol.Value{
			protocol.BulkString{Data: key},
			protocol.Array{Elements: streamEntries},
		}},
	}}
}

func parseRangeBounds(args [][]byte) (int64, int64, error) {
	start, err := parseIntegerArgument(args[0])
	if err != nil {
		return 0, 0, err
	}
	stop, err := parseIntegerArgument(args[1])
	if err != nil {
		return 0, 0, err
	}

	return start, stop, nil
}

func storageCommandError(err error) error {
	switch {
	case errors.Is(err, storage.ErrWrongType):
		return ErrWrongTypeError()
	case errors.Is(err, storage.ErrNotHyperLogLog):
		return ErrNotHyperLogLogError()
	case errors.Is(err, storage.ErrValueNotInteger):
		return ErrValueNotIntegerError()
	case errors.Is(err, storage.ErrSyntax):
		return ErrSyntaxError()
	case errors.Is(err, storage.ErrInvalidStreamID):
		return ErrInvalidStreamIDError()
	case errors.Is(err, storage.ErrStreamIDTooSmall):
		return ErrStreamIDTooSmallError()
	case errors.Is(err, storage.ErrMemoryLimitExceeded):
		return ErrOutOfMemoryError()
	default:
		return err
	}
}

func (e *Executor) recordWriteEffects(ctx context.Context, key string, evicted []string) {
	e.touchWatchKeys(key)
	e.recordEvictedKeys(ctx, evicted)
}

func pubSubAckResponse(kind string, channel protocol.Value, count int) protocol.Array {
	return protocol.Array{Elements: []protocol.Value{
		protocol.TextBulkString{Value: kind},
		channel,
		protocol.Integer{Value: int64(count)},
	}}
}

func pubSubMessageResponse(channel []byte, payload []byte) protocol.Array {
	return protocol.Array{Elements: []protocol.Value{
		protocol.TextBulkString{Value: "message"},
		protocol.BulkString{Data: channel},
		protocol.BulkString{Data: payload},
	}}
}

func (e *Executor) deliverPubSubMessage(channel string, payload []byte, subscribers []*server.ClientState) int64 {
	matched := int64(0)
	for _, subscriber := range subscribers {
		if e.deliverPubSubMessageToSubscriber(channel, payload, subscriber) {
			matched++
		}
	}

	return matched
}

func (e *Executor) deliverPubSubMessageToSubscriber(channel string, payload []byte, subscriber *server.ClientState) bool {
	if subscriber == nil {
		return false
	}
	if err := subscriber.WriteEncodedWithDeadline(payload, pubSubWriteTimeout); err != nil {
		e.logger.Warn(
			"failed to deliver pub/sub message",
			"channel", channel,
			"subscriber_id", subscriber.ID,
			"error", err,
		)
		subscriber.Disconnect()
		return false
	}

	return true
}

func parseIntegerArgument(raw []byte) (int64, error) {
	value, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil {
		return 0, ErrValueNotIntegerError()
	}

	return value, nil
}

// parseIntArgument parses an argument whose destination is an int rather than
// an int64. Passing bitSize 0 makes ParseInt reject anything that would not fit
// in an int on this platform, so the narrowing conversion cannot truncate.
func parseIntArgument(raw []byte) (int, error) {
	value, err := strconv.ParseInt(string(raw), 10, 0)
	if err != nil {
		return 0, ErrValueNotIntegerError()
	}

	return int(value), nil
}

func parseBitmapOffsetArgument(raw []byte) (int64, error) {
	offset, err := parseIntegerArgument(raw)
	if err != nil {
		return 0, err
	}
	if offset < 0 || offset > storage.MaxBitmapOffset {
		return 0, ErrValueNotIntegerError()
	}

	return offset, nil
}

func parseBitValueArgument(raw []byte) (int64, error) {
	bit, err := parseIntegerArgument(raw)
	if err != nil {
		return 0, err
	}
	if bit != 0 && bit != 1 {
		return 0, ErrValueNotIntegerError()
	}

	return bit, nil
}

func parseFloatArgument(raw []byte) (float64, error) {
	value, err := strconv.ParseFloat(string(raw), 64)
	if err != nil || math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, ErrValueNotFloatError()
	}

	return value, nil
}

func formatFloatScore(score float64) string {
	return strconv.FormatFloat(score, 'g', -1, 64)
}

func parseReplicationPortArgument(raw []byte) (int, error) {
	port, err := strconv.Atoi(string(raw))
	if err != nil {
		return 0, err
	}
	if port < 1 || port > 65535 {
		return 0, fmt.Errorf("invalid port %d", port)
	}

	return port, nil
}

func clone(value []byte) []byte {
	copied := make([]byte, len(value))
	copy(copied, value)
	return copied
}

func clientStateFromContext(ctx context.Context) (*server.ClientState, error) {
	state, ok := server.ClientStateFromContext(ctx)
	if !ok || state == nil {
		return nil, fmt.Errorf("client state unavailable")
	}

	return state, nil
}

func (e *Executor) touchWatchKeys(keys ...string) {
	if e.watchRegistry == nil || len(keys) == 0 {
		return
	}

	e.watchRegistry.Touch(keys...)
}

func (e *Executor) countReplicasAtOrAbove(targetOffset int64) int {
	if e.replicaPeers == nil {
		return 0
	}

	return e.replicaPeers.CountReplicasAtOrAbove(targetOffset)
}

func (e *Executor) countReplicasAtOrAboveWithNotify(targetOffset int64) (int, <-chan struct{}) {
	if e.replicaPeers == nil {
		return 0, nil
	}

	return e.replicaPeers.CountReplicasAtOrAboveWithNotify(targetOffset)
}

func (e *Executor) requestReplicaAcknowledgements() error {
	if e.replicaPeers == nil {
		return nil
	}

	encoded, err := cachedReplConfGetAckPayload()
	if err != nil {
		return fmt.Errorf("encode REPLCONF GETACK: %w", err)
	}
	if e.replication != nil {
		e.replication.AdvanceMasterOffset(int64(len(encoded)))
	}

	peers := e.replicaPeers.Snapshot()
	e.logger.Debug("requested replica acknowledgements", "replica_count", len(peers), "payload_size", len(encoded))
	for _, peer := range peers {
		if err := peer.WriteEncoded(encoded); err != nil {
			e.logger.Warn("failed to request replica ACK", "replica_id", peer.ID, "error", err)
			if closeErr := e.replicaPeers.RemoveAndClose(peer.ID); closeErr != nil {
				e.logger.Debug("failed to close replica after ACK request failure", "replica_id", peer.ID, "error", closeErr)
			}
		}
	}

	return nil
}
