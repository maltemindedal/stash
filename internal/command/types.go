package command

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/server"
	"github.com/maltemindedal/stash/internal/storage"
)

// Request is the parsed command sent by a client.
type Request struct {
	Name string
	Args [][]byte

	// effects records what one execution of the request did that its logged
	// and propagated frames depend on: the keys it evicted, SET's absolute
	// expiry, XADD's generated ID. Each execution starts it from zero.
	effects executionEffects
	// inTransaction marks a command EXEC runs from its queue. EXEC holds the
	// sequencer exclusively while it does, so such a command must not wait for
	// anything another request would have to provide.
	inTransaction bool
}

// Handler executes a command against the current server state.
type Handler func(context.Context, *Request) (protocol.Value, error)

// DetailedHandler executes a command that may emit multiple RESP frames.
type DetailedHandler func(context.Context, *Request) (server.ExecuteResult, error)

type commandValidator func(*Request) error

type commandSpec struct {
	handler            Handler
	detailed           DetailedHandler
	validate           commandValidator
	transactionControl bool
	propagates         bool
	durable            bool
	// keys says which arguments are the keys the command writes, so that writes
	// to different keys are not ordered against each other. Left unset, the
	// command is ordered against every write.
	keys keyShape
	// noTransaction refuses the command inside MULTI, when it is queued, and
	// marks the transaction dirty so that EXEC aborts, as Redis does with its
	// NO_MULTI commands.
	noTransaction bool
	// rewriteFrame optionally replaces the verbatim command frame used for
	// replication and AOF durability with a deterministic equivalent. It returns
	// (frame, true) to substitute the frame, or (_, false) to keep the verbatim
	// form. Used by SET to rewrite relative EX/PX expirations to an absolute
	// PXAT so replicas and AOF reloads do not re-anchor the TTL to their own
	// clock.
	rewriteFrame func(*Request) (protocol.Array, bool)
}

type executionEffects struct {
	propagation []protocol.Value
	durability  []protocol.Value
	// setExpiryMillis is the absolute Unix-millis deadline the SET handler
	// computed and stored for this request, carried here so the PXAT rewrite
	// frame reuses that exact value instead of re-deriving it from a later
	// clock. Zero means no expiry (keep the verbatim frame).
	setExpiryMillis int64
	// streamID is the ID the XADD handler stored the entry under, so the logged
	// frame can carry it in place of an auto-ID request ("*"). Empty when the
	// command did not add a stream entry.
	streamID string
}

// Executor routes protocol frames to concrete command handlers.
type Executor struct {
	store               *storage.Store
	logger              *slog.Logger
	watchRegistry       *server.WatchRegistry
	pubSubRegistry      *server.PubSubRegistry
	requirePass         string
	commands            map[string]commandSpec
	replication         *server.ReplicationState
	replicaPeers        *server.ReplicaRegistry
	monitorRegistry     *server.MonitorRegistry
	slowlogRegistry     *server.SlowlogRegistry
	slowlogThreshold    time.Duration
	serverStatsProvider func() server.Stats
	aofRewrite          func(context.Context) error
	seq                 *sequencer
}

// NewExecutor constructs a command executor with the currently supported command set.
func NewExecutor(store *storage.Store, logger *slog.Logger) *Executor {
	executor := &Executor{
		store:          store,
		logger:         logger,
		watchRegistry:  server.NewWatchRegistry(),
		pubSubRegistry: server.NewPubSubRegistry(),
		seq:            newSequencer(),
	}
	executor.commands = executor.commandSpecs()
	return executor
}

// WatchRegistry exposes the shared optimistic-locking registry to the server.
func (e *Executor) WatchRegistry() *server.WatchRegistry {
	return e.watchRegistry
}

// PubSubRegistry exposes the shared exact-channel pub/sub registry to the server.
func (e *Executor) PubSubRegistry() *server.PubSubRegistry {
	return e.pubSubRegistry
}

// SetReplicationState injects shared replication metadata from the server.
func (e *Executor) SetReplicationState(state *server.ReplicationState) {
	e.replication = state
}

// SetReplicaRegistry injects the server's live replica registry.
func (e *Executor) SetReplicaRegistry(registry *server.ReplicaRegistry) {
	e.replicaPeers = registry
}

// SetRequirePass gives AUTH the configured password to verify. It does not
// decide who may run commands; the Client state does (see validateAuthContext).
func (e *Executor) SetRequirePass(password string) {
	e.requirePass = password
}

// SetAOFRewriteTrigger injects the background AOF rewrite hook.
func (e *Executor) SetAOFRewriteTrigger(trigger func(context.Context) error) {
	e.aofRewrite = trigger
}

// SetSlowlogConfig injects the shared slowlog registry and execution threshold.
func (e *Executor) SetSlowlogConfig(registry *server.SlowlogRegistry, threshold time.Duration) {
	e.slowlogRegistry = registry
	e.slowlogThreshold = threshold
}

// SetMonitorRegistry injects the shared MONITOR fan-out registry.
func (e *Executor) SetMonitorRegistry(registry *server.MonitorRegistry) {
	e.monitorRegistry = registry
}

// SetServerStatsProvider injects a snapshot provider used by INFO.
func (e *Executor) SetServerStatsProvider(provider func() server.Stats) {
	e.serverStatsProvider = provider
}

// Execute dispatches a parsed RESP frame to its command handler.
func (e *Executor) Execute(ctx context.Context, value protocol.Value) (protocol.Value, error) {
	result, err := e.ExecuteDetailed(ctx, value)
	if err != nil {
		return nil, err
	}
	if len(result.Responses) == 1 {
		return result.Responses[0], nil
	}

	return nil, fmt.Errorf("command: expected single response, got %d", len(result.Responses))
}

// ExecuteDetailed dispatches a parsed RESP frame to a handler that may emit multiple responses.
func (e *Executor) ExecuteDetailed(ctx context.Context, value protocol.Value) (server.ExecuteResult, error) {
	request, err := DecodeRequest(value)
	if err != nil {
		return server.ExecuteResult{}, err
	}

	return e.executeRequestDetailed(ctx, request, true)
}

func (e *Executor) executeRequestDetailed(ctx context.Context, request *Request, allowQueue bool) (server.ExecuteResult, error) {
	request.effects = executionEffects{}
	effects := &request.effects

	if err := e.validateSubscriptionContext(ctx, request); err != nil {
		return server.ExecuteResult{}, err
	}
	if err := e.validateAuthContext(ctx, request); err != nil {
		return server.ExecuteResult{}, err
	}

	if allowQueue {
		if queued, response, err := e.maybeQueueRequest(ctx, request); queued || err != nil {
			if response == nil {
				return server.ExecuteResult{}, err
			}
			return server.SingleResponse(response), err
		}
	}

	spec, ok := e.command(request.Name)
	if !ok {
		return server.ExecuteResult{}, ErrUnknownCommand(request.Name)
	}
	if spec.validate != nil {
		if err := spec.validate(request); err != nil {
			return server.ExecuteResult{}, err
		}
	}

	startedAt := time.Now()
	if spec.detailed != nil {
		result, err := spec.detailed(ctx, request)
		if err != nil {
			return server.ExecuteResult{}, err
		}
		e.recordSlowCommand(ctx, request, startedAt, time.Since(startedAt))
		result.Propagation = append(result.Propagation, effects.propagation...)
		result.Durability = append(result.Durability, effects.durability...)
		return result, nil
	}
	if spec.handler == nil {
		return server.ExecuteResult{}, fmt.Errorf("command: %s does not support single-response execution", request.Name)
	}

	response, err := spec.handler(ctx, request)
	if err != nil {
		return server.ExecuteResult{}, err
	}
	e.recordSlowCommand(ctx, request, startedAt, time.Since(startedAt))

	propagation, durability := executionFrames(ctx, request, spec)
	result := server.SingleResponse(response)
	result.Propagation = append(propagation, effects.propagation...)
	result.Durability = append(durability, effects.durability...)
	return result, nil
}

func (e *Executor) validateQueueableRequest(request *Request) error {
	spec, ok := e.command(request.Name)
	if !ok {
		return ErrUnknownCommand(request.Name)
	}
	// Refused before its arguments are checked: Redis checks only the arity of
	// a NO_MULTI command inside MULTI, so PSYNC with arguments its validator
	// rejects is still refused for being inside a transaction.
	if spec.noTransaction {
		return ErrNotAllowedInTransactionError()
	}
	if spec.validate != nil {
		return spec.validate(request)
	}

	return nil
}

func (e *Executor) maybeQueueRequest(ctx context.Context, request *Request) (bool, protocol.Value, error) {
	state, ok := server.ClientStateFromContext(ctx)
	if !ok || !state.InTransactionActive() || e.isTransactionControlCommand(request.Name) {
		return false, nil, nil
	}
	if err := e.validateQueueableRequest(request); err != nil {
		state.MarkTransactionDirty()
		return false, nil, err
	}

	state.EnqueueCommand(request.Name, request.Args)
	return true, protocol.SimpleString{Value: "QUEUED"}, nil
}

func responseErrorValue(err error) protocol.ErrorValue {
	prefix := "ERR"

	var typed RESPError
	if errors.As(err, &typed) {
		prefix = typed.RESPErrorPrefix()
	}

	return protocol.ErrorValue{Message: prefix + " " + err.Error()}
}

func executionFrames(ctx context.Context, request *Request, spec commandSpec) ([]protocol.Value, []protocol.Value) {
	var (
		frame     protocol.Array
		haveFrame bool
	)
	ensureFrame := func() protocol.Array {
		if !haveFrame {
			if spec.rewriteFrame != nil {
				if rewritten, ok := spec.rewriteFrame(request); ok {
					frame = rewritten
					haveFrame = true
					return frame
				}
			}
			frame = propagationFrame(request)
			haveFrame = true
		}

		return frame
	}

	var propagation []protocol.Value
	if spec.propagates && !server.IsReplicationOrigin(ctx) {
		propagation = []protocol.Value{ensureFrame()}
	}

	var durability []protocol.Value
	if spec.durable {
		durability = []protocol.Value{ensureFrame()}
	}

	return propagation, durability
}

func propagationFrame(request *Request) protocol.Array {
	elements := make([]protocol.Value, 0, len(request.Args)+1)
	elements = append(elements, protocol.TextBulkString{Value: request.Name})
	for _, arg := range request.Args {
		elements = append(elements, protocol.BulkString{Data: arg})
	}

	return protocol.Array{Elements: elements}
}

// rewriteSetFrame rewrites a SET carrying a relative EX/PX expiration into
// `SET key value PXAT <absolute-ms>` so replicas and AOF replay anchor the
// expiry to the master's clock at execution time rather than re-evaluating the
// relative window against their own, later, clock. The absolute deadline is the
// one the handler already computed and stored (carried on the request's
// effects), so the propagated frame matches the master's stored expiry exactly.
// SET without an expiration keeps its verbatim frame.
func rewriteSetFrame(request *Request) (protocol.Array, bool) {
	effects := &request.effects
	if effects.setExpiryMillis <= 0 {
		return protocol.Array{}, false
	}

	elements := []protocol.Value{
		protocol.TextBulkString{Value: request.Name},
		protocol.BulkString{Data: request.Args[0]},
		protocol.BulkString{Data: request.Args[1]},
		protocol.TextBulkString{Value: "PXAT"},
		protocol.BulkString{Data: []byte(strconv.FormatInt(effects.setExpiryMillis, 10))},
	}
	return protocol.Array{Elements: elements}, true
}

// rewriteXAddFrame replaces the ID argument of an XADD that asked for an
// auto-generated ID with the one the handler stored. Replaying the verbatim
// frame would generate a new ID from the clock at replay time, so entries came
// back from the AOF under different IDs than the ones clients had been given.
// An XADD with an explicit ID keeps its verbatim frame.
func rewriteXAddFrame(request *Request) (protocol.Array, bool) {
	effects := &request.effects
	if effects.streamID == "" || effects.streamID == string(request.Args[1]) {
		return protocol.Array{}, false
	}

	frame := propagationFrame(request)
	frame.Elements[2] = protocol.TextBulkString{Value: effects.streamID}
	return frame, true
}

func (e *Executor) recordEvictedKeys(ctx context.Context, request *Request, keys []string) {
	if len(keys) == 0 {
		return
	}

	e.touchWatchKeys(keys...)
	frame := server.DeleteFrame(keys)
	if !server.IsReplicationOrigin(ctx) {
		request.effects.propagation = append(request.effects.propagation, frame)
	}
	request.effects.durability = append(request.effects.durability, frame)
}

// DecodeRequest converts a RESP array into a command request.
func DecodeRequest(value protocol.Value) (*Request, error) {
	array, ok := value.(protocol.Array)
	if !ok || array.Null {
		return nil, ErrProtocol("expected non-null RESP array")
	}
	if len(array.Elements) == 0 {
		return nil, ErrProtocol("expected array with at least one element")
	}

	name, err := protocol.Bytes(array.Elements[0])
	if err != nil {
		return nil, ErrProtocol(err.Error())
	}

	args := make([][]byte, len(array.Elements)-1)
	for i, element := range array.Elements[1:] {
		arg, err := protocol.Bytes(element)
		if err != nil {
			return nil, ErrProtocol(err.Error())
		}
		args[i] = arg
	}

	return &Request{
		Name: strings.ToUpper(string(name)),
		Args: args,
	}, nil
}

func (e *Executor) command(name string) (commandSpec, bool) {
	spec, ok := e.commands[name]
	return spec, ok
}

func (e *Executor) isTransactionControlCommand(name string) bool {
	spec, ok := e.command(name)
	return ok && spec.transactionControl
}

func (e *Executor) commandSpecs() map[string]commandSpec {
	return map[string]commandSpec{
		"SUBSCRIBE": {
			detailed:           e.handleSubscribe,
			validate:           validateSubscribeRequest,
			transactionControl: true,
		},
		"UNSUBSCRIBE": {
			detailed:           e.handleUnsubscribe,
			validate:           validateUnsubscribeRequest,
			transactionControl: true,
		},
		"WATCH": {
			handler:            e.handleWatch,
			validate:           validateWatchRequest,
			transactionControl: true,
		},
		"MULTI": {
			handler:            e.handleMulti,
			validate:           exactArgsValidator("MULTI", 0),
			transactionControl: true,
		},
		"EXEC": {
			detailed:           e.handleExec,
			validate:           exactArgsValidator("EXEC", 0),
			transactionControl: true,
		},
		"DISCARD": {
			handler:            e.handleDiscard,
			validate:           exactArgsValidator("DISCARD", 0),
			transactionControl: true,
		},
		"PING": {
			handler:  e.handlePing,
			validate: maxArgsValidator("PING", 1),
		},
		"AUTH": {
			handler:  e.handleAuth,
			validate: exactArgsValidator("AUTH", 1),
		},
		"ECHO": {
			handler:  e.handleEcho,
			validate: exactArgsValidator("ECHO", 1),
		},
		"SET": {
			handler:      e.handleSet,
			validate:     validateSetRequest,
			propagates:   true,
			durable:      true,
			rewriteFrame: rewriteSetFrame,
			keys:         keysFirstArg,
		},
		"GET": {
			handler:  e.handleGet,
			validate: exactArgsValidator("GET", 1),
		},
		"SETBIT": {
			handler:    e.handleSetBit,
			validate:   validateSetBitRequest,
			propagates: true,
			durable:    true,
			keys:       keysFirstArg,
		},
		"GETBIT": {
			handler:  e.handleGetBit,
			validate: validateGetBitRequest,
		},
		"BITCOUNT": {
			handler:  e.handleBitCount,
			validate: validateBitCountRequest,
		},
		"PFADD": {
			handler:    e.handlePFAdd,
			validate:   minArgsValidator("PFADD", 1),
			propagates: true,
			durable:    true,
			keys:       keysFirstArg,
		},
		"PFCOUNT": {
			handler:  e.handlePFCount,
			validate: minArgsValidator("PFCOUNT", 1),
		},
		"DEL": {
			handler:    e.handleDel,
			validate:   minArgsValidator("DEL", 1),
			propagates: true,
			durable:    true,
			keys:       keysEveryArg,
		},
		"INCR": {
			handler:    e.handleIncr,
			validate:   exactArgsValidator("INCR", 1),
			propagates: true,
			durable:    true,
			keys:       keysFirstArg,
		},
		"LPUSH": {
			handler:    e.handleLPush,
			validate:   minArgsValidator("LPUSH", 2),
			propagates: true,
			durable:    true,
			keys:       keysFirstArg,
		},
		"RPUSH": {
			handler:    e.handleRPush,
			validate:   minArgsValidator("RPUSH", 2),
			propagates: true,
			durable:    true,
			keys:       keysFirstArg,
		},
		"LRANGE": {
			handler:  e.handleLRange,
			validate: validateLRangeRequest,
		},
		"LPOP": {
			handler:    e.handleLPop,
			validate:   validateLPopRequest,
			propagates: true,
			durable:    true,
			keys:       keysFirstArg,
		},
		"RPOP": {
			handler:    e.handleRPop,
			validate:   validateRPopRequest,
			propagates: true,
			durable:    true,
			keys:       keysFirstArg,
		},
		"BLPOP": {
			detailed: e.handleBLPop,
			validate: exactArgsValidator("BLPOP", 1),
		},
		"ZADD": {
			handler:    e.handleZAdd,
			validate:   validateZAddRequest,
			propagates: true,
			durable:    true,
			keys:       keysFirstArg,
		},
		"ZRANGE": {
			handler:  e.handleZRange,
			validate: validateZRangeRequest,
		},
		"GEOADD": {
			handler:    e.handleGeoAdd,
			validate:   validateGeoAddRequest,
			propagates: true,
			durable:    true,
			keys:       keysFirstArg,
		},
		"GEODIST": {
			handler:  e.handleGeoDist,
			validate: validateGeoDistRequest,
		},
		"GEORADIUS": {
			handler:  e.handleGeoRadius,
			validate: validateGeoRadiusRequest,
		},
		"XADD": {
			handler:      e.handleXAdd,
			validate:     validateXAddRequest,
			durable:      true,
			rewriteFrame: rewriteXAddFrame,
			keys:         keysFirstArg,
		},
		"XREAD": {
			handler:  e.handleXRead,
			validate: validateXReadRequest,
		},
		"HSET": {
			handler:    e.handleHSet,
			validate:   validateHSetRequest,
			propagates: true,
			durable:    true,
			keys:       keysFirstArg,
		},
		"HGET": {
			handler:  e.handleHGet,
			validate: exactArgsValidator("HGET", 2),
		},
		"HDEL": {
			handler:    e.handleHDel,
			validate:   minArgsValidator("HDEL", 2),
			propagates: true,
			durable:    true,
			keys:       keysFirstArg,
		},
		"HGETALL": {
			handler:  e.handleHGetAll,
			validate: exactArgsValidator("HGETALL", 1),
		},
		"SADD": {
			handler:    e.handleSAdd,
			validate:   minArgsValidator("SADD", 2),
			propagates: true,
			durable:    true,
			keys:       keysFirstArg,
		},
		"SISMEMBER": {
			handler:  e.handleSIsMember,
			validate: exactArgsValidator("SISMEMBER", 2),
		},
		"SREM": {
			handler:    e.handleSRem,
			validate:   minArgsValidator("SREM", 2),
			propagates: true,
			durable:    true,
			keys:       keysFirstArg,
		},
		"SMEMBERS": {
			handler:  e.handleSMembers,
			validate: exactArgsValidator("SMEMBERS", 1),
		},
		"PUBLISH": {
			handler:    e.handlePublish,
			validate:   validatePublishRequest,
			propagates: true,
			keys:       keysFirstArg,
		},
		"REPLCONF": {
			detailed: e.handleReplConf,
			validate: validateReplConfRequest,
		},
		"PSYNC": {
			detailed: e.handlePSync,
			validate: validatePSyncRequest,
			// As in Redis, which marks PSYNC NO_MULTI. EXEC returns one
			// reply per queued command, and PSYNC answers with two frames
			// and turns the connection into a replica. PSYNC's attach cut
			// would also wait forever for the gate EXEC holds.
			noTransaction: true,
		},
		"WAIT": {
			detailed: e.handleWait,
			validate: validateWaitRequest,
		},
		"BGREWRITEAOF": {
			handler:  e.handleBGRewriteAOF,
			validate: exactArgsValidator("BGREWRITEAOF", 0),
		},
		"INFO": {
			handler:  e.handleInfo,
			validate: validateInfoRequest,
		},
		"SLOWLOG": {
			handler:  e.handleSlowlog,
			validate: validateSlowlogRequest,
		},
		"MONITOR": {
			handler:            e.handleMonitor,
			validate:           exactArgsValidator("MONITOR", 0),
			transactionControl: true,
		},
	}
}

func (e *Executor) validateSubscriptionContext(ctx context.Context, request *Request) error {
	state, ok := server.ClientStateFromContext(ctx)
	if !ok || state == nil {
		return nil
	}

	if state.IsSubscribed() && !isAllowedSubscribedModeCommand(request.Name) {
		return ErrSubscribedModeOnlyError()
	}
	if state.IsMonitoring() && !isAllowedMonitoringModeCommand(request.Name) {
		return ErrMonitorModeOnlyError()
	}

	return nil
}

func (e *Executor) recordSlowCommand(ctx context.Context, request *Request, timestamp time.Time, duration time.Duration) {
	if e.slowlogRegistry == nil || request == nil || e.slowlogThreshold < 0 || duration < e.slowlogThreshold || server.IsReplicationOrigin(ctx) {
		return
	}

	entry := server.SlowlogEntry{
		Timestamp: timestamp,
		Duration:  duration,
		Command:   requestTokens(request),
	}
	if state, ok := server.ClientStateFromContext(ctx); ok && state != nil {
		entry.ClientAddr = state.RemoteAddress()
	}
	e.slowlogRegistry.Record(entry)
}

// errNoClientState refuses a client request that reached the executor without
// the Client state the server gives every connection.
var errNoClientState = errors.New("command: client request without a client state")

// validateAuthContext is the auth gate. It decides from the Client state alone,
// which the server creates unauthenticated whenever a password is required. The
// executor's own copy of the password only verifies AUTH, so a password that
// never reached the executor leaves every command but AUTH and PING refused
// rather than open. A client request that arrives without a Client state is
// refused too, instead of running as if it had authenticated. Only the Master's
// replication stream and AOF replay, which come from no client, run without one.
func (e *Executor) validateAuthContext(ctx context.Context, request *Request) error {
	state, _ := server.ClientStateFromContext(ctx)
	if state != nil && state.IsAuthenticated() {
		return nil
	}
	if server.IsReplicationOrigin(ctx) {
		return nil
	}
	if state == nil {
		return errNoClientState
	}
	if isAllowedUnauthenticatedCommand(request.Name) {
		return nil
	}

	return ErrNoAuthError()
}

func isAllowedUnauthenticatedCommand(name string) bool {
	switch name {
	case "AUTH", "PING":
		return true
	default:
		return false
	}
}

func isAllowedSubscribedModeCommand(name string) bool {
	switch name {
	case "PING", "SUBSCRIBE", "UNSUBSCRIBE":
		return true
	default:
		return false
	}
}

func isAllowedMonitoringModeCommand(name string) bool {
	return name == "PING"
}

func exactArgsValidator(name string, count int) commandValidator {
	return func(request *Request) error {
		if len(request.Args) != count {
			return wrongNumberOfArgumentsError(name)
		}
		return nil
	}
}

func maxArgsValidator(name string, maxArgs int) commandValidator {
	return func(request *Request) error {
		if len(request.Args) > maxArgs {
			return wrongNumberOfArgumentsError(name)
		}
		return nil
	}
}

func minArgsValidator(name string, minArgs int) commandValidator {
	return func(request *Request) error {
		if len(request.Args) < minArgs {
			return wrongNumberOfArgumentsError(name)
		}
		return nil
	}
}

func validateWatchRequest(request *Request) error {
	if len(request.Args) == 0 {
		return wrongNumberOfArgumentsError("WATCH")
	}
	return nil
}

func validateSubscribeRequest(request *Request) error {
	if len(request.Args) == 0 {
		return wrongNumberOfArgumentsError("SUBSCRIBE")
	}

	return validateNonEmptyPubSubChannels(request.Args...)
}

func validateUnsubscribeRequest(request *Request) error {
	if len(request.Args) == 0 {
		return nil
	}

	return validateNonEmptyPubSubChannels(request.Args...)
}

func validatePublishRequest(request *Request) error {
	if len(request.Args) != 2 {
		return wrongNumberOfArgumentsError("PUBLISH")
	}

	return validateNonEmptyPubSubChannels(request.Args[0])
}

func validateNonEmptyPubSubChannels(channels ...[]byte) error {
	for _, channel := range channels {
		if len(channel) == 0 {
			return ErrSyntaxError()
		}
	}

	return nil
}

func validateSetRequest(request *Request) error {
	if len(request.Args) < 2 {
		return wrongNumberOfArgumentsError("SET")
	}
	if _, err := storage.ParseExpiryMillis(request.Args[2:]); err != nil {
		switch {
		case errors.Is(err, storage.ErrSyntax):
			return ErrSyntaxError()
		case errors.Is(err, storage.ErrInvalidExpireTime):
			return ErrInvalidExpireTimeError()
		default:
			return err
		}
	}
	return nil
}

func validateSetBitRequest(request *Request) error {
	if len(request.Args) != 3 {
		return wrongNumberOfArgumentsError("SETBIT")
	}
	if _, err := parseBitmapOffsetArgument(request.Args[1]); err != nil {
		return err
	}
	if _, err := parseBitValueArgument(request.Args[2]); err != nil {
		return err
	}
	return nil
}

func validateGetBitRequest(request *Request) error {
	if len(request.Args) != 2 {
		return wrongNumberOfArgumentsError("GETBIT")
	}
	if _, err := parseBitmapOffsetArgument(request.Args[1]); err != nil {
		return err
	}
	return nil
}

func validateBitCountRequest(request *Request) error {
	if len(request.Args) != 1 && len(request.Args) != 3 {
		return wrongNumberOfArgumentsError("BITCOUNT")
	}
	if len(request.Args) == 3 {
		if _, err := parseIntegerArgument(request.Args[1]); err != nil {
			return err
		}
		if _, err := parseIntegerArgument(request.Args[2]); err != nil {
			return err
		}
	}
	return nil
}

func validateLRangeRequest(request *Request) error {
	if len(request.Args) != 3 {
		return wrongNumberOfArgumentsError("LRANGE")
	}
	if _, err := parseIntegerArgument(request.Args[1]); err != nil {
		return err
	}
	if _, err := parseIntegerArgument(request.Args[2]); err != nil {
		return err
	}
	return nil
}

func validateZAddRequest(request *Request) error {
	if len(request.Args) < 3 || len(request.Args)%2 == 0 {
		return wrongNumberOfArgumentsError("ZADD")
	}
	for i := 1; i < len(request.Args); i += 2 {
		if _, err := parseFloatArgument(request.Args[i]); err != nil {
			return err
		}
	}
	return nil
}

func validateZRangeRequest(request *Request) error {
	if len(request.Args) != 3 && len(request.Args) != 4 {
		return wrongNumberOfArgumentsError("ZRANGE")
	}
	if len(request.Args) == 4 && !strings.EqualFold(string(request.Args[3]), "WITHSCORES") {
		return ErrSyntaxError()
	}
	if _, err := parseIntegerArgument(request.Args[1]); err != nil {
		return err
	}
	if _, err := parseIntegerArgument(request.Args[2]); err != nil {
		return err
	}
	return nil
}

func validateGeoAddRequest(request *Request) error {
	if len(request.Args) < 4 || (len(request.Args)-1)%3 != 0 {
		return wrongNumberOfArgumentsError("GEOADD")
	}
	for i := 1; i < len(request.Args); i += 3 {
		if _, _, err := parseGeoCoordinates(request.Args[i], request.Args[i+1]); err != nil {
			return err
		}
	}
	return nil
}

func validateGeoDistRequest(request *Request) error {
	if len(request.Args) != 3 && len(request.Args) != 4 {
		return wrongNumberOfArgumentsError("GEODIST")
	}
	if len(request.Args) == 4 {
		if _, err := parseGeoUnitArgument(request.Args[3]); err != nil {
			return err
		}
	}
	return nil
}

func validateGeoRadiusRequest(request *Request) error {
	if len(request.Args) < 5 {
		return wrongNumberOfArgumentsError("GEORADIUS")
	}
	if len(request.Args) > 5 {
		return ErrSyntaxError()
	}
	if _, _, err := parseGeoCoordinates(request.Args[1], request.Args[2]); err != nil {
		return err
	}
	if _, err := parseGeoRadiusArgument(request.Args[3]); err != nil {
		return err
	}
	if _, err := parseGeoUnitArgument(request.Args[4]); err != nil {
		return err
	}
	return nil
}

func validateXAddRequest(request *Request) error {
	if len(request.Args) < 4 || len(request.Args)%2 != 0 {
		return wrongNumberOfArgumentsError("XADD")
	}
	if err := storage.ValidateXAddID(string(request.Args[1])); err != nil {
		if errors.Is(err, storage.ErrInvalidStreamID) {
			return ErrInvalidStreamIDError()
		}
		return err
	}
	return nil
}

func validateXReadRequest(request *Request) error {
	if len(request.Args) == 0 {
		return wrongNumberOfArgumentsError("XREAD")
	}
	if !strings.EqualFold(string(request.Args[0]), "STREAMS") {
		return ErrSyntaxError()
	}
	if len(request.Args) != 3 {
		return ErrSyntaxError()
	}
	if err := storage.ValidateXReadID(string(request.Args[2])); err != nil {
		if errors.Is(err, storage.ErrInvalidStreamID) {
			return ErrInvalidStreamIDError()
		}
		return err
	}
	return nil
}

func validateReplConfRequest(request *Request) error {
	if len(request.Args) != 2 {
		return wrongNumberOfArgumentsError("REPLCONF")
	}

	subcommand := strings.ToUpper(string(request.Args[0]))
	switch subcommand {
	case "LISTENING-PORT":
		if _, err := parseReplicationPortArgument(request.Args[1]); err != nil {
			return ErrSyntaxError()
		}
	case "GETACK":
		if string(request.Args[1]) != "*" {
			return ErrSyntaxError()
		}
	case "ACK":
		value, err := parseIntegerArgument(request.Args[1])
		if err != nil || value < 0 {
			return ErrSyntaxError()
		}
	default:
		return ErrSyntaxError()
	}

	return nil
}

func validatePSyncRequest(request *Request) error {
	if len(request.Args) != 2 {
		return wrongNumberOfArgumentsError("PSYNC")
	}
	if string(request.Args[0]) != "?" || string(request.Args[1]) != "-1" {
		return ErrSyntaxError()
	}
	return nil
}

func validateWaitRequest(request *Request) error {
	if len(request.Args) != 2 {
		return wrongNumberOfArgumentsError("WAIT")
	}
	for _, arg := range request.Args {
		value, err := parseIntegerArgument(arg)
		if err != nil {
			return err
		}
		if value < 0 {
			return ErrValueNotIntegerError()
		}
	}
	return nil
}
