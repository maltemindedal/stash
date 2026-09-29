package server

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/maltemindedal/stash/internal/protocol"
)

// QueuedCommand stores command metadata queued for transactional EXEC.
type QueuedCommand struct {
	Name string
	Args [][]byte
}

// ClientState holds connection-scoped state for auth and transaction features.
type ClientState struct {
	ID         uint64
	RemoteAddr string

	mu sync.RWMutex
	// responseMu guards responseWriter and writerClosed and is held across each
	// whole-frame write, which blocks for as long as the peer does not read.
	// responseConn is guarded by mu instead, so Disconnect can close it to end a
	// write that holds responseMu.
	responseMu sync.Mutex

	watchRegistry *WatchRegistry
	watchedKeys   map[string]struct{}
	// pubSubRegistry and subscribedChannels mirror the same exact-channel
	// membership. Registry mutations take PubSubRegistry.mu before mutating the
	// client-local subscribedChannels set via ClientState.mu.
	pubSubRegistry     *PubSubRegistry
	subscribedChannels map[string]struct{}
	monitorRegistry    *MonitorRegistry
	responseWriter     *bufio.Writer
	responseConn       net.Conn
	writerClosed       bool

	Authenticated     bool
	Replica           bool
	Monitoring        bool
	ReplicaListenPort int
	LastWriteOffset   int64
	InTransaction     bool
	TxFailed          bool
	TxDirty           bool
	TxQueue           []QueuedCommand
}

type clientStateContextKey struct{}

// WithClientState attaches a connection-scoped client state to ctx.
func WithClientState(ctx context.Context, state *ClientState) context.Context {
	return context.WithValue(ctx, clientStateContextKey{}, state)
}

// ClientStateFromContext retrieves the connection-scoped client state from ctx.
func ClientStateFromContext(ctx context.Context) (*ClientState, bool) {
	state, ok := ctx.Value(clientStateContextKey{}).(*ClientState)
	return state, ok
}

// BeginTransaction marks the client as inside a transaction.
// It preserves any optimistic-lock invalidation recorded by prior WATCH
// activity so a watched key changed before MULTI still aborts the next EXEC.
// It returns false when the client is already inside a transaction.
func (s *ClientState) BeginTransaction() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.InTransaction {
		return false
	}

	s.InTransaction = true
	s.TxDirty = false
	s.TxQueue = nil
	return true
}

// SetWatchRegistry binds the shared watch registry to the client state.
func (s *ClientState) SetWatchRegistry(registry *WatchRegistry) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.watchRegistry = registry
	if s.watchedKeys == nil {
		s.watchedKeys = make(map[string]struct{})
	}
}

// SetPubSubRegistry binds the shared pub/sub registry to the client state.
func (s *ClientState) SetPubSubRegistry(registry *PubSubRegistry) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.pubSubRegistry = registry
	if s.subscribedChannels == nil {
		s.subscribedChannels = make(map[string]struct{})
	}
}

// SetMonitorRegistry binds the shared MONITOR fan-out registry to the client state.
func (s *ClientState) SetMonitorRegistry(registry *MonitorRegistry) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.monitorRegistry = registry
}

// BindResponseWriter binds the connection-scoped writer used for command
// replies, async push messages, and replica propagation. All writes for a
// given connection must flow through this writer to avoid interleaving.
func (s *ClientState) BindResponseWriter(writer *bufio.Writer) {
	s.responseMu.Lock()
	defer s.responseMu.Unlock()

	s.responseWriter = writer
	s.writerClosed = writer == nil
}

// BindResponseConn records the underlying connection used by the response
// writer when one is available.
func (s *ClientState) BindResponseConn(conn net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.responseConn = conn
}

// HasActiveResponseWriter reports whether the client can currently receive
// command replies or async push messages.
func (s *ClientState) HasActiveResponseWriter() bool {
	if s == nil {
		return false
	}

	s.responseMu.Lock()
	defer s.responseMu.Unlock()

	return !s.writerClosed && s.responseWriter != nil
}

// Disconnect marks the client inactive and detaches it from shared registries so
// future async deliveries fail fast.
func (s *ClientState) Disconnect() {
	if s == nil {
		return
	}

	// A reply flush stuck on a peer that stopped reading holds responseMu until
	// the connection closes, so close it before waiting for the lock.
	s.mu.Lock()
	conn := s.responseConn
	s.responseConn = nil
	s.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}

	s.responseMu.Lock()
	s.writerClosed = true
	s.responseWriter = nil
	s.responseMu.Unlock()

	s.ResetTransaction()
	s.StopMonitoring()
	s.UnsubscribeAll()
	s.UnwatchAll()
}

// SetAuthenticated records whether the client has successfully authenticated.
func (s *ClientState) SetAuthenticated(authenticated bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.Authenticated = authenticated
}

// SetRemoteAddr records the client's network address for observability output.
func (s *ClientState) SetRemoteAddr(addr string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.RemoteAddr = addr
}

// RemoteAddress returns the client's recorded network address.
func (s *ClientState) RemoteAddress() string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.RemoteAddr
}

// IsAuthenticated reports whether the client has successfully authenticated.
func (s *ClientState) IsAuthenticated() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.Authenticated
}

// WriteResponses writes RESP values to the bound client writer and flushes them,
// without allowing interleaving with other goroutines writing to the same
// connection.
func (s *ClientState) WriteResponses(values []protocol.Value) error {
	return s.writeResponses(values, true)
}

// QueueResponses writes RESP values to the bound client writer without flushing
// them, so replies to a client that pipelines its requests leave in one write.
// They are sent by the next flush: FlushResponses, a push to the client, or a
// full buffer. The connection handler flushes just before it waits for more
// input.
func (s *ClientState) QueueResponses(values []protocol.Value) error {
	return s.writeResponses(values, false)
}

// FlushResponses sends whatever has been queued for the client, waiting for any
// other writer to finish first. Having nothing to send is success, including when
// the client is already disconnected.
func (s *ClientState) FlushResponses() error {
	if s == nil {
		return nil
	}

	s.responseMu.Lock()
	defer s.responseMu.Unlock()

	return s.flushLocked()
}

// TryFlushResponses is FlushResponses, except that it does nothing when another
// goroutine is writing to the client at that moment. That writer (a push, replica
// propagation) flushes the shared buffer itself, queued replies included, so
// nothing is lost by not waiting; and waiting could park the connection handler
// behind a peer that has stopped reading, so that it stops reading that peer's
// own requests, which for a replica means its acknowledgements.
func (s *ClientState) TryFlushResponses() error {
	if s == nil || !s.responseMu.TryLock() {
		return nil
	}
	defer s.responseMu.Unlock()

	return s.flushLocked()
}

func (s *ClientState) flushLocked() error {
	if s.writerClosed || s.responseWriter == nil {
		return nil
	}
	return s.responseWriter.Flush()
}

// FlushClientResponses flushes the replies queued for the client that ctx belongs
// to, without waiting for other writers (see TryFlushResponses). A command that is
// about to block the connection calls it first, so replies to the requests before
// it are not held back behind it.
func FlushClientResponses(ctx context.Context) error {
	state, ok := ClientStateFromContext(ctx)
	if !ok {
		return nil
	}
	return state.TryFlushResponses()
}

// ErrClientDisconnected reports that a command was waiting for something and
// the client went away while it waited.
var ErrClientDisconnected = errors.New("server: client disconnected")

// ClientDisconnected reports whether the client behind ctx has closed its side of
// the connection, without reading from it. A command that blocks for a long time
// polls it, so it can stop waiting for a client that is not there. It is false
// when there is no connection to look at (the event loop, an unsupported
// platform, a context with no client) and while the client has sent requests the
// server has not yet read.
func ClientDisconnected(ctx context.Context) bool {
	state, ok := ClientStateFromContext(ctx)
	if !ok || state == nil {
		return false
	}

	state.mu.RLock()
	conn := state.responseConn
	state.mu.RUnlock()
	if conn == nil {
		return false
	}
	return peerClosed(conn)
}

func (s *ClientState) writeResponses(values []protocol.Value, flush bool) error {
	var payload []byte
	if len(values) > 1 {
		var err error
		payload, err = protocol.EncodeValues(values)
		if err != nil {
			return err
		}
	}

	s.responseMu.Lock()
	defer s.responseMu.Unlock()

	if s.writerClosed || s.responseWriter == nil {
		return fmt.Errorf("client response writer unavailable")
	}

	if len(values) == 1 {
		if err := protocol.WriteValue(s.responseWriter, values[0]); err != nil {
			return err
		}
	} else if _, err := s.responseWriter.Write(payload); err != nil {
		return err
	}

	if !flush {
		return nil
	}
	return s.responseWriter.Flush()
}

// WriteEncoded writes a pre-encoded RESP payload to the bound client writer
// without allowing interleaving with other goroutines writing to the same
// connection.
func (s *ClientState) WriteEncoded(payload []byte) error {
	s.responseMu.Lock()
	defer s.responseMu.Unlock()

	if s.writerClosed || s.responseWriter == nil {
		return fmt.Errorf("client response writer unavailable")
	}
	if _, err := s.responseWriter.Write(payload); err != nil {
		return err
	}

	return s.responseWriter.Flush()
}

// WriteEncodedWithDeadline writes a pre-encoded RESP payload with a temporary
// write deadline when the underlying connection is known. Waiting for the
// connection's writer counts against the same timeout, so a client whose own
// reply is stuck cannot hold up the goroutine pushing to it.
func (s *ClientState) WriteEncodedWithDeadline(payload []byte, timeout time.Duration) error {
	if !s.lockResponses(timeout) {
		return fmt.Errorf("client response writer busy for %v", timeout)
	}
	defer s.responseMu.Unlock()

	if s.writerClosed || s.responseWriter == nil {
		return fmt.Errorf("client response writer unavailable")
	}
	s.mu.RLock()
	conn := s.responseConn
	s.mu.RUnlock()

	deadlineSet := false
	if conn != nil && timeout > 0 {
		if err := conn.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
			return err
		}
		deadlineSet = true
	}
	_, writeErr := s.responseWriter.Write(payload)
	if writeErr == nil {
		writeErr = s.responseWriter.Flush()
	}
	if deadlineSet {
		if clearErr := conn.SetWriteDeadline(time.Time{}); writeErr == nil {
			writeErr = clearErr
		}
	}

	return writeErr
}

// lockResponses takes responseMu and reports whether it got it within timeout.
// A client whose own reply flush is blocked on a peer that stopped reading holds
// responseMu for as long as that peer stays silent, so waiting for it without a
// bound would let one such client stall everything that pushes to it (PUBLISH
// and MONITOR fan-out run on other clients' request paths). A non-positive
// timeout waits as long as it takes.
func (s *ClientState) lockResponses(timeout time.Duration) bool {
	if timeout <= 0 {
		s.responseMu.Lock()
		return true
	}
	if s.responseMu.TryLock() {
		return true
	}

	deadline := time.Now().Add(timeout)
	for wait := 50 * time.Microsecond; time.Now().Before(deadline); wait = min(2*wait, 5*time.Millisecond) {
		time.Sleep(wait)
		if s.responseMu.TryLock() {
			return true
		}
	}
	return false
}

// SetReplicaListeningPort records the port announced during REPLCONF listening-port.
func (s *ClientState) SetReplicaListeningPort(port int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.ReplicaListenPort = port
}

// ReplicaListeningPort returns the port announced by the replica, if any.
func (s *ClientState) ReplicaListeningPort() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.ReplicaListenPort
}

// PromoteToReplica marks the connection as a replica peer.
func (s *ClientState) PromoteToReplica() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.Replica = true
}

// IsReplica reports whether the connection has completed replica handshake setup.
func (s *ClientState) IsReplica() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.Replica
}

// StartMonitoring registers the client for MONITOR event fan-out.
func (s *ClientState) StartMonitoring() {
	s.mu.RLock()
	registry := s.monitorRegistry
	s.mu.RUnlock()

	if registry == nil {
		return
	}

	registry.Subscribe(s)
}

// StopMonitoring unregisters the client from MONITOR event fan-out.
func (s *ClientState) StopMonitoring() {
	s.mu.RLock()
	registry := s.monitorRegistry
	s.mu.RUnlock()

	if registry == nil {
		s.setMonitoring(false)
		return
	}

	registry.Unsubscribe(s)
}

// IsMonitoring reports whether the client is currently in MONITOR mode.
func (s *ClientState) IsMonitoring() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.Monitoring
}

// SetLastWriteReplicationOffset records the replication offset produced by the
// most recent write command issued by this client.
func (s *ClientState) SetLastWriteReplicationOffset(offset int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.LastWriteOffset = offset
}

// LastWriteReplicationOffset returns the replication offset produced by the
// client's most recent write command.
func (s *ClientState) LastWriteReplicationOffset() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.LastWriteOffset
}

// InTransactionActive reports whether the client is currently inside a transaction.
func (s *ClientState) InTransactionActive() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.InTransaction
}

// EnqueueCommand appends a command to the transaction queue.
func (s *ClientState) EnqueueCommand(name string, args [][]byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	queuedArgs := make([][]byte, 0, len(args))
	for _, arg := range args {
		queuedArgs = append(queuedArgs, bytes.Clone(arg))
	}

	s.TxQueue = append(s.TxQueue, QueuedCommand{
		Name: name,
		Args: queuedArgs,
	})
}

// WatchKeys registers the supplied keys for optimistic locking.
func (s *ClientState) WatchKeys(keys ...string) {
	s.mu.RLock()
	registry := s.watchRegistry
	s.mu.RUnlock()

	if registry == nil {
		return
	}

	registry.Watch(s, keys...)
}

// UnwatchAll removes all watched keys for the client and clears failure state.
func (s *ClientState) UnwatchAll() {
	s.mu.RLock()
	registry := s.watchRegistry
	s.mu.RUnlock()

	if registry != nil {
		registry.UnwatchAll(s)
	}

	s.ClearTransactionFailure()
}

// SubscriptionCount reports how many exact channels this client is currently subscribed to.
func (s *ClientState) SubscriptionCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return len(s.subscribedChannels)
}

// IsSubscribed reports whether the client is currently in subscribed mode.
func (s *ClientState) IsSubscribed() bool {
	return s.SubscriptionCount() > 0
}

// SubscribedChannels returns the currently subscribed channel names in sorted order.
func (s *ClientState) SubscribedChannels() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	channels := make([]string, 0, len(s.subscribedChannels))
	for channel := range s.subscribedChannels {
		channels = append(channels, channel)
	}
	sort.Strings(channels)
	return channels
}

// SubscribeChannel registers the client on channel.
func (s *ClientState) SubscribeChannel(channel string) {
	s.mu.RLock()
	registry := s.pubSubRegistry
	s.mu.RUnlock()

	if registry == nil {
		return
	}

	registry.Subscribe(s, channel)
}

// UnsubscribeChannel removes the client from channel.
func (s *ClientState) UnsubscribeChannel(channel string) {
	s.mu.RLock()
	registry := s.pubSubRegistry
	s.mu.RUnlock()

	if registry == nil {
		return
	}

	registry.Unsubscribe(s, channel)
}

// UnsubscribeAll removes all pub/sub subscriptions for the client.
func (s *ClientState) UnsubscribeAll() {
	s.mu.RLock()
	registry := s.pubSubRegistry
	s.mu.RUnlock()

	if registry == nil {
		return
	}

	registry.UnsubscribeAll(s)
}

// TransactionFailed reports whether optimistic locking marked the transaction as aborted.
func (s *ClientState) TransactionFailed() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.TxFailed
}

// MarkTransactionFailed marks the transaction as invalidated by a watched-key mutation.
func (s *ClientState) MarkTransactionFailed() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.TxFailed = true
}

// ClearTransactionFailure resets the optimistic-locking failure flag.
func (s *ClientState) ClearTransactionFailure() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.TxFailed = false
}

// TransactionDirty reports whether queue-time validation has already failed.
func (s *ClientState) TransactionDirty() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.TxDirty
}

// MarkTransactionDirty marks the current transaction as invalid due to queue-time errors.
func (s *ClientState) MarkTransactionDirty() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.TxDirty = true
}

func (s *ClientState) addWatchedKey(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.watchedKeys == nil {
		s.watchedKeys = make(map[string]struct{})
	}
	s.watchedKeys[key] = struct{}{}
}

func (s *ClientState) addSubscribedChannel(channel string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.subscribedChannels == nil {
		s.subscribedChannels = make(map[string]struct{})
	}
	s.subscribedChannels[channel] = struct{}{}
}

func (s *ClientState) removeSubscribedChannel(channel string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.subscribedChannels, channel)
}

func (s *ClientState) setMonitoring(monitoring bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.Monitoring = monitoring
}

func (s *ClientState) drainWatchedKeys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	keys := make([]string, 0, len(s.watchedKeys))
	for key := range s.watchedKeys {
		keys = append(keys, key)
	}
	clear(s.watchedKeys)
	return keys
}

func (s *ClientState) drainSubscribedChannels() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	channels := make([]string, 0, len(s.subscribedChannels))
	for channel := range s.subscribedChannels {
		channels = append(channels, channel)
	}
	clear(s.subscribedChannels)
	return channels
}

// DrainTransaction returns the queued commands and exits the transaction state.
func (s *ClientState) DrainTransaction() []QueuedCommand {
	s.mu.Lock()
	defer s.mu.Unlock()

	queued := s.TxQueue
	s.InTransaction = false
	s.TxDirty = false
	s.TxQueue = nil
	return queued
}

// ResetTransaction clears queued transaction state.
func (s *ClientState) ResetTransaction() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.InTransaction = false
	s.TxFailed = false
	s.TxDirty = false
	s.TxQueue = nil
}
