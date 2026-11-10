package server

import (
	"bytes"
	"errors"
	"sort"
	"sync"
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

	watchRegistry *WatchRegistry
	watchedKeys   map[string]struct{}
	// pubSubRegistry and subscribedChannels mirror the same exact-channel
	// membership. Registry mutations take PubSubRegistry.mu before mutating the
	// client-local subscribedChannels set via ClientState.mu.
	pubSubRegistry     *PubSubRegistry
	subscribedChannels map[string]struct{}
	monitorRegistry    *MonitorRegistry
	// out is the connection's Client output, which the registries push to. It is
	// bound once, before the Client state is shared.
	out *Output

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

// BindOutput binds the connection's Client output. It is called once, before
// the Client state is used by a request or entered in a registry.
func (s *ClientState) BindOutput(out *Output) {
	s.out = out
}

// Output returns the connection's Client output, or nil when the Client state
// has none (a nil Client state, or one built without a connection).
func (s *ClientState) Output() *Output {
	if s == nil {
		return nil
	}
	return s.out
}

// Abandon leaves every shared registry and drops the queued transaction, when
// the connection ends. It touches registries only: the connection's Output is
// closed by whoever owns the connection.
func (s *ClientState) Abandon() {
	if s == nil {
		return
	}

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

// ErrClientDisconnected reports that a command was waiting for something and
// the client went away while it waited.
var ErrClientDisconnected = errors.New("server: client disconnected")

// Disconnected reports whether the client has closed its side of the
// connection, without reading from it (Output.PeerClosed). A command that
// blocks for a long time polls it, so it can stop waiting for a client that is
// not there.
func (s *ClientState) Disconnected() bool {
	return s.Output().PeerClosed()
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
// A nil ClientState is not a replica.
func (s *ClientState) IsReplica() bool {
	if s == nil {
		return false
	}

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
