package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maltemindedal/stash/internal/aof"
	"github.com/maltemindedal/stash/internal/config"
	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/rdb"
	"github.com/maltemindedal/stash/internal/storage"
)

type executor interface {
	ExecuteDetailed(context.Context, protocol.Value) (ExecuteResult, error)
}

// sequencedExecutor is an executor that can order requests against one another.
// Its results carry a Release function that the caller calls once it has applied
// the result's durability and propagation frames.
type sequencedExecutor interface {
	ExecuteSequenced(context.Context, protocol.Value) (ExecuteResult, error)
}

type writeOrderingSetter interface {
	SetWriteOrdering(func() bool)
}

// backgroundWriteSequencer orders work the server does on its own (the expiry
// sweep, the snapshot of an AOF rewrite) against client requests. The returned
// function is called when that work's frames have been applied.
type backgroundWriteSequencer interface {
	BeginBackgroundWrite() (release func())
}

type watchRegistryProvider interface {
	WatchRegistry() *WatchRegistry
}

type pubSubRegistryProvider interface {
	PubSubRegistry() *PubSubRegistry
}

type replicationStateSetter interface {
	SetReplicationState(*ReplicationState)
}

type replicaRegistrySetter interface {
	SetReplicaRegistry(*ReplicaRegistry)
}

type authConfigSetter interface {
	SetRequirePass(string)
}

type aofRewriteTriggerSetter interface {
	SetAOFRewriteTrigger(func(context.Context) error)
}

type slowlogConfigSetter interface {
	SetSlowlogConfig(*SlowlogRegistry, time.Duration)
}

type monitorRegistrySetter interface {
	SetMonitorRegistry(*MonitorRegistry)
}

type serverStatsProviderSetter interface {
	SetServerStatsProvider(func() Stats)
}

type temporaryError interface {
	error
	Temporary() bool
}

// Server owns the TCP listener, active clients, and command execution pipeline.
type Server struct {
	cfg               config.Config
	logger            *slog.Logger
	store             *storage.Store
	executor          executor
	registry          *ClientRegistry
	replicaPeers      *ReplicaRegistry
	watchRegistry     *WatchRegistry
	pubSubRegistry    *PubSubRegistry
	monitorRegistry   *MonitorRegistry
	slowlogRegistry   *SlowlogRegistry
	replication       *ReplicationState
	aofWriter         *aof.Writer
	runtimeCtx        context.Context
	commandsProcessed atomic.Int64

	clientStates   map[uint64]*ClientState
	clientStatesMu sync.RWMutex

	upstreamConn   net.Conn
	upstreamConnMu sync.Mutex

	listener     net.Listener
	listenerMu   sync.RWMutex
	shutdownOnce sync.Once
	handlerWG    sync.WaitGroup
}

// New constructs a server ready to listen for TCP clients.
func New(cfg config.Config, logger *slog.Logger, store *storage.Store, executor executor) *Server {
	srv := &Server{
		cfg:             cfg,
		logger:          logger,
		store:           store,
		executor:        executor,
		registry:        NewClientRegistry(),
		replicaPeers:    NewReplicaRegistry(),
		monitorRegistry: NewMonitorRegistry(),
		slowlogRegistry: NewSlowlogRegistry(),
		replication:     newReplicationState(),
		clientStates:    make(map[uint64]*ClientState),
	}
	srv.replicaPeers.SetFeedErrorHandler(srv.dropReplica)
	if store != nil {
		store.SetLogger(logger)
		store.SetExpirationListener(srv.recordExpiredKeys)
		if sequencer, ok := executor.(backgroundWriteSequencer); ok {
			store.SetEvictionGuard(sequencer.BeginBackgroundWrite)
		}
	}
	if provider, ok := executor.(watchRegistryProvider); ok {
		srv.watchRegistry = provider.WatchRegistry()
	}
	if provider, ok := executor.(pubSubRegistryProvider); ok {
		srv.pubSubRegistry = provider.PubSubRegistry()
	}
	if setter, ok := executor.(replicationStateSetter); ok {
		setter.SetReplicationState(srv.replication)
	}
	if setter, ok := executor.(replicaRegistrySetter); ok {
		setter.SetReplicaRegistry(srv.replicaPeers)
	}
	if setter, ok := executor.(writeOrderingSetter); ok {
		setter.SetWriteOrdering(srv.recordsWrites)
	}
	if setter, ok := executor.(authConfigSetter); ok {
		setter.SetRequirePass(cfg.RequirePass)
	}
	if setter, ok := executor.(aofRewriteTriggerSetter); ok {
		setter.SetAOFRewriteTrigger(srv.beginAOFRewrite)
	}
	if setter, ok := executor.(slowlogConfigSetter); ok {
		setter.SetSlowlogConfig(srv.slowlogRegistry, cfg.SlowlogLogSlowerThan)
	}
	if setter, ok := executor.(monitorRegistrySetter); ok {
		setter.SetMonitorRegistry(srv.monitorRegistry)
	}
	if setter, ok := executor.(serverStatsProviderSetter); ok {
		setter.SetServerStatsProvider(srv.ServerStats)
	}

	return srv
}

// ListenAndServe starts the TCP listener and blocks until shutdown.
func (s *Server) ListenAndServe(ctx context.Context) error {
	s.runtimeCtx = ctx

	if s.cfg.IsReplica() {
		if _, err := s.cfg.ReplicaAddress(); err != nil {
			return fmt.Errorf("server: validate replica configuration: %w", err)
		}
	}
	if err := s.checkBindSafety(); err != nil {
		return err
	}
	if err := s.initializePersistence(ctx); err != nil {
		return err
	}
	if s.store != nil {
		s.store.ConfigureMaxMemory(s.cfg.MaxMemory, s.cfg.EvictionSampleSize)
		if evicted, err := s.store.EnforceMaxMemory(); err != nil {
			return fmt.Errorf("server: enforce maxmemory: %w", err)
		} else if len(evicted) > 0 {
			s.logger.Info("applied startup maxmemory eviction", "evicted_keys", len(evicted), "used_memory", s.store.UsedMemory(), "maxmemory", s.cfg.MaxMemory)
		}
	}

	listener, err := net.Listen("tcp", s.cfg.Address())
	if err != nil {
		if closeErr := s.closeAOFWriter(); closeErr != nil {
			s.logger.Warn("failed to close AOF writer after listen failure", "error", closeErr)
		}
		return fmt.Errorf("server: listen on %s: %w", s.cfg.Address(), err)
	}

	s.setListener(listener)
	defer s.shutdown()

	s.logger.Info("Stash listening", "address", listener.Addr().String())
	if s.cfg.RequirePass == "" && listensBeyondLoopback(listener.Addr()) {
		s.logger.Warn(
			"accepting connections from the network with no password; anyone who can reach this address has full access. Set --requirepass or bind a loopback address",
			"address", listener.Addr().String(),
		)
	}
	// The eviction loop runs on its own context so it can be stopped before the
	// durability teardown below, on the path where serve fails without ctx ever
	// being cancelled as much as on the ordinary one.
	evictionCtx, stopEviction := context.WithCancel(ctx)
	defer stopEviction()
	evictionDone := s.store.StartEviction(evictionCtx, s.cfg.EvictionInterval, s.cfg.EvictionSampleSize)
	if s.cfg.IsReplica() {
		s.handlerWG.Add(1)
		go s.startReplicaLink(ctx, listener.Addr().String())
	}

	stopShutdown := context.AfterFunc(ctx, s.shutdown)
	defer stopShutdown()

	serveErr := s.serve(ctx, listener)
	if serveErr != nil {
		// serve failed without ctx cancellation, so client connections may still
		// be open; force the shutdown path (idempotent) to close the listener and
		// connections so the handlers below unblock before we wait on them.
		s.shutdown()
	}

	// Durability teardown must run regardless of how serve exited: closeAOFWriter
	// stops the everysec fsync goroutine and flushes buffered commands, and
	// persistSnapshot writes the shutdown RDB. Skipping them on the error path
	// would leak the goroutine and silently drop unflushed writes.
	s.handlerWG.Wait()
	// The eviction loop reports expired keys into the same AOF writer and
	// replica connections, so it has to be finished before they are torn down.
	stopEviction()
	<-evictionDone

	var errs []error
	if serveErr != nil {
		errs = append(errs, serveErr)
	}
	if err := s.closeAOFWriter(); err != nil {
		errs = append(errs, fmt.Errorf("server: close AOF writer: %w", err))
	}
	if err := s.persistSnapshot(); err != nil {
		errs = append(errs, fmt.Errorf("server: persist shutdown snapshot: %w", err))
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	s.logger.Info("Stash stopped")
	return nil
}

// errEventLoopUnsupported reports that the current platform has no OS I/O
// multiplexing backend (epoll on Linux, kqueue on macOS).
var errEventLoopUnsupported = errors.New("server: event loop networking is not supported on this platform")

type inlineExecutionContextKey struct{}

// WithInlineExecution marks ctx as executing commands inline on a shared
// event-loop goroutine, where a blocked command stalls every connection.
func WithInlineExecution(ctx context.Context) context.Context {
	return context.WithValue(ctx, inlineExecutionContextKey{}, true)
}

// IsInlineExecution reports whether commands on ctx run inline on a shared
// event-loop goroutine and therefore must fail instead of blocking.
func IsInlineExecution(ctx context.Context) bool {
	inline, _ := ctx.Value(inlineExecutionContextKey{}).(bool)
	return inline
}

// serve accepts and serves client connections until shutdown. With event-loop
// mode enabled it dispatches sockets through OS readiness notifications where
// supported (epoll on Linux, kqueue on macOS) and falls back to one goroutine
// per connection elsewhere.
func (s *Server) serve(ctx context.Context, listener net.Listener) error {
	if s.cfg.EventLoop {
		err := s.serveEventLoop(ctx, listener)
		if !errors.Is(err, errEventLoopUnsupported) {
			return err
		}
		s.logger.Warn(
			"event loop networking is not supported on this platform; falling back to one goroutine per connection",
			"goos", runtime.GOOS,
		)
	}

	return s.serveGoroutinePerConnection(ctx, listener)
}

// acceptBackoffDelay is how long the goroutine-per-connection accept loop pauses
// after a temporary accept error (e.g. EMFILE/ENFILE fd exhaustion) before
// retrying, so the condition backs off instead of spinning the CPU and flooding
// the logs. It mirrors the event-loop path's pause-on-exhaustion behavior.
const acceptBackoffDelay = 20 * time.Millisecond

// refuseWriteTimeout bounds the best-effort error write to a connection refused
// for exceeding maxclients, so a slow peer cannot stall the accept loop.
const refuseWriteTimeout = 200 * time.Millisecond

// serveGoroutinePerConnection runs the blocking accept loop that spawns one
// handler goroutine per client connection.
func (s *Server) serveGoroutinePerConnection(ctx context.Context, listener net.Listener) error {
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}

			var temporary temporaryError
			if errors.As(err, &temporary) && temporary.Temporary() {
				s.logger.Warn("temporary accept error; backing off", "error", err, "delay", acceptBackoffDelay)
				if !sleepWithContext(ctx, acceptBackoffDelay) {
					return nil
				}
				continue
			}

			return fmt.Errorf("server: accept connection: %w", err)
		}

		if s.overConnectionLimit() {
			s.refuseConnection(conn)
			continue
		}

		clientID, _ := s.registerClient(conn)
		s.handlerWG.Add(1)
		go s.handleConnection(ctx, clientID, conn)
	}
}

// overConnectionLimit reports whether the active connection count has reached
// the configured maxclients cap. A zero cap disables the limit. The check is a
// soft bound: a burst may momentarily exceed it within the accept-to-register
// window, which is harmless.
func (s *Server) overConnectionLimit() bool {
	return s.cfg.MaxClients > 0 && s.registry.Count() >= s.cfg.MaxClients
}

// refuseConnection rejects a connection that would exceed maxclients, sending a
// best-effort Redis-style error before closing it.
func (s *Server) refuseConnection(conn net.Conn) {
	s.logger.Warn("connection limit reached; refusing connection", "max_clients", s.cfg.MaxClients)
	_ = conn.SetWriteDeadline(time.Now().Add(refuseWriteTimeout))
	_ = protocol.WriteValue(conn, protocol.ErrorValue{Message: "ERR max number of clients reached"})
	if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		s.logger.Debug("failed to close refused connection", "error", err)
	}
}

// sleepWithContext sleeps for d, returning false if ctx is canceled first.
func sleepWithContext(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// registerClient adds a connection to the client registry and creates its
// connection-scoped state. Both networking modes use it.
func (s *Server) registerClient(conn ClientConn) (uint64, *ClientState) {
	clientID := s.registry.Add(conn)
	return clientID, s.createClientState(clientID)
}

// teardownClient releases every per-client registration both networking modes
// maintain: replica-peer membership, the client registry entry, and the
// connection-scoped client state (which detaches watch, pub/sub, and monitor
// registrations).
func (s *Server) teardownClient(clientID uint64, logger *slog.Logger) {
	if peer := s.replicaPeers.Remove(clientID); peer != nil {
		logger.Info("replica disconnected", "replica_id", clientID, "listening_port", peer.ListeningPort)
	}
	s.registry.Remove(clientID)
	s.removeClientState(clientID)
	logger.Debug("client disconnected")
}

// Addr returns the bound listener address once the server has started.
func (s *Server) Addr() string {
	s.listenerMu.RLock()
	defer s.listenerMu.RUnlock()

	if s.listener == nil {
		return ""
	}

	return s.listener.Addr().String()
}

// checkBindSafety refuses to start a server that would accept connections from
// other machines with no password, unless --allow-open-bind says that is
// intended. It runs before persistence is opened so a refused start has no side
// effects. The address is resolved the way net.Listen will resolve it; if it
// cannot be, the listen that follows reports the real error.
func (s *Server) checkBindSafety() error {
	if s.cfg.RequirePass != "" || s.cfg.AllowOpenBind {
		return nil
	}

	addr, err := net.ResolveTCPAddr("tcp", s.cfg.Address())
	if err != nil || !listensBeyondLoopback(addr) {
		return nil
	}

	return fmt.Errorf("server: refusing to listen on %s with no password: anyone who can reach it would have full access. Set --requirepass, bind a loopback address, or pass --allow-open-bind to serve without a password anyway", s.cfg.Address())
}

// listensBeyondLoopback reports whether a listener bound to addr accepts
// connections from other machines: any address that is not a loopback one,
// including the unspecified addresses 0.0.0.0 and ::.
func listensBeyondLoopback(addr net.Addr) bool {
	tcp, ok := addr.(*net.TCPAddr)
	return ok && !tcp.IP.IsLoopback()
}

func (s *Server) setListener(listener net.Listener) {
	s.listenerMu.Lock()
	defer s.listenerMu.Unlock()

	s.listener = listener
}

// replicaShutdownGrace is how long shutdown waits for replicas to take the
// propagated commands still queued for them.
const replicaShutdownGrace = time.Second

func (s *Server) shutdown() {
	s.shutdownOnce.Do(func() {
		s.closeUpstreamConn()
		// Commands already propagated are written to the replicas before their
		// sockets are closed, as they were when propagation wrote them directly.
		if unfinished := s.replicaPeers.StopFeeds(replicaShutdownGrace); unfinished > 0 {
			s.logger.Warn("some replicas did not receive the last propagated commands before shutdown", "replicas", unfinished)
		}

		s.listenerMu.RLock()
		listener := s.listener
		s.listenerMu.RUnlock()
		if listener != nil {
			if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				s.logger.Debug("failed to close listener during shutdown", "error", err)
			}
		}
		if err := s.registry.CloseAll(); err != nil {
			s.logger.Debug("failed to close one or more client connections during shutdown", "error", err)
		}
		s.clearClientStates()
	})
}

func (s *Server) persistSnapshot() error {
	if s == nil || s.store == nil || s.cfg.DumpPath == "" {
		return nil
	}

	entries, snapshotStats := s.store.SnapshotStrings()
	writeStats, err := rdb.SaveSnapshot(s.cfg.DumpPath, entries)
	if err != nil {
		s.logger.Error(
			"failed to save shutdown RDB snapshot",
			"path", s.cfg.DumpPath,
			"candidate_keys", len(entries),
			"error", err,
		)
		return fmt.Errorf("server: save shutdown snapshot to %q: %w", s.cfg.DumpPath, err)
	}

	if snapshotStats.SkippedUnsupportedKeys > 0 {
		s.logger.Warn(
			"shutdown snapshot skipped unsupported value types",
			"path", s.cfg.DumpPath,
			"skipped_unsupported_keys", snapshotStats.SkippedUnsupportedKeys,
			"exported_keys", snapshotStats.ExportedKeys,
		)
	}

	s.logger.Info(
		"saved shutdown RDB snapshot",
		"path", s.cfg.DumpPath,
		"written_keys", writeStats.WrittenKeys,
		"skipped_expired_keys", snapshotStats.SkippedExpiredKeys+writeStats.SkippedExpiredKeys,
	)

	return nil
}

func (s *Server) createClientState(clientID uint64) *ClientState {
	state := &ClientState{
		ID:            clientID,
		Authenticated: s.cfg.RequirePass == "",
	}
	state.SetWatchRegistry(s.watchRegistry)
	state.SetPubSubRegistry(s.pubSubRegistry)
	state.SetMonitorRegistry(s.monitorRegistry)

	s.clientStatesMu.Lock()
	defer s.clientStatesMu.Unlock()

	s.clientStates[clientID] = state
	return state
}

func (s *Server) getClientState(clientID uint64) *ClientState {
	s.clientStatesMu.RLock()
	defer s.clientStatesMu.RUnlock()

	return s.clientStates[clientID]
}

func (s *Server) disconnectClientState(state *ClientState) {
	if state == nil {
		return
	}

	state.Disconnect()
}

func (s *Server) removeClientState(clientID uint64) {
	s.clientStatesMu.Lock()
	state := s.clientStates[clientID]
	delete(s.clientStates, clientID)
	s.clientStatesMu.Unlock()

	s.disconnectClientState(state)
}

func (s *Server) clearClientStates() {
	s.clientStatesMu.Lock()
	states := make([]*ClientState, 0, len(s.clientStates))
	for _, state := range s.clientStates {
		states = append(states, state)
	}
	clear(s.clientStates)
	s.clientStatesMu.Unlock()

	for _, state := range states {
		s.disconnectClientState(state)
	}
}

// ReplicaCount returns the number of replica peers connected to this server.
func (s *Server) ReplicaCount() int {
	return s.replicaPeers.Count()
}

// ServerStats returns a snapshot of server-level observability data.
func (s *Server) ServerStats() Stats {
	if s == nil {
		return Stats{}
	}

	role := "master"
	if s.cfg.IsReplica() {
		role = "slave"
	}

	replicaPeers := s.replicaPeers.Snapshot()
	replicas := make([]ReplicaInfo, 0, len(replicaPeers))
	for _, peer := range replicaPeers {
		replicas = append(replicas, ReplicaInfo{
			ID:            peer.ID,
			ListeningPort: peer.ListeningPort,
			AckOffset:     peer.acknowledgedMasterOffset(),
		})
	}

	writer := s.aofWriter
	return Stats{
		ConnectedClients:    s.registry.Count(),
		MonitoringClients:   s.monitorRegistry.Count(),
		CommandsProcessed:   s.commandsProcessed.Load(),
		Role:                role,
		MasterReplicationID: s.replication.MasterReplicationID,
		MasterOffset:        s.replication.MasterOffset(),
		ReplicaOffset:       s.replication.ReplicaOffset(),
		Replicas:            replicas,
		AOFEnabled:          writer != nil,
		AOFLastWriteOK:      writer.LastWriteOK(),
	}
}
