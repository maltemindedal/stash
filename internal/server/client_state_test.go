package server

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"log/slog"

	"github.com/maltemindedal/stash/internal/config"
	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/storage"
)

type stubExecutor struct{}

func (stubExecutor) Execute() (protocol.Value, error) {
	return protocol.SimpleString{Value: "OK"}, nil
}

func (stubExecutor) ExecuteDetailed(context.Context, protocol.Value) (ExecuteResult, error) {
	return SingleResponse(protocol.SimpleString{Value: "OK"}), nil
}

type stubWatchExecutor struct {
	registry *WatchRegistry
}

func (s stubWatchExecutor) Execute() (protocol.Value, error) {
	return protocol.SimpleString{Value: "OK"}, nil
}

func (s stubWatchExecutor) ExecuteDetailed(context.Context, protocol.Value) (ExecuteResult, error) {
	return SingleResponse(protocol.SimpleString{Value: "OK"}), nil
}

func (s stubWatchExecutor) WatchRegistry() *WatchRegistry {
	return s.registry
}

func TestClientStateTransactionLifecycle(t *testing.T) {
	t.Run("BeginTransaction preserves watch invalidation and clears stale queue state", func(t *testing.T) {
		state := &ClientState{ID: 1, Authenticated: true}
		state.MarkTransactionFailed()
		state.MarkTransactionDirty()
		state.TxQueue = []QueuedCommand{{Name: "SET", Args: [][]byte{[]byte("name"), []byte("Stash")}}}

		if ok := state.BeginTransaction(); !ok {
			t.Fatal("BeginTransaction() = false, want true")
		}
		if !state.InTransactionActive() {
			t.Fatal("InTransactionActive() = false, want true")
		}
		if !state.TransactionFailed() {
			t.Fatal("TransactionFailed() = false, want true")
		}
		if state.TransactionDirty() {
			t.Fatal("TransactionDirty() = true, want false")
		}
		if state.TxQueue != nil {
			t.Fatalf("TxQueue = %#v, want nil", state.TxQueue)
		}
		if ok := state.BeginTransaction(); ok {
			t.Fatal("second BeginTransaction() = true, want false")
		}
	})

	t.Run("DrainTransaction returns queued commands and exits transaction mode", func(t *testing.T) {
		state := &ClientState{ID: 2, Authenticated: true}
		if ok := state.BeginTransaction(); !ok {
			t.Fatal("BeginTransaction() = false, want true")
		}
		state.EnqueueCommand("SET", [][]byte{[]byte("name"), []byte("Stash")})
		state.MarkTransactionDirty()

		queued := state.DrainTransaction()
		if len(queued) != 1 {
			t.Fatalf("len(queued) = %d, want 1", len(queued))
		}
		if queued[0].Name != "SET" {
			t.Fatalf("queued[0].Name = %q, want %q", queued[0].Name, "SET")
		}
		if state.InTransactionActive() {
			t.Fatal("InTransactionActive() = true, want false")
		}
		if state.TransactionDirty() {
			t.Fatal("TransactionDirty() = true, want false")
		}
		if state.TxQueue != nil {
			t.Fatalf("TxQueue = %#v after drain, want nil", state.TxQueue)
		}
	})

	t.Run("ResetTransaction clears flags and queue idempotently", func(t *testing.T) {
		state := &ClientState{ID: 3, Authenticated: true}
		if ok := state.BeginTransaction(); !ok {
			t.Fatal("BeginTransaction() = false, want true")
		}
		state.EnqueueCommand("SET", [][]byte{[]byte("name"), []byte("Stash")})
		state.MarkTransactionDirty()
		state.MarkTransactionFailed()

		state.ResetTransaction()
		state.ResetTransaction()

		if state.InTransactionActive() {
			t.Fatal("InTransactionActive() = true, want false")
		}
		if state.TransactionFailed() {
			t.Fatal("TransactionFailed() = true, want false")
		}
		if state.TransactionDirty() {
			t.Fatal("TransactionDirty() = true, want false")
		}
		if state.TxQueue != nil {
			t.Fatalf("TxQueue = %#v after reset, want nil", state.TxQueue)
		}
	})

	t.Run("UnwatchAll clears watched keys and transaction failure idempotently", func(t *testing.T) {
		registry := NewWatchRegistry()
		state := &ClientState{ID: 4, Authenticated: true}
		state.SetWatchRegistry(registry)
		state.WatchKeys("alpha", "beta")
		registry.Touch("alpha")

		if !state.TransactionFailed() {
			t.Fatal("TransactionFailed() = false after touch, want true")
		}
		if got := len(state.watchedKeys); got != 2 {
			t.Fatalf("len(watchedKeys) = %d, want 2", got)
		}

		state.UnwatchAll()
		state.UnwatchAll()

		if state.TransactionFailed() {
			t.Fatal("TransactionFailed() = true after UnwatchAll, want false")
		}
		if got := len(state.watchedKeys); got != 0 {
			t.Fatalf("len(watchedKeys) = %d after UnwatchAll, want 0", got)
		}

		registry.Touch("alpha", "beta")
		if state.TransactionFailed() {
			t.Fatal("TransactionFailed() = true after registry.Touch post-cleanup, want false")
		}
	})
}

func TestClientStateDisconnectClearsWriterAndPubSubState(t *testing.T) {
	registry := NewPubSubRegistry()
	watchRegistry := NewWatchRegistry()
	state := &ClientState{ID: 5, Authenticated: true}
	state.SetPubSubRegistry(registry)
	state.SetWatchRegistry(watchRegistry)

	var outbound bytes.Buffer
	state.BindResponseWriter(bufio.NewWriter(&outbound))
	state.SubscribeChannel("alpha")
	state.SubscribeChannel("beta")
	state.WatchKeys("pubsub-key")
	if ok := state.BeginTransaction(); !ok {
		t.Fatal("BeginTransaction() = false, want true")
	}
	state.EnqueueCommand("SET", [][]byte{[]byte("pubsub-key"), []byte("1")})
	state.MarkTransactionDirty()
	state.MarkTransactionFailed()

	if got := len(registry.Subscribers("alpha")); got != 1 {
		t.Fatalf("len(Subscribers(alpha)) = %d, want 1", got)
	}
	if got := len(registry.Subscribers("beta")); got != 1 {
		t.Fatalf("len(Subscribers(beta)) = %d, want 1", got)
	}

	state.Disconnect()
	state.Disconnect()

	if state.HasActiveResponseWriter() {
		t.Fatal("HasActiveResponseWriter() = true after Disconnect, want false")
	}
	if got := state.SubscriptionCount(); got != 0 {
		t.Fatalf("SubscriptionCount() = %d after Disconnect, want 0", got)
	}
	if len(state.SubscribedChannels()) != 0 {
		t.Fatalf("SubscribedChannels() = %#v after Disconnect, want empty", state.SubscribedChannels())
	}
	if state.InTransactionActive() {
		t.Fatal("InTransactionActive() = true after Disconnect, want false")
	}
	if state.TransactionFailed() {
		t.Fatal("TransactionFailed() = true after Disconnect, want false")
	}
	if state.TransactionDirty() {
		t.Fatal("TransactionDirty() = true after Disconnect, want false")
	}
	if state.TxQueue != nil {
		t.Fatalf("TxQueue = %#v after Disconnect, want nil", state.TxQueue)
	}
	if got := len(registry.Subscribers("alpha")); got != 0 {
		t.Fatalf("len(Subscribers(alpha)) = %d after Disconnect, want 0", got)
	}
	if got := len(registry.Subscribers("beta")); got != 0 {
		t.Fatalf("len(Subscribers(beta)) = %d after Disconnect, want 0", got)
	}
	if err := state.WriteEncoded([]byte("+OK\r\n")); err == nil {
		t.Fatal("WriteEncoded() error = nil after Disconnect, want error")
	}

	watchRegistry.Touch("pubsub-key")
	if state.TransactionFailed() {
		t.Fatal("TransactionFailed() = true after Disconnect and Touch, want false")
	}
}

func TestClientStateWriteEncodedWithDeadlineTimesOut(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer func() {
		if err := serverConn.Close(); err != nil {
			t.Errorf("serverConn.Close() error = %v", err)
		}
	}()
	defer func() {
		if err := clientConn.Close(); err != nil {
			t.Errorf("clientConn.Close() error = %v", err)
		}
	}()

	state := &ClientState{ID: 6, Authenticated: true}
	state.BindResponseWriter(bufio.NewWriter(serverConn))
	state.BindResponseConn(serverConn)

	if err := state.WriteEncodedWithDeadline([]byte("+OK\r\n"), 5*time.Millisecond); err == nil {
		t.Fatal("WriteEncodedWithDeadline() error = nil, want timeout error")
	}
}

func TestClientStateLifecycle(t *testing.T) {
	cfg := config.Default()
	cfg.RequirePass = "secret"

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := New(cfg, logger, storage.NewStore(), stubExecutor{})

	first := srv.createClientState(1)
	second := srv.createClientState(2)
	if first == nil {
		t.Fatal("createClientState(1) returned nil state")
		return
	}
	if second == nil {
		t.Fatal("createClientState(2) returned nil state")
		return
	}
	if first == second {
		t.Fatal("createClientState() returned the same state for distinct clients")
	}
	if first.Authenticated {
		t.Fatal("first.Authenticated = true, want false when requirepass is configured")
	}
	if second.Authenticated {
		t.Fatal("second.Authenticated = true, want false when requirepass is configured")
	}

	if got := srv.getClientState(1); got != first {
		t.Fatalf("getClientState(1) = %p, want %p", got, first)
	}

	ctx := WithClientState(context.Background(), first)
	restored, ok := ClientStateFromContext(ctx)
	if !ok {
		t.Fatal("ClientStateFromContext() ok = false, want true")
	}
	if restored != first {
		t.Fatalf("ClientStateFromContext() = %p, want %p", restored, first)
	}

	srv.removeClientState(1)
	if got := srv.getClientState(1); got != nil {
		t.Fatalf("getClientState(1) after remove = %p, want nil", got)
	}
	if got := srv.getClientState(2); got != second {
		t.Fatalf("getClientState(2) = %p, want %p", got, second)
	}

	srv.clearClientStates()
	if got := srv.getClientState(2); got != nil {
		t.Fatalf("getClientState(2) after clear = %p, want nil", got)
	}
}

func TestServerClientStateCleanupUnwatchesKeys(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	registry := NewWatchRegistry()
	srv := New(config.Default(), logger, storage.NewStore(), stubWatchExecutor{registry: registry})

	state := srv.createClientState(1)
	state.WatchKeys("alpha")
	if ok := state.BeginTransaction(); !ok {
		t.Fatal("BeginTransaction() = false, want true")
	}
	state.EnqueueCommand("SET", [][]byte{[]byte("alpha"), []byte("1")})
	state.MarkTransactionDirty()
	state.MarkTransactionFailed()

	srv.removeClientState(1)
	registry.Touch("alpha")

	if state.TransactionFailed() {
		t.Fatal("TransactionFailed() = true, want false after cleanup removed all watchers")
	}
	if state.InTransactionActive() {
		t.Fatal("InTransactionActive() = true after cleanup, want false")
	}
	if state.TransactionDirty() {
		t.Fatal("TransactionDirty() = true after cleanup, want false")
	}
	if state.TxQueue != nil {
		t.Fatalf("TxQueue = %#v after cleanup, want nil", state.TxQueue)
	}

	other := srv.createClientState(2)
	other.WatchKeys("beta")
	if ok := other.BeginTransaction(); !ok {
		t.Fatal("BeginTransaction() for second state = false, want true")
	}
	other.EnqueueCommand("SET", [][]byte{[]byte("beta"), []byte("2")})
	other.MarkTransactionFailed()
	srv.clearClientStates()
	registry.Touch("beta")

	if other.TransactionFailed() {
		t.Fatal("TransactionFailed() for cleared state = true, want false after clearClientStates cleanup")
	}
	if other.InTransactionActive() {
		t.Fatal("InTransactionActive() for cleared state = true, want false")
	}
	if other.TxQueue != nil {
		t.Fatalf("TxQueue for cleared state = %#v, want nil", other.TxQueue)
	}
}

func TestHandleConnectionDisconnectCleansTransactionState(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	registry := NewWatchRegistry()
	srv := New(config.Default(), logger, storage.NewStore(), stubWatchExecutor{registry: registry})

	serverConn, clientConn := net.Pipe()
	defer func() { _ = clientConn.Close() }()

	clientID := srv.registry.Add(serverConn)
	state := srv.createClientState(clientID)
	state.WatchKeys("alpha")
	if ok := state.BeginTransaction(); !ok {
		t.Fatal("BeginTransaction() = false, want true")
	}
	state.EnqueueCommand("SET", [][]byte{[]byte("alpha"), []byte("1")})
	state.MarkTransactionDirty()
	state.MarkTransactionFailed()

	srv.handlerWG.Add(1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.handleConnection(context.Background(), clientID, serverConn)
	}()

	if err := clientConn.Close(); err != nil {
		t.Fatalf("Close() client side error = %v", err)
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handleConnection() did not exit after disconnect")
	}

	if got := srv.getClientState(clientID); got != nil {
		t.Fatalf("getClientState(%d) after disconnect = %p, want nil", clientID, got)
	}
	if state.InTransactionActive() {
		t.Fatal("InTransactionActive() = true after disconnect cleanup, want false")
	}
	if state.TransactionFailed() {
		t.Fatal("TransactionFailed() = true after disconnect cleanup, want false")
	}
	if state.TransactionDirty() {
		t.Fatal("TransactionDirty() = true after disconnect cleanup, want false")
	}
	if state.TxQueue != nil {
		t.Fatalf("TxQueue after disconnect cleanup = %#v, want nil", state.TxQueue)
	}

	registry.Touch("alpha")
	if state.TransactionFailed() {
		t.Fatal("TransactionFailed() = true after registry.Touch post-disconnect, want false")
	}
}

func TestHandleConnectionClosesConnectionBeforeDisconnect(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := New(config.Default(), logger, storage.NewStore(), stubExecutor{})

	conn := newBlockingConn()
	clientID := srv.registry.Add(conn)
	state := srv.createClientState(clientID)

	srv.handlerWG.Add(1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.handleConnection(context.Background(), clientID, conn)
	}()

	waitForCondition(t, time.Second, func() bool {
		return state.HasActiveResponseWriter()
	}, "handleConnection to bind the client response writer")

	writeErrCh := make(chan error, 1)
	go func() {
		writeErrCh <- state.WriteEncoded([]byte("+push\r\n"))
	}()

	waitForSignal(t, time.Second, conn.writeStarted, "async client write to start")
	conn.ReleaseRead()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handleConnection() did not exit; expected it to close the connection before disconnecting client state")
	}

	select {
	case err := <-writeErrCh:
		if err == nil {
			t.Fatal("WriteEncoded() error = nil, want close-induced error")
		}
	case <-time.After(time.Second):
		t.Fatal("WriteEncoded() did not unblock after handleConnection closed the connection")
	}

	if got := srv.getClientState(clientID); got != nil {
		t.Fatalf("getClientState(%d) after close-before-disconnect cleanup = %p, want nil", clientID, got)
	}
	if state.HasActiveResponseWriter() {
		t.Fatal("HasActiveResponseWriter() = true after handleConnection cleanup, want false")
	}
	if got := srv.registry.Count(); got != 0 {
		t.Fatalf("registry.Count() after handleConnection cleanup = %d, want 0", got)
	}
}

func TestShutdownClosesConnectionsBeforeDisconnectingClientStates(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := config.Default()
	cfg.DumpPath = ""
	srv := New(cfg, logger, storage.NewStore(), stubExecutor{})

	conn := newBlockingConn()
	clientID := srv.registry.Add(conn)
	state := srv.createClientState(clientID)
	state.BindResponseWriter(bufio.NewWriter(conn))

	writeErrCh := make(chan error, 1)
	go func() {
		writeErrCh <- state.WriteEncoded([]byte("+push\r\n"))
	}()

	waitForSignal(t, time.Second, conn.writeStarted, "shutdown write to start")

	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.shutdown()
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("shutdown() did not return; expected it to close client connections before disconnecting client state")
	}

	select {
	case err := <-writeErrCh:
		if err == nil {
			t.Fatal("WriteEncoded() error = nil, want close-induced error")
		}
	case <-time.After(time.Second):
		t.Fatal("WriteEncoded() did not unblock after shutdown closed the client connection")
	}

	if got := srv.getClientState(clientID); got != nil {
		t.Fatalf("getClientState(%d) after shutdown cleanup = %p, want nil", clientID, got)
	}
	if state.HasActiveResponseWriter() {
		t.Fatal("HasActiveResponseWriter() = true after shutdown cleanup, want false")
	}
	if got := srv.registry.Count(); got != 0 {
		t.Fatalf("registry.Count() after shutdown cleanup = %d, want 0", got)
	}
}

type blockingConn struct {
	closed           chan struct{}
	readRelease      chan struct{}
	writeStarted     chan struct{}
	closeOnce        sync.Once
	readReleaseOnce  sync.Once
	writeStartedOnce sync.Once
}

func newBlockingConn() *blockingConn {
	return &blockingConn{
		closed:       make(chan struct{}),
		readRelease:  make(chan struct{}),
		writeStarted: make(chan struct{}),
	}
}

func (c *blockingConn) ReleaseRead() {
	c.readReleaseOnce.Do(func() {
		close(c.readRelease)
	})
}

func (c *blockingConn) Read(_ []byte) (int, error) {
	<-c.readRelease
	return 0, io.EOF
}

func (c *blockingConn) Write(_ []byte) (int, error) {
	c.writeStartedOnce.Do(func() {
		close(c.writeStarted)
	})
	<-c.closed
	return 0, net.ErrClosed
}

func (c *blockingConn) Close() error {
	c.closeOnce.Do(func() {
		close(c.closed)
	})
	return nil
}

func (c *blockingConn) LocalAddr() net.Addr {
	return blockingConnAddr("local")
}

func (c *blockingConn) RemoteAddr() net.Addr {
	return blockingConnAddr("remote")
}

func (c *blockingConn) SetDeadline(time.Time) error {
	return nil
}

func (c *blockingConn) SetReadDeadline(time.Time) error {
	return nil
}

func (c *blockingConn) SetWriteDeadline(time.Time) error {
	return nil
}

type blockingConnAddr string

func (a blockingConnAddr) Network() string {
	return "test"
}

func (a blockingConnAddr) String() string {
	return string(a)
}

func waitForCondition(t *testing.T, timeout time.Duration, condition func() bool, description string) {
	t.Helper()

	deadline := time.After(timeout)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	for {
		if condition() {
			return
		}

		select {
		case <-deadline:
			t.Fatalf("timed out waiting for %s", description)
		case <-ticker.C:
		}
	}
}

func waitForSignal(t *testing.T, timeout time.Duration, signal <-chan struct{}, description string) {
	t.Helper()

	select {
	case <-signal:
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for %s", description)
	}
}

// startBlockedReply starts a reply write that cannot finish, because the peer end
// of the pipe never reads, and returns once that write holds the client's
// response lock, the way a client that stopped draining its socket does.
func startBlockedReply(t *testing.T) (*ClientState, <-chan error) {
	t.Helper()

	server, peer := net.Pipe()
	t.Cleanup(func() {
		_ = server.Close()
		_ = peer.Close()
	})

	state := &ClientState{ID: 9, Authenticated: true}
	state.BindResponseWriter(bufio.NewWriter(server))
	state.BindResponseConn(server)

	done := make(chan error, 1)
	go func() {
		done <- state.WriteResponses([]protocol.Value{protocol.SimpleString{Value: "PONG"}})
	}()

	deadline := time.Now().Add(5 * time.Second)
	for state.responseMu.TryLock() {
		state.responseMu.Unlock()
		if time.Now().After(deadline) {
			t.Fatal("the reply write never took the response lock")
		}
		time.Sleep(time.Millisecond)
	}
	return state, done
}

func TestClientStatePushToBlockedReplyGivesUpAtDeadline(t *testing.T) {
	// A client whose own reply flush is stuck holds its response lock. A publisher
	// pushing to it must give up after its write timeout instead of queueing
	// behind that flush, or one client that stops reading freezes every publisher.
	state, _ := startBlockedReply(t)

	result := make(chan error, 1)
	start := time.Now()
	go func() { result <- state.WriteEncodedWithDeadline([]byte("+pushed\r\n"), 100*time.Millisecond) }()

	select {
	case err := <-result:
		if err == nil {
			t.Fatal("WriteEncodedWithDeadline() error = nil, want a timeout while the reply flush holds the lock")
		}
		if elapsed := time.Since(start); elapsed > 2*time.Second {
			t.Fatalf("WriteEncodedWithDeadline() took %v, want it bounded by its 100ms timeout", elapsed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("WriteEncodedWithDeadline() is still blocked behind the stuck reply flush")
	}
}

func TestClientStateDisconnectUnblocksStuckReplyWrite(t *testing.T) {
	// Disconnect is what publishers call after a failed push. It must not wait
	// for the stuck flush; closing the connection is what ends that flush.
	state, replyDone := startBlockedReply(t)

	disconnected := make(chan struct{})
	go func() {
		state.Disconnect()
		close(disconnected)
	}()

	select {
	case <-disconnected:
	case <-time.After(5 * time.Second):
		t.Fatal("Disconnect() is still blocked behind the stuck reply flush")
	}
	select {
	case err := <-replyDone:
		if err == nil {
			t.Fatal("WriteResponses() error = nil, want the write to fail once the client is disconnected")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the stuck reply write was never released")
	}
}

func TestTryFlushResponsesDoesNotWaitForAStuckWriter(t *testing.T) {
	// A push or replica propagation can be stuck writing to a peer that stopped
	// reading, holding the response lock. The connection handler flushes before
	// each read and must not park behind that writer: it would stop reading the
	// peer's own requests, which for a replica are its acknowledgements.
	state, _ := startBlockedReply(t)

	done := make(chan error, 2)
	go func() { done <- state.TryFlushResponses() }()
	go func() { done <- FlushClientResponses(WithClientState(context.Background(), state)) }()

	for i := 0; i < 2; i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("flush behind a stuck writer error = %v, want it to be skipped", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("a flush waited behind a writer stuck holding the response lock")
		}
	}
}

func TestQueuedResponsesGoOutOnTheNextFlush(t *testing.T) {
	var out bytes.Buffer
	state := &ClientState{ID: 4}
	state.BindResponseWriter(bufio.NewWriter(&out))

	for i := 0; i < 3; i++ {
		if err := state.QueueResponses([]protocol.Value{protocol.SimpleString{Value: "OK"}}); err != nil {
			t.Fatalf("QueueResponses() error = %v", err)
		}
	}
	if out.Len() != 0 {
		t.Fatalf("output = %q before any flush, want the replies held back", out.String())
	}
	if err := state.TryFlushResponses(); err != nil {
		t.Fatalf("TryFlushResponses() error = %v", err)
	}
	if got := out.String(); got != "+OK\r\n+OK\r\n+OK\r\n" {
		t.Fatalf("output after the flush = %q, want the three queued replies in order", got)
	}

	// Nothing queued, or nothing to write to, is not an error.
	if err := state.FlushResponses(); err != nil {
		t.Fatalf("FlushResponses() with nothing queued error = %v", err)
	}
	state.Disconnect()
	if err := state.FlushResponses(); err != nil {
		t.Fatalf("FlushResponses() after Disconnect error = %v, want success", err)
	}
	var nilState *ClientState
	if err := nilState.TryFlushResponses(); err != nil {
		t.Fatalf("TryFlushResponses() on a nil state error = %v", err)
	}
}
