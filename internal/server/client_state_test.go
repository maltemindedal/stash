package server

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"log/slog"

	"github.com/maltemindedal/stash/internal/config"
	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/storage"
)

// stubExecutor is a command executor that answers every request with +OK.
type stubExecutor struct{ unorderedBackground }

func (stubExecutor) Handle(context.Context, Call, protocol.Value) (ExecuteResult, error) {
	return SingleResponse(protocol.SimpleString{Value: "OK"}), nil
}

// unorderedBackground is the background half of a test's command executor: it
// runs the work at once, orders nothing and records nothing.
type unorderedBackground struct{}

func (unorderedBackground) RunBackgroundUnit(work func(changes storage.Changes)) error {
	work(nil)
	return nil
}

func (unorderedBackground) BeginBackgroundWrite() (release func()) { return func() {} }

// newTestServer builds a server around executor, or around stubExecutor when
// executor is nil.
func newTestServer(t testing.TB, cfg config.Config, logger *slog.Logger, store *storage.Store, executor CommandExecutor) *Server {
	t.Helper()

	if executor == nil {
		executor = stubExecutor{}
	}
	srv, err := New(cfg, logger, store, func(Services) (CommandExecutor, error) { return executor, nil })
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return srv
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

func TestClientStateAbandonLeavesEveryRegistryAndDropsTheTransaction(t *testing.T) {
	registry := NewPubSubRegistry()
	watchRegistry := NewWatchRegistry()
	state := &ClientState{ID: 5, Authenticated: true}
	state.SetPubSubRegistry(registry)
	state.SetWatchRegistry(watchRegistry)

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

	state.Abandon()
	state.Abandon()

	if got := state.SubscriptionCount(); got != 0 {
		t.Fatalf("SubscriptionCount() = %d after Abandon, want 0", got)
	}
	if len(state.SubscribedChannels()) != 0 {
		t.Fatalf("SubscribedChannels() = %#v after Abandon, want empty", state.SubscribedChannels())
	}
	if state.InTransactionActive() {
		t.Fatal("InTransactionActive() = true after Abandon, want false")
	}
	if state.TransactionFailed() {
		t.Fatal("TransactionFailed() = true after Abandon, want false")
	}
	if state.TransactionDirty() {
		t.Fatal("TransactionDirty() = true after Abandon, want false")
	}
	if state.TxQueue != nil {
		t.Fatalf("TxQueue = %#v after Abandon, want nil", state.TxQueue)
	}
	if got := len(registry.Subscribers("alpha")); got != 0 {
		t.Fatalf("len(Subscribers(alpha)) = %d after Abandon, want 0", got)
	}
	if got := len(registry.Subscribers("beta")); got != 0 {
		t.Fatalf("len(Subscribers(beta)) = %d after Abandon, want 0", got)
	}
	watchRegistry.Touch("pubsub-key")
	if state.TransactionFailed() {
		t.Fatal("TransactionFailed() = true after Abandon and Touch, want false")
	}
}

func TestClientStateLifecycle(t *testing.T) {
	cfg := config.Default()
	cfg.RequirePass = "secret"

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := newTestServer(t, cfg, logger, storage.NewStore(), stubExecutor{})

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
	srv := newTestServer(t, config.Default(), logger, storage.NewStore(), nil)
	registry := srv.watchRegistry

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
	srv := newTestServer(t, config.Default(), logger, storage.NewStore(), nil)
	registry := srv.watchRegistry

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
