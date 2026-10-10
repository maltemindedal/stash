package command

import (
	"context"
	"testing"

	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/server"
)

func TestMonitorRegistersTheClientWithTheRegistryItsClientStateHolds(t *testing.T) {
	// The server binds every Client state to its MONITOR registry, and MONITOR
	// registers the client there. Nothing else has to be wired into the executor
	// for MONITOR to work.
	executor := newTestExecutor()
	registry := server.NewMonitorRegistry()
	state := newTestClientState(executor, 1)
	state.SetMonitorRegistry(registry)
	ctx := withClient(context.Background(), state)

	value, err := executor.Execute(ctx, requestValue("MONITOR"))
	if err != nil {
		t.Fatalf("MONITOR error = %v", err)
	}
	assertValueEqual(t, value, protocol.SimpleString{Value: "OK"})
	if !state.IsMonitoring() || registry.Count() != 1 {
		t.Fatalf("after MONITOR IsMonitoring() = %v and the registry holds %d clients, want true and 1", state.IsMonitoring(), registry.Count())
	}
}
