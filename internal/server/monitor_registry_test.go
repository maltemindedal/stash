package server

import "testing"

func TestMonitorRegistryMaintainsClientStateInvariants(t *testing.T) {
	t.Run("duplicate subscribe keeps count stable and snapshots detached", func(t *testing.T) {
		registry := NewMonitorRegistry()
		state := &ClientState{ID: 1, Authenticated: true}
		state.SetMonitorRegistry(registry)

		registry.Subscribe(state)
		registry.Subscribe(state)

		if got := registry.Count(); got != 1 {
			t.Fatalf("Count() = %d after duplicate Subscribe, want 1", got)
		}
		if !registry.HasSubscribers() {
			t.Fatal("HasSubscribers() = false after Subscribe, want true")
		}
		if !state.IsMonitoring() {
			t.Fatal("IsMonitoring() = false after Subscribe, want true")
		}

		snapshot := registry.Subscribers()
		if len(snapshot) != 1 || snapshot[0] != state {
			t.Fatalf("snapshot = %#v, want one subscribed state", snapshot)
		}

		snapshot[0] = nil
		fresh := registry.Subscribers()
		if len(fresh) != 1 || fresh[0] != state {
			t.Fatalf("fresh snapshot = %#v, want detached original subscriber", fresh)
		}
	})

	t.Run("unsubscribe missing client keeps count stable", func(t *testing.T) {
		registry := NewMonitorRegistry()
		state := &ClientState{ID: 2, Authenticated: true}
		other := &ClientState{ID: 3, Authenticated: true}

		registry.Subscribe(state)
		registry.Unsubscribe(other)

		if got := registry.Count(); got != 1 {
			t.Fatalf("Count() = %d after missing Unsubscribe, want 1", got)
		}
		if !registry.HasSubscribers() {
			t.Fatal("HasSubscribers() = false after missing Unsubscribe, want true")
		}
	})

	t.Run("unsubscribe last client clears fast-path count", func(t *testing.T) {
		registry := NewMonitorRegistry()
		state := &ClientState{ID: 4, Authenticated: true}

		registry.Subscribe(state)
		registry.Unsubscribe(state)

		if got := registry.Count(); got != 0 {
			t.Fatalf("Count() = %d after Unsubscribe, want 0", got)
		}
		if registry.HasSubscribers() {
			t.Fatal("HasSubscribers() = true after last Unsubscribe, want false")
		}
		if state.IsMonitoring() {
			t.Fatal("IsMonitoring() = true after Unsubscribe, want false")
		}
	})
}
