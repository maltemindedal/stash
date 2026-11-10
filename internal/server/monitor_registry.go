package server

import (
	"sync"
	"sync/atomic"

	"github.com/maltemindedal/stash/internal/protocol"
)

// MonitorRegistry tracks clients currently receiving MONITOR events.
type MonitorRegistry struct {
	mu              sync.RWMutex
	subscriberCount atomic.Int64
	subscribers     map[uint64]*ClientState
}

// NewMonitorRegistry constructs an empty MONITOR subscriber registry.
func NewMonitorRegistry() *MonitorRegistry {
	return &MonitorRegistry{subscribers: make(map[uint64]*ClientState)}
}

// Subscribe registers state for MONITOR fan-out.
func (r *MonitorRegistry) Subscribe(state *ClientState) {
	if r == nil || state == nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.subscribers[state.ID]; !exists {
		r.subscriberCount.Add(1)
	}
	// Under the lock, so that every event Broadcast queues from here on waits
	// behind the reply to this MONITOR.
	state.Output().HoldPushes()
	r.subscribers[state.ID] = state
	state.setMonitoring(true)
}

// Unsubscribe removes state from MONITOR fan-out.
func (r *MonitorRegistry) Unsubscribe(state *ClientState) {
	if r == nil || state == nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.subscribers[state.ID]; exists {
		delete(r.subscribers, state.ID)
		r.subscriberCount.Add(-1)
	}
	state.setMonitoring(false)
}

// Subscribers returns a snapshot of the clients currently in MONITOR mode.
func (r *MonitorRegistry) Subscribers() []*ClientState {
	if r == nil {
		return nil
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	subscribers := make([]*ClientState, 0, len(r.subscribers))
	for _, state := range r.subscribers {
		subscribers = append(subscribers, state)
	}
	return subscribers
}

// Broadcast queues line as a Pushed message for every client in MONITOR mode.
// It never waits on a socket. A client that the event takes past its Push
// limit, or that is closed to keep the Push budget, is closed.
func (r *MonitorRegistry) Broadcast(line string) {
	if r == nil {
		return
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.subscribers) == 0 {
		return
	}
	frame, err := protocol.Encode(protocol.SimpleString{Value: line})
	if err != nil {
		return
	}
	for _, state := range r.subscribers {
		state.Output().Push(frame)
	}
}

// Count reports the number of clients currently in MONITOR mode.
func (r *MonitorRegistry) Count() int {
	if r == nil {
		return 0
	}

	return int(r.subscriberCount.Load())
}

// HasSubscribers reports whether any client is currently in MONITOR mode.
func (r *MonitorRegistry) HasSubscribers() bool {
	return r != nil && r.subscriberCount.Load() > 0
}
