package server

import (
	"strconv"
	"sync"
)

// PubSubRegistry tracks active exact-channel subscriptions across all client
// states.
//
// A plain map plus RWMutex is used here because registry mutations must preserve
// a second invariant on ClientState.subscribedChannels. When both structures are
// touched, registry.mu is acquired before ClientState.mu. Publish appends to
// each subscriber's Output under the read lock, which never waits on a socket,
// so a message and a change of subscription reach a connection in one order.
type PubSubRegistry struct {
	mu          sync.RWMutex
	subscribers map[string]map[uint64]*ClientState
}

// NewPubSubRegistry constructs an empty pub/sub registry.
func NewPubSubRegistry() *PubSubRegistry {
	return &PubSubRegistry{subscribers: make(map[string]map[uint64]*ClientState)}
}

// Subscribe records that state is listening on channel.
func (r *PubSubRegistry) Subscribe(state *ClientState, channel string) {
	if r == nil || state == nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	subscribers := r.subscribers[channel]
	if subscribers == nil {
		subscribers = make(map[uint64]*ClientState)
		r.subscribers[channel] = subscribers
	}

	// Under the lock, so that every message Publish queues from here on waits
	// behind the acknowledgement this request replies with.
	state.Output().HoldPushes()
	subscribers[state.ID] = state
	state.addSubscribedChannel(channel)
}

// Unsubscribe removes state from channel.
func (r *PubSubRegistry) Unsubscribe(state *ClientState, channel string) {
	if r == nil || state == nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	subscribers := r.subscribers[channel]
	if subscribers != nil {
		delete(subscribers, state.ID)
		if len(subscribers) == 0 {
			delete(r.subscribers, channel)
		}
	}

	state.removeSubscribedChannel(channel)
}

// UnsubscribeAll removes every active pub/sub subscription for state.
func (r *PubSubRegistry) UnsubscribeAll(state *ClientState) {
	if r == nil || state == nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	channels := state.drainSubscribedChannels()
	for _, channel := range channels {
		subscribers := r.subscribers[channel]
		if subscribers == nil {
			continue
		}

		delete(subscribers, state.ID)
		if len(subscribers) == 0 {
			delete(r.subscribers, channel)
		}
	}
}

// Subscribers returns a snapshot of the clients currently subscribed to channel.
func (r *PubSubRegistry) Subscribers(channel string) []*ClientState {
	if r == nil {
		return nil
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	subscribers := make([]*ClientState, 0, len(r.subscribers[channel]))
	for _, state := range r.subscribers[channel] {
		subscribers = append(subscribers, state)
	}
	return subscribers
}

// publishFrames pools the buffers Publish encodes a message into, so that a
// PUBLISH allocates nothing for it.
var publishFrames = sync.Pool{New: func() any { return new([]byte) }}

// Publish queues message as a Pushed message for every connection subscribed to
// channel and returns how many it was queued for, whether or not they are
// reading. It never waits on a socket, so it may run under the sequencer's
// locks. A subscriber that the message takes past its Push limit, or that is
// closed to keep the Push budget, is closed and not counted.
func (r *PubSubRegistry) Publish(channel, message []byte) int {
	if r == nil {
		return 0
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	subscribers := r.subscribers[string(channel)]
	if len(subscribers) == 0 {
		return 0
	}

	pooled := publishFrames.Get().(*[]byte)
	defer publishFrames.Put(pooled)
	frame := appendPubSubMessage((*pooled)[:0], channel, message)
	*pooled = frame

	queued := 0
	for _, state := range subscribers {
		if state.Output().Push(frame) {
			queued++
		}
	}
	return queued
}

// appendPubSubMessage appends the RESP2 frame of a pub/sub message: the array
// "message", channel, payload.
func appendPubSubMessage(dst, channel, message []byte) []byte {
	dst = append(dst, "*3\r\n$7\r\nmessage\r\n"...)
	for _, part := range [2][]byte{channel, message} {
		dst = append(dst, '$')
		dst = strconv.AppendInt(dst, int64(len(part)), 10)
		dst = append(dst, '\r', '\n')
		dst = append(dst, part...)
		dst = append(dst, '\r', '\n')
	}
	return dst
}
