package storage

import (
	"container/list"
	"sync"
)

// listWaiters queues the clients waiting for an element of a list, one queue for
// each key, longest-waiting first. Each waiter is a channel of capacity 1, and
// leaves its queue once, in one of two ways, both under mu: notifyOne takes it
// from the front and signals it in the same hold, or unsubscribe removes it
// unsignaled. So a waiter that is no longer queued when its client unsubscribes
// it was signaled exactly once, and its wake-up is either still in the channel
// or has been received.
//
// A wake-up is the client's turn at an element. A turn its client does not take
// is passed on to the next waiter (unsubscribe, and Store.PassListPushWake), so
// a client that stops waiting never strands an element that another client is
// waiting for.
//
// mu is a leaf: nothing else is locked while it is held. Callers take it with no
// shard lock held (Store.wakeNextListWaiter).
type listWaiters struct {
	mu      sync.Mutex
	waiters map[string]*waiterQueue
}

type waiterQueue struct {
	order *list.List
	index map[chan struct{}]*list.Element
}

func newListWaiters() *listWaiters {
	return &listWaiters{waiters: make(map[string]*waiterQueue)}
}

func (w *listWaiters) subscribe(key string) chan struct{} {
	w.mu.Lock()
	defer w.mu.Unlock()

	queue := w.waiters[key]
	if queue == nil {
		queue = &waiterQueue{order: list.New(), index: make(map[chan struct{}]*list.Element)}
		w.waiters[key] = queue
	}

	ch := make(chan struct{}, 1)
	queue.index[ch] = queue.order.PushBack(ch)
	return ch
}

// unsubscribe removes ch from key's queue. If ch has already left the queue
// because notifyOne signaled it, and nobody has received that wake-up, the
// client is leaving without taking its turn: unsubscribe drains the wake-up and
// signals the next waiter instead, under the same hold of mu, so no notifier can
// slip in between and find the queue as it was.
func (w *listWaiters) unsubscribe(key string, ch chan struct{}) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if queue := w.waiters[key]; queue != nil {
		if element, ok := queue.index[ch]; ok {
			queue.order.Remove(element)
			delete(queue.index, ch)
			if queue.order.Len() == 0 {
				delete(w.waiters, key)
			}
			return
		}
	}

	select {
	case <-ch:
		w.notifyOneLocked(key)
	default:
	}
}

// notifyOne signals the longest-waiting waiter for key, if there is one.
func (w *listWaiters) notifyOne(key string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.notifyOneLocked(key)
}

// notifyOneLocked takes the front waiter off key's queue and signals it. The
// caller holds mu. Signaling under mu is what lets unsubscribe tell a waiter
// still queued from one holding an unreceived wake-up, with nothing in between.
func (w *listWaiters) notifyOneLocked(key string) {
	queue := w.waiters[key]
	if queue == nil {
		return
	}
	front := queue.order.Front()
	if front == nil {
		return
	}

	ch, ok := front.Value.(chan struct{})
	queue.order.Remove(front)
	if ok {
		delete(queue.index, ch)
	}
	if queue.order.Len() == 0 {
		delete(w.waiters, key)
	}
	if !ok {
		return
	}

	// A waiter is signaled only here, as it leaves the queue, so its buffer of
	// one is empty and the send succeeds. The default case only guarantees that
	// a send under mu can never block.
	select {
	case ch <- struct{}{}:
	default:
	}
}
