package storage

import (
	"container/list"
	"sync"
)

// listWaiters queues the clients blocked waiting for a push to each key, the one
// that has waited longest first. A waiter's channel gets at most one wake-up,
// sent as notifyOne takes the waiter off the queue. A waiter that leaves without
// receiving its wake-up passes it on (unsubscribe), so every wake-up reaches a
// waiter that receives it, or runs out of waiters to wake.
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

// unsubscribe removes the waiter ch from key's queue.
//
// A waiter that is no longer queued has been woken (notifyOne). If its wake-up is
// still in ch, nobody has received it and nobody will, so it is passed on to the
// next waiter, which could otherwise stay blocked with an element in the list.
// It is drained and passed on under the same hold of mu that notifyOne sends
// under, so no wake-up is ever between the queue and its channel.
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

// notifyOne wakes the waiter that has waited longest for a push to key, if any.
func (w *listWaiters) notifyOne(key string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.notifyOneLocked(key)
}

// notifyOneLocked is notifyOne for a caller that holds mu.
//
// It sends the wake-up while still holding mu, so that unsubscribe sees a waiter
// either still queued or already holding (or having received) its wake-up, never
// taken off the queue with the wake-up yet to come. The send does not block: the
// channel has room for one wake-up, and this is the only send it ever gets, made
// as it leaves the queue.
func (w *listWaiters) notifyOneLocked(key string) {
	queue := w.waiters[key]
	if queue == nil || queue.order.Len() == 0 {
		return
	}

	front := queue.order.Front()
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
	select {
	case ch <- struct{}{}:
	default:
	}
}
