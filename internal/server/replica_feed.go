package server

import (
	"errors"
	"net"
	"sync"
	"time"
)

// replicaFeedLimit is how many bytes of the propagated stream may be waiting for
// one replica before it is dropped: Redis's replica output-buffer hard limit. A
// dropped replica has to synchronise again; a stalled one must not be able to
// grow the master's memory, or hold up its writers, without bound.
const replicaFeedLimit = 256 << 20

// replicaFeedWriteChunk is the most bytes handed to the socket at once, so that
// the write deadline bounds how long one stretch of the stream may stall, not the
// whole backlog.
const replicaFeedWriteChunk = 1 << 20

// replicaFeedWriteTimeout is how long one chunk may take to be accepted by the
// replica's socket before the replica is considered stalled and dropped.
const replicaFeedWriteTimeout = 30 * time.Second

// errReplicaBacklog reports that a replica has fallen further behind than
// replicaFeedLimit allows.
var errReplicaBacklog = errors.New("server: replica is too far behind the propagated stream")

// replicaFeed carries the propagated command stream to one replica. A writer that
// propagates a command only appends its bytes to the feed, which never blocks on
// the replica; a goroutine of the feed's own writes them to the socket, in the
// order they were appended and many at a time. Before, each writer wrote to every
// replica's socket itself and flushed after every command, so one stalled replica
// stopped every writer and one healthy replica cost a syscall per command.
type replicaFeed struct {
	mu      sync.Mutex
	pending []byte
	spare   []byte
	limit   int
	closed  bool

	wake chan struct{}
	stop chan struct{}
	done chan struct{}
	once sync.Once
}

func newReplicaFeed(limit int) *replicaFeed {
	return &replicaFeed{
		limit: limit,
		wake:  make(chan struct{}, 1),
		stop:  make(chan struct{}),
		done:  make(chan struct{}),
	}
}

// enqueue appends payload to the stream waiting for the replica. It never
// blocks. It fails when the feed is closed or the backlog would pass the limit.
func (f *replicaFeed) enqueue(payload []byte) error {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return net.ErrClosed
	}
	if len(f.pending)+len(payload) > f.limit {
		f.mu.Unlock()
		return errReplicaBacklog
	}
	f.pending = append(f.pending, payload...)
	f.mu.Unlock()

	select {
	case f.wake <- struct{}{}:
	default:
	}
	return nil
}

// backlog reports how many bytes are waiting to be written.
func (f *replicaFeed) backlog() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.pending)
}

// run writes the queued stream with write until the feed is closed (after one
// last drain) or a write fails, whose error it returns.
func (f *replicaFeed) run(write func([]byte) error) error {
	defer close(f.done)

	for {
		select {
		case <-f.wake:
		case <-f.stop:
			return f.flush(write)
		}
		if err := f.flush(write); err != nil {
			return err
		}
	}
}

func (f *replicaFeed) flush(write func([]byte) error) error {
	for {
		f.mu.Lock()
		if len(f.pending) == 0 {
			f.mu.Unlock()
			return nil
		}
		data := f.pending
		f.pending = f.spare
		f.spare = nil
		f.mu.Unlock()

		var err error
		for rest := data; len(rest) > 0 && err == nil; {
			n := min(len(rest), replicaFeedWriteChunk)
			err = write(rest[:n])
			rest = rest[n:]
		}

		f.mu.Lock()
		// Keep the buffer for the next batch unless a burst made it large.
		if cap(data) <= 1<<20 {
			f.spare = data[:0]
		}
		f.mu.Unlock()
		if err != nil {
			return err
		}
	}
}

// close stops the feed. New bytes are refused; the flusher writes what is already
// queued and exits, and close waits up to grace for that. It reports whether the
// flusher had exited by then.
func (f *replicaFeed) close(grace time.Duration) bool {
	f.once.Do(func() {
		f.mu.Lock()
		f.closed = true
		f.mu.Unlock()
		close(f.stop)
	})

	select {
	case <-f.done:
		return true
	case <-time.After(grace):
		return false
	}
}
