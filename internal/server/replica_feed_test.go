package server

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestReplicaFeedDeliversEverythingInOrderWithoutBlockingWriters(t *testing.T) {
	feed := newReplicaFeed(1 << 20)

	var mu sync.Mutex
	var written bytes.Buffer
	release := make(chan struct{})
	writes := 0
	done := make(chan error, 1)
	feed.start(func(chunk []byte) error {
		<-release // a slow socket: nothing is written until the test lets it
		mu.Lock()
		written.Write(chunk)
		writes++
		mu.Unlock()
		return nil
	}, func(err error) { done <- err })

	// Producers never wait for the socket, however slow it is.
	var want bytes.Buffer
	started := time.Now()
	for i := 0; i < 1000; i++ {
		payload := []byte(fmt.Sprintf("cmd-%04d;", i))
		want.Write(payload)
		if err := feed.enqueue(payload); err != nil {
			t.Fatalf("enqueue(%d) error = %v", i, err)
		}
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("1000 enqueues took %v behind a stalled socket, want them not to wait for it", elapsed)
	}

	close(release)
	if !feed.close(2 * time.Second) {
		t.Fatal("the feed did not finish writing after it was closed")
	}
	if err := <-done; err != nil {
		t.Fatalf("run() error = %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if !bytes.Equal(written.Bytes(), want.Bytes()) {
		t.Fatalf("the replica received %d bytes, want the %d queued, in order", written.Len(), want.Len())
	}
	if writes >= 1000 {
		t.Fatalf("%d socket writes for 1000 commands, want them coalesced", writes)
	}
}

func TestReplicaFeedRejectsAReplicaThatFallsTooFarBehind(t *testing.T) {
	feed := newReplicaFeed(100)

	if err := feed.enqueue(bytes.Repeat([]byte("x"), 60)); err != nil {
		t.Fatalf("enqueue within the limit error = %v", err)
	}
	if err := feed.enqueue(bytes.Repeat([]byte("x"), 60)); !errors.Is(err, errReplicaBacklog) {
		t.Fatalf("enqueue over the limit error = %v, want errReplicaBacklog", err)
	}
	if got := feed.backlog(); got != 60 {
		t.Fatalf("backlog() = %d, want the rejected payload not queued", got)
	}
}

func TestReplicaFeedStopsWhenAWriteFails(t *testing.T) {
	feed := newReplicaFeed(1 << 20)
	boom := errors.New("write boom")

	result := make(chan error, 1)
	feed.start(func([]byte) error { return boom }, func(err error) { result <- err })
	if err := feed.enqueue([]byte("x")); err != nil {
		t.Fatalf("enqueue() error = %v", err)
	}

	select {
	case err := <-result:
		if !errors.Is(err, boom) {
			t.Fatalf("run() error = %v, want the write error", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("run() did not stop after a failed write")
	}
}

func TestReplicaFeedWritesInChunksSoTheDeadlineBoundsEachStretch(t *testing.T) {
	feed := newReplicaFeed(8 << 20)
	var sizes []int
	var mu sync.Mutex
	feed.start(func(chunk []byte) error {
		mu.Lock()
		sizes = append(sizes, len(chunk))
		mu.Unlock()
		return nil
	}, nil)

	if err := feed.enqueue(bytes.Repeat([]byte("x"), 3*replicaFeedWriteChunk+10)); err != nil {
		t.Fatalf("enqueue() error = %v", err)
	}
	if !feed.close(2 * time.Second) {
		t.Fatal("the feed did not finish")
	}

	mu.Lock()
	defer mu.Unlock()
	total := 0
	for _, size := range sizes {
		if size > replicaFeedWriteChunk {
			t.Fatalf("a write of %d bytes exceeds the %d byte chunk", size, replicaFeedWriteChunk)
		}
		total += size
	}
	if total != 3*replicaFeedWriteChunk+10 {
		t.Fatalf("wrote %d bytes, want %d", total, 3*replicaFeedWriteChunk+10)
	}
}

func TestReplicaFeedRefusesWorkOnceClosed(t *testing.T) {
	feed := newReplicaFeed(100)
	feed.start(func([]byte) error { return nil }, nil)
	feed.close(time.Second)

	if err := feed.enqueue([]byte("x")); err == nil {
		t.Fatal("enqueue() after close error = nil, want an error")
	}
}

// TestAReplicaFeedHoldsWhatItQueuesUntilItStarts queues frames into a feed that
// has not started, as a replica's feed does while its full resync reply is sent,
// and checks that nothing is written until it starts and then everything is, in
// order. A feed closed before it starts never starts, and closing it does not
// wait for a flusher it does not have.
func TestAReplicaFeedHoldsWhatItQueuesUntilItStarts(t *testing.T) {
	t.Run("what was queued is written once it starts", func(t *testing.T) {
		feed := newReplicaFeed(1 << 20)
		for _, frame := range []string{"one;", "two;"} {
			if err := feed.enqueue([]byte(frame)); err != nil {
				t.Fatalf("enqueue(%q) error = %v", frame, err)
			}
		}

		var mu sync.Mutex
		var written bytes.Buffer
		if !feed.start(func(chunk []byte) error {
			mu.Lock()
			written.Write(chunk)
			mu.Unlock()
			return nil
		}, nil) {
			t.Fatal("start() = false for a new feed, want true")
		}
		if feed.start(func([]byte) error { return nil }, nil) {
			t.Fatal("a second start() = true, want the feed started once")
		}
		if err := feed.enqueue([]byte("three;")); err != nil {
			t.Fatalf("enqueue() after start error = %v", err)
		}
		if !feed.close(2 * time.Second) {
			t.Fatal("the feed did not finish writing after it was closed")
		}

		mu.Lock()
		defer mu.Unlock()
		if got, want := written.String(), "one;two;three;"; got != want {
			t.Fatalf("written = %q, want %q", got, want)
		}
	})

	t.Run("a feed closed before it starts never starts and does not hold up closing", func(t *testing.T) {
		feed := newReplicaFeed(1 << 20)
		if err := feed.enqueue([]byte("held;")); err != nil {
			t.Fatalf("enqueue() error = %v", err)
		}

		closed := make(chan bool, 1)
		go func() { closed <- feed.close(time.Minute) }()
		select {
		case finished := <-closed:
			if !finished {
				t.Fatal("close() = false, want true: there is no flusher to wait for")
			}
		case <-time.After(5 * time.Second):
			t.Fatal("close() waited for a feed that never started")
		}
		if feed.start(func([]byte) error {
			t.Error("a feed closed before it started wrote to its replica")
			return nil
		}, nil) {
			t.Fatal("start() after close = true, want false")
		}
	})
}
