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
	go func() {
		done <- feed.run(func(chunk []byte) error {
			<-release // a slow socket: nothing is written until the test lets it
			mu.Lock()
			written.Write(chunk)
			writes++
			mu.Unlock()
			return nil
		})
	}()

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
	go func() { result <- feed.run(func([]byte) error { return boom }) }()
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
	go func() {
		_ = feed.run(func(chunk []byte) error {
			mu.Lock()
			sizes = append(sizes, len(chunk))
			mu.Unlock()
			return nil
		})
	}()

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
	go func() { _ = feed.run(func([]byte) error { return nil }) }()
	feed.close(time.Second)

	if err := feed.enqueue([]byte("x")); err == nil {
		t.Fatal("enqueue() after close error = nil, want an error")
	}
}
