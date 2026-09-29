//go:build (linux || darwin) && !race

package test

import (
	"net"
	"sync"
	"testing"
	"time"
)

// Not built under the race detector: it moves several hundred megabytes through
// the server, which the detector slows down by an order of magnitude.

func TestEventLoopBoundsTheOutputHeldForAllStalledSubscribersTogether(t *testing.T) {
	if testing.Short() {
		t.Skip("queues hundreds of megabytes")
	}

	// Each subscriber stays under the per-connection limit (24 MiB against 32),
	// but together they hold more than the 128 MiB budget even after the kernel's
	// socket buffers have taken their share, so the server closes some of them
	// rather than let fan-out to many slow clients multiply memory.
	addr, stop, errCh := startTestServer(t, eventLoopTestConfig())
	defer func() {
		stop()
		waitForServerStop(t, errCh)
	}()

	const subscribers = 12
	conns := make([]net.Conn, subscribers)
	for i := range conns {
		conns[i] = stalledSubscriber(t, addr, "news")
	}
	publishMiB(t, addr, "news", 24)

	var wg sync.WaitGroup
	var mu sync.Mutex
	closedCount := 0
	for _, conn := range conns {
		wg.Add(1)
		go func(conn net.Conn) {
			defer wg.Done()
			if _, closed := drain(conn, 2*time.Second); closed {
				mu.Lock()
				closedCount++
				mu.Unlock()
			}
		}(conn)
	}
	wg.Wait()

	if closedCount == 0 {
		t.Fatalf("no subscriber was closed although %d of them were each holding about 24 MiB, want the server to enforce its total budget", subscribers)
	}
	if closedCount == subscribers {
		t.Fatalf("all %d subscribers were closed, want the server to shed only as many as needed", subscribers)
	}
}
