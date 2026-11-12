package server

import (
	"bytes"
	"sync"
	"testing"
	"time"

	"github.com/maltemindedal/stash/internal/server/output"
)

// outputWire is the socket of a connection served by its own goroutine, in
// memory: it records what it is written and whether it was closed.
type outputWire struct {
	mu        sync.Mutex
	received  bytes.Buffer
	closed    chan struct{}
	closeOnce sync.Once
}

func newOutputWire() *outputWire {
	return &outputWire{closed: make(chan struct{})}
}

func (w *outputWire) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.received.Write(p)
}

func (w *outputWire) Close() error {
	w.closeOnce.Do(func() { close(w.closed) })
	return nil
}

func (w *outputWire) sent() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.received.String()
}

func (w *outputWire) isClosed() bool {
	select {
	case <-w.closed:
		return true
	default:
		return false
	}
}

// newGoroutineOutput returns the Output of a connection served by its own
// goroutine, which writes to wire, under the limits in shared.
func newGoroutineOutput(shared *output.Shared, wire *outputWire) *output.Output {
	return output.NewBlocking(shared, wire, discardLogger)
}

// waitForOutput polls until done reports true, and fails the test with what it
// was waiting for after five seconds.
func waitForOutput(t *testing.T, what string, done func() bool) {
	t.Helper()

	for deadline := time.Now().Add(5 * time.Second); !done(); {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}
