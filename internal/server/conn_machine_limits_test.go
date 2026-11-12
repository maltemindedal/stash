//go:build linux || darwin

package server

import "testing"

func TestConnMachineRetainsTheReadBuffersTheEventLoopFills(t *testing.T) {
	// The machine builds on every platform, so it cannot name the event loop's
	// constants and carries its own. They have to stay large enough for the
	// traffic the loop produces, or ordinary connections would reallocate their
	// buffers on every read.
	if want := 4 * eventLoopReadChunk; retainedReadBufferCap < want {
		t.Fatalf("retainedReadBufferCap = %d, want at least %d (four event-loop read chunks)", retainedReadBufferCap, want)
	}
}
