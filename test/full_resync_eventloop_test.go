//go:build linux || darwin

package test

import "testing"

// TestAWriteMadeWhileAFullResyncIsSentReachesTheReplicaUnderTheEventLoop is the
// event-loop row of TestAWriteMadeWhileAFullResyncIsSentReachesTheReplica. The
// event loop runs PSYNC and every other client's command on one goroutine, so a
// write from another client always runs after PSYNC has returned. It has no WAIT
// row: a WAIT that has to wait is an error under --event-loop.
func TestAWriteMadeWhileAFullResyncIsSentReachesTheReplicaUnderTheEventLoop(t *testing.T) {
	cfg := defaultTestConfig()
	cfg.EventLoop = true
	writeDuringFullResync(t, cfg, false)
}
