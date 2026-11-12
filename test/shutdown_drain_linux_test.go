//go:build linux

package test

import (
	"syscall"
	"testing"
	"time"

	"github.com/maltemindedal/stash/internal/config"
	"github.com/maltemindedal/stash/internal/protocol"
)

// TestAReplicaReceivesEveryAcknowledgedWriteWhenItsMastersAcceptLoopFails is
// TestAReplicaReceivesEveryAcknowledgedWriteWhenItsMasterShutsDown for a master
// that stops because accepting failed, with its context still live. In the
// default mode its clients stay connected while it sends the replica what is
// queued, and one writes then. That write reaches no replica, and must not cut
// the replica off: its stream, ended for the shutdown, used to refuse the write,
// which dropped the replica and closed its socket in the middle of the drain.
// The event loop closes its clients when it stops accepting, and used to close
// the replica with them.
func TestAReplicaReceivesEveryAcknowledgedWriteWhenItsMastersAcceptLoopFails(t *testing.T) {
	forEachNetworkingMode(t, defaultTestConfig(), func(t *testing.T, cfg config.Config) {
		replicaDrainsAtShutdown(t, cfg, func(drain *shutdownDrain) {
			breakAcceptLoop(t, drain.addr)
			waitForLogFragments(t, drain.logs, 5*time.Second, "sending the replicas what is queued for them before shutting down")
			if !cfg.EventLoop {
				assertCommandResponse(t, drain.client, drain.clientParser, protocol.SimpleString{Value: "OK"}, "SET", "written-during-the-drain", "x")
			}
		}, syscall.EINVAL)
	})
}
