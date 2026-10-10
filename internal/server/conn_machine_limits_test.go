//go:build linux || darwin

package server

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/maltemindedal/stash/internal/protocol"
)

func TestConnMachineRetainsTheReadBuffersTheEventLoopFills(t *testing.T) {
	// The machine builds on every platform, so it cannot name the event loop's
	// constants and carries its own. They have to stay large enough for the
	// traffic the loop produces, or ordinary connections would reallocate their
	// buffers on every read.
	if want := 4 * eventLoopReadChunk; retainedReadBufferCap < want {
		t.Fatalf("retainedReadBufferCap = %d, want at least %d (four event-loop read chunks)", retainedReadBufferCap, want)
	}
}

func TestConnMachineRetainsTheWriteBufferTheEventLoopFills(t *testing.T) {
	// The loop executes a connection's requests until eventLoopOutputHighWater
	// bytes of output are pending, flushes them, and carries on. A client that
	// pipelines 64 KiB replies takes the buffer to that mark and past it again
	// and again, so that buffer has to be kept.
	if want := 2 * eventLoopOutputHighWater; retainedWriteBufferCap < want {
		t.Fatalf("retainedWriteBufferCap = %d, want at least %d (twice the output high-water mark)", retainedWriteBufferCap, want)
	}

	reply := bytes.Repeat([]byte("x"), 64<<10)
	run := func(context.Context, protocol.Value) ([]protocol.Value, error) {
		return []protocol.Value{protocol.BulkString{Data: reply}}, nil
	}
	pipeline := bytes.Repeat(machineFrame("GET", "k"), 64)
	machine := NewConnMachine(nil)

	// drain flushes everything and returns the identity of the buffer it emptied.
	drain := func() *byte {
		identity := bufferIdentity(machine.writeBuf)
		for machine.HasPendingOutput() {
			if err := machine.Flush(io.Discard); err != nil {
				t.Fatalf("Flush() error = %v", err)
			}
		}
		return identity
	}

	var identities []*byte
	for round := 0; round < 5; round++ {
		if err := machine.Feed(pipeline); err != nil {
			t.Fatalf("Feed() error = %v", err)
		}
		for {
			if machine.PendingOutputBytes() > eventLoopOutputHighWater {
				identities = append(identities, drain())
			}
			more, err := machine.ProcessNext(context.Background(), run)
			if err != nil {
				t.Fatalf("ProcessNext() error = %v", err)
			}
			if !more {
				break
			}
		}
		identities = append(identities, drain())
	}

	replaced := 0
	for i := 1; i < len(identities); i++ {
		if identities[i] != identities[i-1] {
			replaced++
		}
	}
	if limit := len(identities) / 4; replaced > limit {
		t.Fatalf("the write buffer was replaced %d times in %d flushes, want at most %d", replaced, len(identities), limit)
	}
}
