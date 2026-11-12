package output

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/maltemindedal/stash/internal/protocol"
)

// countingDiscard drops what it is written and counts it. It accepts at most
// limit bytes per Write, like a transport that applies backpressure.
type countingDiscard struct {
	limit int
	total int
}

func (w *countingDiscard) Write(p []byte) (int, error) {
	n := min(len(p), w.limit)
	w.total += n
	return n, nil
}

// idleBufferCapBound is the capacity above which an idle connection must not
// keep a write buffer.
const idleBufferCapBound = 64 << 10

// bufferIdentity returns the address of the first byte of b's backing array. It
// changes whenever the Output replaces a buffer with a new one.
func bufferIdentity(b []byte) *byte {
	if cap(b) == 0 {
		return nil
	}
	return &b[:1][0]
}

// serviceUntilWritten has the Event loop's Service write to w until nothing
// waits for out.
func serviceUntilWritten(t *testing.T, out *Output, w io.Writer) {
	t.Helper()

	for out.Backlog() > 0 {
		if err := out.Service(w).Closed; err != nil {
			t.Fatalf("Service() closed the Output: %v", err)
		}
	}
}

func TestAnOutputRetainsTheWriteBufferTheEventLoopFills(t *testing.T) {
	// The loop executes a connection's requests until outputHighWater
	// bytes of output are pending, flushes them, and carries on. A client that
	// pipelines 64 KiB replies takes the buffer to that mark and past it again
	// and again, so that buffer has to be kept.
	if want := 2 * outputHighWater; outputRetainedBuffer < want {
		t.Fatalf("outputRetainedBuffer = %d, want at least %d (twice the output high-water mark)", outputRetainedBuffer, want)
	}

	reply := protocol.BulkString{Data: bytes.Repeat([]byte("x"), 64<<10)}
	out := newLoopOutput(NewShared())

	// drain flushes everything and returns the identity of the buffer it emptied.
	drain := func() *byte {
		identity := bufferIdentity(out.queued)
		serviceUntilWritten(t, out, io.Discard)
		return identity
	}

	var identities []*byte
	for round := 0; round < 5; round++ {
		for request := 0; request < 64; request++ {
			if !out.MayReadRequests() {
				identities = append(identities, drain())
			}
			if err := out.Reply(reply); err != nil {
				t.Fatalf("Reply() error = %v", err)
			}
		}
		identities = append(identities, drain())
	}

	// The output fills one buffer while it writes the other, so two buffers
	// serve every pipeline; a third would be an allocation per pipeline.
	if distinct := distinctBuffers(identities); distinct > 2 {
		t.Fatalf("the output used %d buffers in %d flushes", distinct, len(identities))
	}
}

func TestAnOutputReleasesItsWriteBufferAfterALargeReply(t *testing.T) {
	// A connection that once sent a large value must not keep the buffer it grew
	// to hold the reply once the reply has drained. A reply the Output would
	// keep working memory for is kept, so the next one needs no new buffer.
	tests := []struct {
		name         string
		replySize    int
		writeLimit   int
		wantReleased bool
	}{
		{name: "a large reply the peer accepts in one write", replySize: 32 << 20, writeLimit: 1 << 30, wantReleased: true},
		{name: "a large reply the peer accepts a megabyte at a time", replySize: 32 << 20, writeLimit: 1 << 20, wantReleased: true},
		{name: "a reply just over what the Output keeps", replySize: outputRetainedBuffer + 1, writeLimit: 1 << 30, wantReleased: true},
		{name: "a reply of a megabyte keeps its buffer for the next one", replySize: 1 << 20, writeLimit: 1 << 30, wantReleased: false},
		{name: "a small reply keeps its buffer for the next one", replySize: 1 << 10, writeLimit: 1 << 30, wantReleased: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reply := protocol.BulkString{Data: bytes.Repeat([]byte("x"), tt.replySize)}
			out := newLoopOutput(NewShared())
			peer := &countingDiscard{limit: tt.writeLimit}

			if err := out.Reply(reply); err != nil {
				t.Fatalf("Reply() error = %v", err)
			}
			if got := out.Backlog(); got < tt.replySize {
				t.Fatalf("Backlog() = %d, want the %d byte reply buffered", got, tt.replySize)
			}

			serviceUntilWritten(t, out, peer)
			wantTotal := len(fmt.Sprintf("$%d\r\n", tt.replySize)) + tt.replySize + len("\r\n")
			if peer.total != wantTotal {
				t.Fatalf("peer received %d bytes, want %d", peer.total, wantTotal)
			}

			got := max(cap(out.queued), cap(out.inflight), cap(out.spare))
			if tt.wantReleased && got > idleBufferCapBound {
				t.Fatalf("write buffer capacity after the reply drained = %d bytes, want at most %d", got, idleBufferCapBound)
			}
			if !tt.wantReleased && (got == 0 || got > outputRetainedBuffer) {
				t.Fatalf("write buffer capacity after the reply drained = %d bytes, want it kept for reuse and at most %d", got, outputRetainedBuffer)
			}

			// The released buffer is still good for the next output.
			if !out.Push([]byte("+later\r\n")) {
				t.Fatal("Push() = false, want the later frame queued")
			}
			serviceUntilWritten(t, out, peer)
			if peer.total != wantTotal+len("+later\r\n") {
				t.Fatalf("peer received %d bytes after the later push, want %d", peer.total, wantTotal+len("+later\r\n"))
			}
		})
	}
}

func TestAnOutputKeepsItsWriteBufferWhilePipelinedRepliesKeepDraining(t *testing.T) {
	// A client that sends a pipeline of requests and reads all the replies before
	// the next pipeline drains the buffer completely each time. That buffer is
	// working memory, so replacing it after every pipeline would cost an
	// allocation and a copy per pipeline.
	tests := []struct {
		name      string
		count     int
		replySize int
	}{
		{name: "100 replies of 1 KiB", count: 100, replySize: 1 << 10},
		{name: "16 replies of 4 KiB", count: 16, replySize: 4 << 10},
		{name: "16 replies of 64 KiB", count: 16, replySize: 64 << 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reply := protocol.BulkString{Data: bytes.Repeat([]byte("x"), tt.replySize)}
			out := newLoopOutput(NewShared())

			var identities []*byte
			var caps []int
			for round := 0; round < 20; round++ {
				for request := 0; request < tt.count; request++ {
					if err := out.Reply(reply); err != nil {
						t.Fatalf("Reply() error = %v", err)
					}
				}
				// The identity is taken while the replies are buffered, since a buffer
				// that has drained is empty.
				identities = append(identities, bufferIdentity(out.queued))
				caps = append(caps, cap(out.queued))
				serviceUntilWritten(t, out, io.Discard)
			}

			// The output fills one buffer while it writes the other, so two buffers
			// serve every pipeline; a third would be an allocation per pipeline.
			if distinct := distinctBuffers(identities); distinct > 2 {
				t.Fatalf("the output used %d buffers in %d pipelines (capacities %v)", distinct, len(identities), caps)
			}
		})
	}
}

// distinctBuffers counts the different backing arrays among identities.
func distinctBuffers(identities []*byte) int {
	seen := make(map[*byte]struct{}, len(identities))
	for _, identity := range identities {
		seen[identity] = struct{}{}
	}
	return len(seen)
}
