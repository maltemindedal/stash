package server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/maltemindedal/stash/internal/protocol"
)

// connMachineReadSize is how much the event loop reads from a socket at a time.
const connMachineReadSize = 64 * 1024

// drainTo flushes everything the machine has buffered to w.
func drainTo(b *testing.B, machine *ConnMachine, w io.Writer) {
	b.Helper()

	for machine.HasPendingOutput() {
		if err := machine.Flush(w); err != nil {
			b.Fatalf("Flush() error = %v", err)
		}
	}
}

// replyRunner answers every request with one bulk string of the given size.
func replyRunner(size int) ConnCommandRunner {
	reply := protocol.BulkString{Data: bytes.Repeat([]byte("x"), size)}
	return func(context.Context, protocol.Value) ([]protocol.Value, error) {
		return []protocol.Value{reply}, nil
	}
}

// BenchmarkConnMachinePipelinedReplies measures a long-lived connection whose
// client sends a pipeline of GETs and reads all the replies before sending the
// next pipeline. The reply buffer is reused from one pipeline to the next.
func BenchmarkConnMachinePipelinedReplies(b *testing.B) {
	benchmarks := []struct {
		name      string
		count     int
		replySize int
	}{
		{name: "100 replies of 1 KiB", count: 100, replySize: 1 << 10},
		{name: "16 replies of 4 KiB", count: 16, replySize: 4 << 10},
		{name: "16 replies of 16 KiB", count: 16, replySize: 16 << 10},
		{name: "16 replies of 64 KiB", count: 16, replySize: 64 << 10},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			pipeline := bytes.Repeat(machineFrame("GET", "key"), bm.count)
			run := replyRunner(bm.replySize)
			machine := NewConnMachine(nil)

			b.SetBytes(int64(bm.count * bm.replySize))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := machine.Feed(pipeline); err != nil {
					b.Fatalf("Feed() error = %v", err)
				}
				if err := machine.ProcessPending(context.Background(), run); err != nil {
					b.Fatalf("ProcessPending() error = %v", err)
				}
				drainTo(b, machine, io.Discard)
			}
		})
	}
}

// BenchmarkConnMachineLargeReply measures a long-lived connection that is sent
// one large reply at a time, such as a GET of a big value.
func BenchmarkConnMachineLargeReply(b *testing.B) {
	for _, size := range []int{100 << 10, 1 << 20} {
		b.Run(fmt.Sprintf("%d KiB reply", size>>10), func(b *testing.B) {
			get := machineFrame("GET", "key")
			run := replyRunner(size)
			machine := NewConnMachine(nil)

			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := machine.Feed(get); err != nil {
					b.Fatalf("Feed() error = %v", err)
				}
				if err := machine.ProcessPending(context.Background(), run); err != nil {
					b.Fatalf("ProcessPending() error = %v", err)
				}
				drainTo(b, machine, io.Discard)
			}
		})
	}
}

// BenchmarkConnMachinePipelinedReadsWithPartialFrames measures a connection that
// receives a long stream of SETs in event-loop sized reads. The read size is not
// a multiple of the frame size, so most reads end inside a frame and the next
// read starts with its first half.
func BenchmarkConnMachinePipelinedReadsWithPartialFrames(b *testing.B) {
	benchmarks := []struct {
		name      string
		valueSize int
	}{
		{name: "100 byte values", valueSize: 100},
		{name: "4 KiB values", valueSize: 4 << 10},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			frame := machineFrame("SET", "key:123", string(bytes.Repeat([]byte("v"), bm.valueSize)))
			stream := bytes.Repeat(frame, (1<<20)/len(frame))
			run := func(context.Context, protocol.Value) ([]protocol.Value, error) {
				return []protocol.Value{protocol.SimpleString{Value: "OK"}}, nil
			}
			machine := NewConnMachine(nil)

			b.SetBytes(int64(len(stream)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for off := 0; off < len(stream); off += connMachineReadSize {
					end := min(off+connMachineReadSize, len(stream))
					if err := machine.Feed(stream[off:end]); err != nil {
						b.Fatalf("Feed() error = %v", err)
					}
					if err := machine.ProcessPending(context.Background(), run); err != nil {
						b.Fatalf("ProcessPending() error = %v", err)
					}
					drainTo(b, machine, io.Discard)
				}
			}
		})
	}
}
