package replication

import (
	"fmt"
	"sync"
	"testing"

	"github.com/maltemindedal/stash/internal/protocol"
)

// BenchmarkPropagateToReplicas measures what propagating one write costs a master:
// encoding its frame, counting it into the replication offset and queueing it for
// every replica. The writers run in parallel, as clients writing different keys do.
// Each replica's Output copies the frame under a lock, as the server's does,
// and writes it nowhere: BenchmarkSetWithAReplica in test/ measures a write
// with a replica's real connection behind it.
func BenchmarkPropagateToReplicas(b *testing.B) {
	frames := []protocol.Value{protocol.Array{Elements: []protocol.Value{
		protocol.BulkString{Data: []byte("SET")},
		protocol.BulkString{Data: []byte("key:000000000042")},
		protocol.BulkString{Data: []byte("value:0123456789abcdef")},
	}}}

	for _, replicas := range []int{0, 1, 3} {
		replicas := replicas
		b.Run(fmt.Sprintf("replicas=%d", replicas), func(b *testing.B) {
			master := newTestMaster()
			for id := uint64(1); id <= uint64(replicas); id++ {
				master.Add(Candidate{ID: id, ListeningPort: 6380, Out: &copyingOutput{buf: make([]byte, 0, 1<<20)}})
			}

			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					payload, err := protocol.EncodeValues(frames)
					if err != nil {
						panic(err)
					}
					master.propagate(payload, true)
				}
			})
			b.StopTimer()

			// A replica whose Output refused a frame would have been detached, and
			// the rest of the run would have measured fewer replicas than its name
			// says.
			if got := master.Count(); got != replicas {
				b.Fatalf("%d replicas attached after the run, want %d", got, replicas)
			}
		})
	}
}

// copyingOutput is a Replica's Output that copies each frame into a buffer it
// reuses, so the copies allocate nothing, and never refuses one.
type copyingOutput struct {
	mu  sync.Mutex
	buf []byte
}

func (o *copyingOutput) Push(frame []byte) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.buf)+len(frame) > cap(o.buf) {
		o.buf = o.buf[:0]
	}
	o.buf = append(o.buf, frame...)
	return true
}

func (o *copyingOutput) MarkReplica() {}
func (o *copyingOutput) HoldPushes()  {}
func (o *copyingOutput) Err() error   { return nil }
func (o *copyingOutput) EndStream()   {}
