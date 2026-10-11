package server

import (
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/maltemindedal/stash/internal/config"
	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/storage"
)

// BenchmarkPropagateToReplicas measures what propagating one write costs a master:
// encoding its frame, counting it into the replication offset and queueing it for
// every replica. The writers run in parallel, as clients writing different keys do.
// Each replica's feed writes to a connection that discards the bytes.
func BenchmarkPropagateToReplicas(b *testing.B) {
	frames := []protocol.Value{protocol.Array{Elements: []protocol.Value{
		protocol.BulkString{Data: []byte("SET")},
		protocol.BulkString{Data: []byte("key:000000000042")},
		protocol.BulkString{Data: []byte("value:0123456789abcdef")},
	}}}

	for _, replicas := range []int{0, 1, 3} {
		replicas := replicas
		b.Run(fmt.Sprintf("replicas=%d", replicas), func(b *testing.B) {
			srv := newTestServer(b, config.Config{}, slog.New(slog.NewTextHandler(io.Discard, nil)), storage.NewStore(), nil)
			for id := uint64(1); id <= uint64(replicas); id++ {
				conn := &stubConn{}
				srv.replicaPeers.Add(srv.replication, id, conn, 6380, newReplicaPeerStateForTest(id, conn))
			}
			defer srv.replicaPeers.StopFeeds(time.Second)

			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					srv.propagateToReplicas(frames)
				}
			})
			b.StopTimer()

			// A feed that fell 256 MiB behind would have been dropped, and the rest
			// of the run would have measured fewer replicas than its name says.
			if got := srv.replicaPeers.Count(); got != replicas {
				b.Fatalf("%d replicas attached after the run, want %d", got, replicas)
			}
		})
	}
}
