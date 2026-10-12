package command

import (
	"context"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/maltemindedal/stash/internal/aof"
	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/record"
	"github.com/maltemindedal/stash/internal/server"
)

// BenchmarkSequencedSetUnderMaxMemory measures parallel SETs of different keys
// through Handle, recorded to a real append-only file or handed to a stand-in
// for one attached replica, with maxmemory off and on. The limit is far above
// what the keys use, so nothing is evicted: what is measured is how the writes
// are ordered against each other. Each goroutine cycles through keys of its own.
func BenchmarkSequencedSetUnderMaxMemory(b *testing.B) {
	sinks := []struct {
		name   string
		policy aof.Policy
		aof    bool
	}{
		{name: "aof=everysec", policy: aof.PolicyEverysec, aof: true},
		{name: "aof=always", policy: aof.PolicyAlways, aof: true},
		{name: "replica"},
	}

	for _, sink := range sinks {
		sink := sink
		for _, maxMemory := range []int64{0, 1 << 30} {
			maxMemory := maxMemory
			b.Run(sink.name+"/maxmemory="+strconv.FormatInt(maxMemory, 10), func(b *testing.B) {
				var log record.Log
				if sink.aof {
					writer, err := aof.OpenWriter(context.Background(), filepath.Join(b.TempDir(), "appendonly.aof"), sink.policy, nil)
					if err != nil {
						b.Fatalf("OpenWriter() error = %v", err)
					}
					defer func() {
						if err := writer.Close(); err != nil {
							b.Errorf("Close() error = %v", err)
						}
					}()
					log = writer
				}
				executor := newTestExecutorWith(func(s *server.Services) {
					var stream record.Stream = server.NewReplicaStream(s.Replication, s.Replicas, s.Logger)
					if !sink.aof {
						stream = &benchStream{buf: make([]byte, 0, 1<<20)}
					}
					s.Recorder = record.New(stream, s.Logger)
					s.Recorder.SetLog(log)
				})
				executor.store.ConfigureMaxMemory(maxMemory, 0)
				var writers atomic.Int64

				b.ReportAllocs()
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					ctx := clientContext(executor)
					call := callFromContext(ctx)
					prefix := "key:" + strconv.FormatInt(writers.Add(1), 10) + ":"
					requests := make([]protocol.Value, 256)
					for i := range requests {
						requests[i] = requestValue("SET", prefix+strconv.Itoa(i), "value:0123456789abcdef")
					}
					for i := 0; pb.Next(); i++ {
						if _, err := executor.Handle(ctx, call, requests[i%len(requests)]); err != nil {
							b.Errorf("Handle() error = %v", err)
							return
						}
					}
				})
			})
		}
	}
}
