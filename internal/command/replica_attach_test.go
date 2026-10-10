package command

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/maltemindedal/stash/internal/config"
	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/server"
	"github.com/maltemindedal/stash/internal/storage"
)

// TestEveryFrameCountedWhileAReplicaAttachesIsBelowItsBaseOrSentToIt attaches
// replicas one at a time through PSYNC's attach cut while frames are handed to
// the replicas from the paths that do so: client writes, which the sequencer
// orders and which count their frames without the registry lock while no
// replica is registered; WAIT's GETACK, which nothing orders; and the expiry
// DELs that a replica's own full resync publishes, which nothing orders either.
// Each replica acknowledges what it was sent up to a client write's frame, and
// must then count at exactly that frame's end offset, as WAIT counts it. A frame
// counted above the replica's base but not sent to it leaves the replica short;
// one sent to it although counted at or below its base puts it past.
//
// Every replica is removed before the next attaches, so that each attach finds
// none registered and client writes on the path without the lock.
func TestEveryFrameCountedWhileAReplicaAttachesIsBelowItsBaseOrSentToIt(t *testing.T) {
	tests := []struct {
		name string
		// getAcks runs WAIT's GETACK alongside the client writes.
		getAcks bool
		// fullResyncs replaces the dataset as a replica's full resync does, with
		// one that holds an expired key, which the store removes and publishes.
		fullResyncs bool
	}{
		{name: "client writes"},
		{name: "client writes and WAIT's GETACK", getAcks: true},
		{name: "client writes and the expiry DELs of a replica's full resync", fullResyncs: true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			store := storage.NewStore()
			if tt.fullResyncs {
				// A store with maxmemory recounts the dataset a full resync
				// installs, and removes and publishes the keys in it whose TTL
				// has passed. It also makes every write lock every shard, so it is
				// set only where it is needed.
				store.ConfigureMaxMemory(1<<40, 5)
			}
			logger := slog.New(slog.NewTextHandler(io.Discard, nil))
			// Wires the executor and store to the server's replica registry,
			// offsets, write ordering and expiry publishing, as in production.
			var executor *Executor
			if _, err := server.New(config.Config{}, logger, store, func(services server.Services) (server.CommandExecutor, error) {
				built, err := New(services)
				executor = built
				return built, err
			}); err != nil {
				t.Fatalf("server.New() error = %v", err)
			}
			registry, offsets := executor.replicaPeers, executor.replication

			// A client write's value -> the end offset counted for its frame, for
			// the writes made since the current replica attached. It is emptied at
			// every attach and holds at most maxTrackedWrites, so it stays small
			// however fast the writers go: the replica only needs one of them.
			var endsMu sync.Mutex
			ends := make(map[string]int64)
			const maxTrackedWrites = 1 << 16

			stop := make(chan struct{})
			var wg sync.WaitGroup
			run := func(step func(i int) bool) {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; ; i++ {
						select {
						case <-stop:
							return
						default:
						}
						if !step(i) {
							return
						}
					}
				}()
			}
			stopped := false
			stopWriters := func() {
				if !stopped {
					stopped = true
					close(stop)
					wg.Wait()
				}
			}
			defer stopWriters()

			const clientWriters = 2
			for w := 0; w < clientWriters; w++ {
				key := fmt.Sprintf("writer-%d", w)
				ctx := clientContext(executor)
				run(func(i int) bool {
					tag := fmt.Sprintf("%s-%d", key, i)
					// As the server runs a client write (executeClientRequest): its
					// frames are handed to the replicas before its locks are released.
					result, err := handle(ctx, executor, requestValue("SET", key, tag), true)
					if err != nil {
						t.Errorf("SET error = %v", err)
						return false
					}
					payload, err := protocol.EncodeValues(result.Propagation)
					if err != nil {
						result.Release()
						t.Errorf("EncodeValues() error = %v", err)
						return false
					}
					end, _, _ := registry.PropagateOrdered(offsets, payload)
					result.Release()
					endsMu.Lock()
					if len(ends) < maxTrackedWrites {
						ends[tag] = end
					}
					endsMu.Unlock()
					return true
				})
			}
			if tt.getAcks {
				run(func(int) bool {
					if err := executor.requestReplicaAcknowledgements(); err != nil {
						t.Errorf("requestReplicaAcknowledgements() error = %v", err)
						return false
					}
					return true
				})
			}
			if tt.fullResyncs {
				snapshot := storage.NewStore()
				if _, err := snapshot.Set("expired", []byte("v"), 1); err != nil { // a TTL deadline long past
					t.Fatalf("Set() error = %v", err)
				}
				run(func(int) bool {
					store.ReplaceWith(snapshot) // as consumeFullResync does, holding no sequencer lock
					return true
				})
			}

			const attaches = 300
			for i := 0; i < attaches && !t.Failed(); i++ {
				id := uint64(1000 + i)
				stream := &replicaStream{}
				state := newTestClientState(executor, id)
				state.BindResponseWriter(bufio.NewWriter(stream))
				result, err := handle(server.WithClientState(context.Background(), state), executor, requestValue("PSYNC", "?", "-1"), true)
				if err != nil || !result.RegisterReplica {
					t.Fatalf("PSYNC: RegisterReplica = %v, error = %v", result.RegisterReplica, err)
				}
				// The writes counted before the attach are not in this replica's
				// stream; those counted after it start here.
				endsMu.Lock()
				ends = make(map[string]int64)
				endsMu.Unlock()
				// As the connection does once the full resync reply is ahead
				// (registerReplicaPeer).
				if !registry.Start(id, stream) {
					t.Fatalf("Start(%d) = false, want true", id)
				}

				prefix, end := waitForClientWriteInStream(t, stream, func(tag string) (int64, bool) {
					endsMu.Lock()
					defer endsMu.Unlock()
					end, ok := ends[tag]
					return end, ok
				})
				if !registry.UpdateAck(id, int64(prefix)) {
					t.Fatalf("UpdateAck(%d) = false, want true", id)
				}
				if got := registry.CountReplicasAtOrAbove(end); got != 1 {
					t.Errorf("attach %d: the replica acknowledged the %d bytes it was sent up to a client write counted to end at %d, and counts below it: a frame counted after its base was not sent to it", i, prefix, end)
				}
				if got := registry.CountReplicasAtOrAbove(end + 1); got != 0 {
					t.Errorf("attach %d: the replica acknowledged the %d bytes it was sent up to a client write counted to end at %d, and counts past it: it was sent a frame counted at or below its base", i, prefix, end)
				}
				registry.Remove(id)
			}
		})
	}
}

// waitForClientWriteInStream waits until the replica has been sent a client
// write's frame whose end offset is known, and returns how many bytes of its
// stream end with the last such frame, and that offset. It stops the test at
// once when a writer has reported an error, since the frame may never come.
func waitForClientWriteInStream(t *testing.T, stream *replicaStream, endOf func(tag string) (int64, bool)) (prefix int, end int64) {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	for {
		if t.Failed() {
			t.Fatal("stopped waiting for the replica's stream: a writer reported an error")
		}
		parser := protocol.NewParser(bytes.NewReader(stream.Bytes()))
		position, found := 0, false
		for {
			value, err := parser.Parse()
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break // the end of what was sent so far, possibly inside a frame
			}
			if err != nil {
				t.Fatalf("the replica's stream does not parse: %v", err)
			}
			n, err := protocol.EncodedLen(value)
			if err != nil {
				t.Fatalf("EncodedLen() error = %v", err)
			}
			position += n
			array, ok := value.(protocol.Array)
			if !ok || len(array.Elements) != 3 {
				continue
			}
			if name, _ := protocol.Bytes(array.Elements[0]); string(name) != "SET" {
				continue
			}
			tag, _ := protocol.Bytes(array.Elements[2])
			if counted, known := endOf(string(tag)); known {
				prefix, end, found = position, counted, true
			}
		}
		if found {
			return prefix, end
		}
		if time.Now().After(deadline) {
			t.Fatalf("the replica was sent no client write with a known offset within 10s (%d bytes sent)", len(stream.Bytes()))
		}
		time.Sleep(50 * time.Microsecond)
	}
}

// replicaStream is the connection of a replica attached in a test: it keeps
// what the replica's feed writes to it.
type replicaStream struct {
	mu   sync.Mutex
	data []byte
}

func (s *replicaStream) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = append(s.data, p...)
	return len(p), nil
}

func (s *replicaStream) Bytes() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]byte(nil), s.data...)
}

func (s *replicaStream) Close() error { return nil }

func (s *replicaStream) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 6380}
}
