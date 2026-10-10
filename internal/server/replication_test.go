package server

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maltemindedal/stash/internal/config"
	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/storage"
)

func TestReplicaRegistryCountReplicasAtOrAboveWithNotify(t *testing.T) {
	registry := NewReplicaRegistry()
	serverConn, replicaConn := net.Pipe()
	defer func() { _ = serverConn.Close() }()
	defer func() { _ = replicaConn.Close() }()

	state := newReplicaPeerStateForTest(1, serverConn)
	registry.Add(nil, 1, serverConn, 6380, state)

	count, changed := registry.CountReplicasAtOrAboveWithNotify(10)
	if count != 0 {
		t.Fatalf("initial count = %d, want 0", count)
	}

	if ok := registry.UpdateAck(1, 10); !ok {
		t.Fatal("UpdateAck() = false, want true")
	}

	select {
	case <-changed:
	case <-time.After(time.Second):
		t.Fatal("CountReplicasAtOrAboveWithNotify() channel was not notified")
	}

	count, _ = registry.CountReplicasAtOrAboveWithNotify(10)
	if count != 1 {
		t.Fatalf("updated count = %d, want 1", count)
	}
}

func TestReplicaRegistryRemoveAndCloseReturnsCloseError(t *testing.T) {
	registry := NewReplicaRegistry()
	conn := &stubConn{closeErr: errors.New("close boom")}
	registry.Add(nil, 1, conn, 6380, newReplicaPeerStateForTest(1, conn))

	err := registry.RemoveAndClose(1)
	if err == nil || err.Error() != "close boom" {
		t.Fatalf("RemoveAndClose() error = %v, want close boom", err)
	}
}

func TestServerPropagateToReplicasRemovesFailingReplica(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := newTestServer(t, config.Config{}, logger, storage.NewStore(), nil)

	serverConn := &recordingConn{}
	srv.replicaPeers.Add(srv.replication, 1, serverConn, 6380, newReplicaPeerStateForTest(1, serverConn))
	failingConn := &stubConn{writeErr: errors.New("write boom")}
	srv.replicaPeers.Add(srv.replication, 2, failingConn, 6381, newReplicaPeerStateForTest(2, failingConn))

	// Propagating only queues the command for each replica; the replica whose
	// socket fails is found out, and dropped, by its feed.
	report := srv.propagateToReplicas([]protocol.Value{protocol.SimpleString{Value: "OK"}})
	if report.attempted != 2 {
		t.Fatalf("report.attempted = %d, want 2", report.attempted)
	}
	if report.succeeded != 2 || report.failed != 0 {
		t.Fatalf("report = %+v, want both queued", report)
	}
	deadline := time.Now().Add(2 * time.Second)
	for srv.replicaPeers.Count() != 1 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if srv.replicaPeers.Count() != 1 {
		t.Fatalf("replicaPeers.Count() = %d, want the failing replica dropped", srv.replicaPeers.Count())
	}
	for len(serverConn.Bytes()) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	parser := protocol.NewParser(bytes.NewReader(serverConn.Bytes()))
	value, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if got, ok := value.(protocol.SimpleString); !ok || got.Value != "OK" {
		t.Fatalf("parsed value = %#v, want simple string OK", value)
	}
}

// TestConcurrentWritesReachEveryReplicaInOffsetOrder has writers propagate at the
// same time, as writers on different stripes do, and checks that every replica
// receives each frame at the stream position the master counted for it. A replica
// that acknowledges offset N must hold every frame that ends at or before N, which
// WAIT relies on. Counting the offset and queueing the frame used to be separate
// steps, and thousands of the 16,000 frames arrived out of place.
func TestConcurrentWritesReachEveryReplicaInOffsetOrder(t *testing.T) {
	tests := []struct {
		name     string
		replicas int
	}{
		{name: "one replica", replicas: 1},
		{name: "three replicas", replicas: 3},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			srv := newTestServer(t, config.Config{}, slog.New(slog.NewTextHandler(io.Discard, nil)), storage.NewStore(), nil)
			conns := make([]*recordingConn, tt.replicas)
			for i := range conns {
				id := uint64(i + 1)
				conns[i] = &recordingConn{}
				srv.replicaPeers.Add(srv.replication, id, conns[i], 6380+i, newReplicaPeerStateForTest(id, conns[i]))
			}

			const writers, perWriter = 8, 2000
			var mu sync.Mutex
			ends := make(map[string]int64, writers*perWriter) // frame tag -> end offset counted for it
			var wg sync.WaitGroup
			for w := 0; w < writers; w++ {
				w := w
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; i < perWriter; i++ {
						tag := fmt.Sprintf("w%d-%d", w, i)
						frame := protocol.Array{Elements: []protocol.Value{
							protocol.BulkString{Data: []byte("SET")},
							protocol.BulkString{Data: []byte(tag)},
							// Sizes differ, so two frames queued out of order end at
							// positions other than their offsets.
							protocol.BulkString{Data: bytes.Repeat([]byte("x"), (w*37+i)%200)},
						}}
						report := srv.propagateToReplicas([]protocol.Value{frame})
						mu.Lock()
						ends[tag] = report.endOffset
						mu.Unlock()
					}
				}()
			}
			wg.Wait()

			// Stopping the feeds waits for each to write everything queued for it.
			if unfinished := srv.replicaPeers.StopFeeds(10 * time.Second); unfinished != 0 {
				t.Fatalf("%d replica feeds had not finished writing after 10s", unfinished)
			}

			counted := srv.replication.MasterOffset()
			for i, conn := range conns {
				stream := conn.Bytes()
				if int64(len(stream)) != counted {
					t.Fatalf("replica %d received %d bytes, want the %d the master counted", i+1, len(stream), counted)
				}
				frames, misplaced, first := framesAwayFromTheirOffsets(t, stream, ends)
				if frames != writers*perWriter {
					t.Fatalf("replica %d received %d frames, want %d", i+1, frames, writers*perWriter)
				}
				if misplaced > 0 {
					t.Errorf("replica %d: %d of %d frames end at a stream position other than the offset counted for them; first: %s", i+1, misplaced, frames, first)
				}
			}
		})
	}
}

// framesAwayFromTheirOffsets parses the stream a replica received and counts the
// frames that do not end at the offset ends records for their tag (the key of a
// SET frame).
func framesAwayFromTheirOffsets(t *testing.T, stream []byte, ends map[string]int64) (frames, misplaced int, first string) {
	t.Helper()

	parser := protocol.NewParser(bytes.NewReader(stream))
	var position int64
	for position < int64(len(stream)) {
		value, err := parser.Parse()
		if err != nil {
			t.Fatalf("Parse() at stream position %d: %v", position, err)
		}
		n, err := protocol.EncodedLen(value)
		if err != nil {
			t.Fatalf("EncodedLen() at stream position %d: %v", position, err)
		}
		position += int64(n)
		frames++

		tag := string(value.(protocol.Array).Elements[1].(protocol.BulkString).Data)
		if want := ends[tag]; want != position {
			if misplaced == 0 {
				first = fmt.Sprintf("%s ends at %d, counted to end at %d", tag, position, want)
			}
			misplaced++
		}
	}
	return frames, misplaced, first
}

// TestAReplicaThatRefusesAFrameIsDroppedOnce checks that a replica whose feed
// refuses a frame is dropped by one path, once: the feed-error handler, called
// after the registry lock is released so that it can take the lock as
// dropReplica does, or the registry's RemoveAndClose when no handler is set. The
// other replicas still receive the frame, and its offset is counted once.
func TestAReplicaThatRefusesAFrameIsDroppedOnce(t *testing.T) {
	tests := []struct {
		name        string
		withHandler bool
	}{
		{name: "through the feed-error handler", withHandler: true},
		{name: "by the registry when no handler is set", withHandler: false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			registry := NewReplicaRegistry()
			healthy := &recordingConn{}
			registry.Add(nil, 1, healthy, 6380, newReplicaPeerStateForTest(1, healthy))
			// A replica with no writer refuses every frame queued for it.
			refusing := &stubConn{}
			registry.Add(nil, 2, refusing, 6381, nil)

			var handled sync.Mutex
			var dropped []uint64
			if tt.withHandler {
				registry.SetFeedErrorHandler(func(id uint64, _ error) {
					handled.Lock()
					dropped = append(dropped, id)
					handled.Unlock()
					// Takes the registry lock, as the server's handler does.
					registry.Remove(id)
				})
			}

			offsets := &ReplicationState{}
			payload := []byte("*1\r\n$4\r\nPING\r\n")
			for round := 1; round <= 2; round++ {
				type outcome struct {
					end             int64
					queued, refused int
				}
				done := make(chan outcome, 1)
				go func() {
					end, queued, refused := registry.Propagate(offsets, payload)
					done <- outcome{end, queued, refused}
				}()

				var got outcome
				select {
				case got = <-done:
				case <-time.After(5 * time.Second):
					t.Fatalf("round %d: Propagate did not return; dropping the replica waited for the registry lock", round)
				}

				want := outcome{end: int64(round * len(payload)), queued: 1}
				if round == 1 {
					want.refused = 1
				}
				if got != want {
					t.Fatalf("round %d: Propagate() = %+v, want %+v", round, got, want)
				}
			}

			if got := registry.Count(); got != 1 {
				t.Fatalf("Count() = %d, want 1: the refusing replica is still registered", got)
			}
			handled.Lock()
			gotDropped := append([]uint64(nil), dropped...)
			handled.Unlock()
			wantDropped, wantCloses := []uint64{2}, int32(1)
			if tt.withHandler {
				// Closing the socket is the handler's job; the registry must not
				// drop the replica a second way.
				wantCloses = 0
			} else {
				wantDropped = nil
			}
			if fmt.Sprint(gotDropped) != fmt.Sprint(wantDropped) {
				t.Fatalf("handler dropped %v, want %v", gotDropped, wantDropped)
			}
			if got := refusing.closes.Load(); got != wantCloses {
				t.Fatalf("refusing replica's connection closed %d times, want %d", got, wantCloses)
			}

			if unfinished := registry.StopFeeds(5 * time.Second); unfinished != 0 {
				t.Fatalf("%d replica feeds had not finished writing after 5s", unfinished)
			}
			if got, want := healthy.Bytes(), bytes.Repeat(payload, 2); !bytes.Equal(got, want) {
				t.Fatalf("healthy replica received %q, want %q", got, want)
			}
		})
	}
}

// TestAReplicaIsSentNoFrameAfterOneItRefused has a replica's backlog refuse a
// frame, and then propagates a smaller frame that would still fit before the
// replica is dropped. Queueing it would put it at the stream position of the
// refused frame instead of at the offset counted for it, so it must be refused too.
func TestAReplicaIsSentNoFrameAfterOneItRefused(t *testing.T) {
	registry := NewReplicaRegistry()
	conn := &recordingConn{}
	registry.Add(nil, 1, conn, 6380, newReplicaPeerStateForTest(1, conn))
	// A backlog limit the first frame passes and the second does not.
	registry.mu.RLock()
	feed := registry.replicas[1].feed
	registry.mu.RUnlock()
	feed.mu.Lock()
	feed.limit = 16
	feed.mu.Unlock()

	// The handler keeps the replica registered, as it is between a refusal and
	// the drop that follows it.
	registry.SetFeedErrorHandler(func(uint64, error) {})

	offsets := &ReplicationState{}
	large := []byte("*1\r\n$20\r\nxxxxxxxxxxxxxxxxxxxx\r\n")
	small := []byte("*1\r\n$4\r\nPING\r\n")
	for _, payload := range [][]byte{large, small} {
		if _, queued, refused := registry.Propagate(offsets, payload); queued != 0 || refused != 1 {
			t.Fatalf("Propagate(%q) queued %d and refused %d, want 0 and 1", payload, queued, refused)
		}
	}

	if got, want := offsets.MasterOffset(), int64(len(large)+len(small)); got != want {
		t.Fatalf("MasterOffset() = %d, want %d", got, want)
	}
	if unfinished := registry.StopFeeds(5 * time.Second); unfinished != 0 {
		t.Fatalf("%d replica feeds had not finished writing after 5s", unfinished)
	}
	if got := conn.Bytes(); len(got) != 0 {
		t.Fatalf("replica received %q after refusing a frame, want nothing", got)
	}
}

// TestAReplicaCountsFromTheOffsetItAttachedAt registers a replica after 100 bytes
// of writes it is never sent. It acknowledges what it processed counting from
// zero, so WAIT places it at the master offset it attached at plus what it
// acknowledged; compared raw, it stayed 100 bytes short of every later write.
// Before its first acknowledgement it counts only for a target of 0, as in Redis.
func TestAReplicaCountsFromTheOffsetItAttachedAt(t *testing.T) {
	tests := []struct {
		name         string
		acknowledged int64 // 0 sends no acknowledgement
		target       int64
		want         int
	}{
		{name: "before acknowledging it counts for a target of 0", target: 0, want: 1},
		{name: "before acknowledging it does not count for a target of 1", target: 1, want: 0},
		{name: "it counts at the offset it attached at plus what it acknowledged", acknowledged: 31, target: 131, want: 1},
		{name: "it does not count past the offset it attached at plus what it acknowledged", acknowledged: 31, target: 132, want: 0},
		{name: "an acknowledgement past the largest offset does not wrap negative", acknowledged: math.MaxInt64, target: math.MaxInt64, want: 1},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			offsets := &ReplicationState{}
			offsets.AdvanceMasterOffset(100)
			registry := NewReplicaRegistry()
			conn := &stubConn{}
			registry.Add(offsets, 1, conn, 6380, newReplicaPeerStateForTest(1, conn))
			defer registry.StopFeeds(time.Second)

			if tt.acknowledged > 0 && !registry.UpdateAck(1, tt.acknowledged) {
				t.Fatal("UpdateAck() = false, want true")
			}
			if got := registry.CountReplicasAtOrAbove(tt.target); got != tt.want {
				t.Fatalf("CountReplicasAtOrAbove(%d) = %d, want %d", tt.target, got, tt.want)
			}
			if got, _ := registry.CountReplicasAtOrAboveWithNotify(tt.target); got != tt.want {
				t.Fatalf("CountReplicasAtOrAboveWithNotify(%d) = %d, want %d", tt.target, got, tt.want)
			}
		})
	}
}

// TestAReplicaAttachedWhileWritesArePropagatedCountsAtTheMasterOffset registers
// replicas while writers propagate frames that the sequencer does not order, as
// the expiry DELs are, and has each replica acknowledge everything it was sent.
// That must count each at the master's offset and not past it: a replica's base
// is read under the lock Propagate counts and queues under. Read outside that
// lock, a frame counted in between is neither sent nor below the base, and the
// replica falls short of every later write; or one is both, and it counts for a
// write it has not acknowledged. Each replica attaches at a different moment,
// since one attach does not always land between two frames.
//
// The replicas register as PSYNC registers them, through BeginAttach, Reserve and
// Start, but outside an attach cut, which is the command sequencer's. A client
// write skips the registry lock while no replica is registered and relies on the
// cut instead, so it is covered where the cut is, by
// TestEveryFrameCountedWhileAReplicaAttachesIsBelowItsBaseOrSentToIt in the
// command package.
func TestAReplicaAttachedWhileWritesArePropagatedCountsAtTheMasterOffset(t *testing.T) {
	srv := newTestServer(t, config.Config{}, slog.New(slog.NewTextHandler(io.Discard, nil)), storage.NewStore(), nil)
	frame := []protocol.Value{DeleteFrame([]string{"key"})}

	const writers = 8
	var propagated atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				srv.propagateUnorderedToReplicas(frame)
				propagated.Add(1)
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
	waitForPropagated := func(n int64) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for propagated.Load() < n {
			if time.Now().After(deadline) {
				t.Fatalf("%d frames propagated after 10s, want %d", propagated.Load(), n)
			}
			runtime.Gosched()
		}
	}

	// Each replica attaches after frames were counted without it, and frames are
	// counted after it, so both sides of its base are exercised.
	const replicas = 5
	conns := make([]*recordingConn, replicas)
	for i := range conns {
		waitForPropagated(propagated.Load() + 500)
		id := uint64(i + 1)
		conns[i] = &recordingConn{}
		srv.replicaPeers.BeginAttach()
		srv.replicaPeers.Reserve(srv.replication, id, 6380+i, newReplicaPeerStateForTest(id, conns[i]))
		if !srv.replicaPeers.Start(id, conns[i]) {
			t.Fatalf("Start(%d) = false, want true", id)
		}
	}
	waitForPropagated(propagated.Load() + 500)
	stopWriters()

	// Stopping the feeds waits for each to write everything queued for it.
	if unfinished := srv.replicaPeers.StopFeeds(10 * time.Second); unfinished != 0 {
		t.Fatalf("%d replica feeds had not finished writing after 10s", unfinished)
	}
	for i, conn := range conns {
		if !srv.replicaPeers.UpdateAck(uint64(i+1), int64(len(conn.Bytes()))) {
			t.Fatalf("UpdateAck(%d) = false, want true", i+1)
		}
	}

	counted := srv.replication.MasterOffset()
	if got := srv.replicaPeers.CountReplicasAtOrAbove(counted); got != replicas {
		t.Errorf("CountReplicasAtOrAbove(%d) = %d, want %d", counted, got, replicas)
	}
	if got := srv.replicaPeers.CountReplicasAtOrAbove(counted + 1); got != 0 {
		t.Errorf("CountReplicasAtOrAbove(%d) = %d, want 0", counted+1, got)
	}
	if t.Failed() {
		for _, peer := range srv.replicaPeers.Snapshot() {
			t.Logf("replica %d attached at %d and acknowledged %d, the master counted %d", peer.ID, peer.baseOffset, peer.AckOffset.Load(), counted)
		}
	}
}

// TestWithNoReplicaRegisteredOnlyClientWritesSkipTheRegistryLock holds the
// registry lock, as a replica registering holds it while it reads its base, and
// propagates a frame with no replica registered. A client write is counted
// without waiting: the sequencer orders it against the attach cut instead. An
// expiry DEL must wait for the lock: a replica's full resync publishes expiries
// holding no sequencer lock, so nothing else orders the DEL against the
// registration, and counted without the lock it could lie above the new
// replica's base without being sent to it.
func TestWithNoReplicaRegisteredOnlyClientWritesSkipTheRegistryLock(t *testing.T) {
	tests := []struct {
		name      string
		propagate func(srv *Server)
		takesLock bool
	}{
		{name: "a client write", propagate: func(srv *Server) {
			srv.propagateToReplicas([]protocol.Value{protocol.Array{Elements: []protocol.Value{
				protocol.BulkString{Data: []byte("SET")},
				protocol.BulkString{Data: []byte("key")},
				protocol.BulkString{Data: []byte("value")},
			}}})
		}},
		{name: "an expiry DEL", propagate: func(srv *Server) { srv.recordExpiredKeys([]string{"key"}) }, takesLock: true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			srv := newTestServer(t, config.Config{}, slog.New(slog.NewTextHandler(io.Discard, nil)), storage.NewStore(), nil)
			srv.replicaPeers.mu.Lock()
			locked := true
			unlock := func() {
				if locked {
					locked = false
					srv.replicaPeers.mu.Unlock()
				}
			}
			defer unlock()
			done := make(chan struct{})
			go func() {
				defer close(done)
				tt.propagate(srv)
			}()

			if tt.takesLock {
				select {
				case <-done:
					t.Fatal("the frame was counted while the registry lock was held")
				case <-time.After(50 * time.Millisecond):
				}
				if got := srv.replication.MasterOffset(); got != 0 {
					t.Fatalf("MasterOffset() = %d while the frame waits for the registry lock, want 0", got)
				}
				unlock()
			}
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("the frame was not counted within 5s")
			}
			if got := srv.replication.MasterOffset(); got == 0 {
				t.Fatal("MasterOffset() = 0 after the frame was propagated, want it counted")
			}
		})
	}
}

// TestAReplicaAttachesInThreeSteps walks a replica through BeginAttach, Reserve
// and Start, as PSYNC takes them, and checks what each step makes visible: the
// registry is active from BeginAttach, so writes are ordered; Reserve registers
// the replica at the master offset of that moment with a feed that queues every
// frame counted after it without writing; Start writes what was queued and what
// follows, in order.
func TestAReplicaAttachesInThreeSteps(t *testing.T) {
	offsets := &ReplicationState{}
	registry := NewReplicaRegistry()
	payload := []byte("*1\r\n$4\r\nPING\r\n")

	if registry.Active() {
		t.Fatal("Active() = true with no replica, want false")
	}
	registry.BeginAttach()
	if !registry.Active() {
		t.Fatal("Active() = false after BeginAttach, want true: writes must be ordered from then on")
	}
	registry.Propagate(offsets, payload) // before the cut: in the snapshot, not the stream

	conn := &recordingConn{}
	registry.Reserve(offsets, 1, 6380, newReplicaPeerStateForTest(1, conn))
	if !registry.Active() || registry.Count() != 1 {
		t.Fatalf("after Reserve: Active() = %v and Count() = %d, want true and 1", registry.Active(), registry.Count())
	}
	if _, queued, _ := registry.Propagate(offsets, payload); queued != 1 {
		t.Fatalf("Propagate() during the transfer queued for %d replicas, want 1", queued)
	}
	// The feed holds the frame: it has no flusher yet to take it.
	feed := peerFeed(registry, 1)
	if feedStarted(feed) || feed.backlog() != len(payload) {
		t.Fatalf("before Start: the feed started = %v with %d bytes waiting, want not started with %d", feedStarted(feed), feed.backlog(), len(payload))
	}
	if got := conn.Bytes(); len(got) != 0 {
		t.Fatalf("the replica was sent %q before its feed started", got)
	}

	if !registry.Start(1, conn) {
		t.Fatal("Start() = false for a reserved replica, want true")
	}
	registry.Propagate(offsets, payload)
	if unfinished := registry.StopFeeds(5 * time.Second); unfinished != 0 {
		t.Fatalf("%d replica feeds had not finished writing after 5s", unfinished)
	}
	if got, want := conn.Bytes(), bytes.Repeat(payload, 2); !bytes.Equal(got, want) {
		t.Fatalf("the replica received %q, want the two frames counted after its base, %q", got, want)
	}

	// Acknowledging both frames puts it at the master's offset, since its base
	// is the frame counted before Reserve.
	registry.UpdateAck(1, int64(2*len(payload)))
	if got := registry.CountReplicasAtOrAbove(offsets.MasterOffset()); got != 1 {
		t.Fatalf("CountReplicasAtOrAbove(%d) = %d, want 1", offsets.MasterOffset(), got)
	}
	if got := registry.CountReplicasAtOrAbove(offsets.MasterOffset() + 1); got != 0 {
		t.Fatalf("CountReplicasAtOrAbove(%d) = %d, want 0", offsets.MasterOffset()+1, got)
	}

	registry.Remove(1)
	if registry.Active() {
		t.Fatal("Active() = true after the only replica was removed, want false")
	}
}

// TestAnAttachThatEndsWithoutAReplicaLeavesTheRegistryInactive has an attach
// end without Reserve, as a PSYNC that fails inside its cut does, and checks
// that the registry stops counting it, so writes stop being ordered for it.
func TestAnAttachThatEndsWithoutAReplicaLeavesTheRegistryInactive(t *testing.T) {
	registry := NewReplicaRegistry()
	registry.BeginAttach()
	registry.BeginAttach()
	registry.EndAttach()
	if !registry.Active() {
		t.Fatal("Active() = false with one attach still open, want true")
	}
	registry.EndAttach()
	if registry.Active() || registry.Count() != 0 {
		t.Fatalf("Active() = %v and Count() = %d after both attaches ended, want false and 0", registry.Active(), registry.Count())
	}
}

// TestAReplicaDroppedWhileItsFullResyncIsSentIsNeverStarted drops a reserved
// replica before its feed starts, as a disconnect or a backlog past the limit
// during the transfer does. The drop paths must cope with a replica that has no
// connection yet, Start must then report false so that the caller closes the
// connection, the feed must never write, and stopping feeds must not wait for it.
func TestAReplicaDroppedWhileItsFullResyncIsSentIsNeverStarted(t *testing.T) {
	tests := []struct {
		name string
		drop func(srv *Server)
	}{
		{name: "removed when the client disconnects", drop: func(srv *Server) { srv.replicaPeers.Remove(1) }},
		{name: "removed and closed by the registry", drop: func(srv *Server) {
			if err := srv.replicaPeers.RemoveAndClose(1); err != nil {
				t.Fatalf("RemoveAndClose() error = %v", err)
			}
		}},
		{name: "dropped for a backlog past the limit", drop: func(srv *Server) {
			srv.replicaPeers.mu.RLock()
			feed := srv.replicaPeers.replicas[1].feed
			srv.replicaPeers.mu.RUnlock()
			feed.mu.Lock()
			feed.limit = 4
			feed.mu.Unlock()
			// Refused by the feed, so dropped through dropReplica.
			srv.propagateToReplicas([]protocol.Value{protocol.Array{Elements: []protocol.Value{protocol.BulkString{Data: []byte("PING")}}}})
		}},
		{name: "stopped by a shutdown", drop: func(srv *Server) {
			done := make(chan int, 1)
			go func() { done <- srv.replicaPeers.StopFeeds(time.Minute) }()
			select {
			case unfinished := <-done:
				if unfinished != 0 {
					t.Fatalf("StopFeeds() = %d unfinished, want 0: a feed that never started has nothing to finish", unfinished)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("StopFeeds() waited for a feed that never started")
			}
		}},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			srv := newTestServer(t, config.Config{}, slog.New(slog.NewTextHandler(io.Discard, nil)), storage.NewStore(), nil)
			conn := &stubConn{}
			written := &recordingConn{}
			srv.replicaPeers.BeginAttach()
			srv.replicaPeers.Reserve(srv.replication, 1, 6380, newReplicaPeerStateForTest(1, written))

			tt.drop(srv)

			srv.registerReplicaPeer(1, conn)
			if got := conn.closes.Load(); got != 1 {
				t.Fatalf("the connection was closed %d times, want once: a dropped replica's reply is followed by a close", got)
			}
			if srv.replicaPeers.Start(1, conn) {
				t.Fatal("Start() = true for a dropped replica, want false")
			}
			srv.replicaPeers.StopFeeds(time.Second)
			if got := written.Bytes(); len(got) != 0 {
				t.Fatalf("a dropped replica was sent %q", got)
			}
		})
	}
}

// TestAConnectionIsMarkedAReplicaBeforeItsFeedStarts starts the feed of a
// replica whose connection, like the event loop's, holds waiting output to a
// limit that depends on being a replica. The connection must be marked before
// the feed starts: the feed may push what it queued during the full resync at
// once, and a connection not yet marked would hold it to a subscriber's 32 MiB
// instead of a replica's 256 MiB.
func TestAConnectionIsMarkedAReplicaBeforeItsFeedStarts(t *testing.T) {
	srv := newTestServer(t, config.Config{}, slog.New(slog.NewTextHandler(io.Discard, nil)), storage.NewStore(), nil)
	srv.replicaPeers.BeginAttach()
	srv.replicaPeers.Reserve(srv.replication, 1, 6380, newReplicaPeerStateForTest(1, &recordingConn{}))
	defer srv.replicaPeers.StopFeeds(time.Second)
	feed := peerFeed(srv.replicaPeers, 1)

	conn := &markedConn{}
	conn.onMark = func() { conn.feedStartedWhenMarked = feedStarted(feed) }
	srv.registerReplicaPeer(1, conn)

	if conn.marks != 1 {
		t.Fatalf("the connection was marked a replica %d times, want once", conn.marks)
	}
	if conn.feedStartedWhenMarked {
		t.Fatal("the connection was marked a replica after its feed started")
	}
	if !feedStarted(feed) {
		t.Fatal("the feed did not start")
	}
}

// markedConn is a connection that, like the event loop's, is told when it
// becomes a replica.
type markedConn struct {
	stubConn
	marks                 int
	onMark                func()
	feedStartedWhenMarked bool
}

func (c *markedConn) markReplica() {
	c.marks++
	c.onMark()
}

// peerFeed returns the feed of the registered replica id.
func peerFeed(registry *ReplicaRegistry, id uint64) *replicaFeed {
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	return registry.replicas[id].feed
}

func feedStarted(feed *replicaFeed) bool {
	feed.mu.Lock()
	defer feed.mu.Unlock()
	return feed.started
}

type stubConn struct {
	writeErr error
	closeErr error
	closes   atomic.Int32
}

type recordingConn struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (c *stubConn) Read(_ []byte) (int, error) { return 0, io.EOF }
func (c *stubConn) Write(p []byte) (int, error) {
	if c.writeErr != nil {
		return 0, c.writeErr
	}
	return len(p), nil
}
func (c *stubConn) Close() error {
	c.closes.Add(1)
	return c.closeErr
}
func (c *stubConn) LocalAddr() net.Addr                { return stubAddr("local") }
func (c *stubConn) RemoteAddr() net.Addr               { return stubAddr("remote") }
func (c *stubConn) SetDeadline(_ time.Time) error      { return nil }
func (c *stubConn) SetReadDeadline(_ time.Time) error  { return nil }
func (c *stubConn) SetWriteDeadline(_ time.Time) error { return nil }

func (c *recordingConn) Read(_ []byte) (int, error) { return 0, io.EOF }
func (c *recordingConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.Write(p)
}
func (c *recordingConn) Close() error                       { return nil }
func (c *recordingConn) LocalAddr() net.Addr                { return stubAddr("local") }
func (c *recordingConn) RemoteAddr() net.Addr               { return stubAddr("remote") }
func (c *recordingConn) SetDeadline(_ time.Time) error      { return nil }
func (c *recordingConn) SetReadDeadline(_ time.Time) error  { return nil }
func (c *recordingConn) SetWriteDeadline(_ time.Time) error { return nil }

func (c *recordingConn) Bytes() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return bytes.Clone(c.buf.Bytes())
}

type stubAddr string

func (a stubAddr) Network() string { return "tcp" }
func (a stubAddr) String() string  { return string(a) }

func newReplicaPeerStateForTest(id uint64, conn net.Conn) *ClientState {
	state := &ClientState{ID: id, Authenticated: true}
	state.PromoteToReplica()
	state.BindResponseWriter(bufio.NewWriter(conn))
	return state
}

// TestServerStatsConcurrentWithAckUpdates has INFO's data source read replica
// offsets while acknowledgements arrive. ServerStats used to read AckOffset from
// the shared peer without the registry lock that UpdateAck writes it under; the
// race detector reports that, so this test is meaningful under -race.
func TestServerStatsConcurrentWithAckUpdates(t *testing.T) {
	srv := newTestServer(t, config.Default(), slog.New(slog.NewTextHandler(io.Discard, nil)), storage.NewStore(), stubExecutor{})
	srv.replicaPeers.Add(srv.replication, 1, &stubConn{}, 6380, nil)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for offset := int64(1); ; offset++ {
			select {
			case <-stop:
				return
			default:
				srv.replicaPeers.UpdateAck(1, offset)
			}
		}
	}()

	var last int64
	for i := 0; i < 2000; i++ {
		stats := srv.ServerStats()
		if len(stats.Replicas) != 1 {
			t.Fatalf("len(Replicas) = %d, want 1", len(stats.Replicas))
		}
		if got := stats.Replicas[0].AckedMasterOffset; got < last {
			t.Fatalf("AckedMasterOffset went backwards from %d to %d", last, got)
		} else {
			last = got
		}
	}
	close(stop)
	wg.Wait()
}
