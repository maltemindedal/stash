package replication

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/rdb"
	"github.com/maltemindedal/stash/internal/storage"
)

func encodeFrame(t testing.TB, frame protocol.Value) []byte {
	t.Helper()
	payload, err := protocol.Encode(frame)
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	return payload
}

func TestAnAcknowledgementWakesWhoeverWaitsForIt(t *testing.T) {
	master := newTestMaster()
	serverConn, replicaConn := net.Pipe()
	defer func() { _ = serverConn.Close() }()
	defer func() { _ = replicaConn.Close() }()

	master.Add(Candidate{ID: 1, ListeningPort: 6380, Out: outTo(serverConn)}, serverConn)

	count, changed := master.ackedWithNotify(10)
	if count != 0 {
		t.Fatalf("initial count = %d, want 0", count)
	}

	if ok := master.Ack(1, 10); !ok {
		t.Fatal("Ack() = false, want true")
	}

	select {
	case <-changed:
	case <-time.After(time.Second):
		t.Fatal("ackedWithNotify() channel was not notified")
	}

	count, _ = master.ackedWithNotify(10)
	if count != 1 {
		t.Fatalf("updated count = %d, want 1", count)
	}
}

func TestDetachingAndClosingAReplicaReturnsItsCloseError(t *testing.T) {
	master := newTestMaster()
	conn := &closeCounter{closeErr: errors.New("close boom")}
	master.Add(Candidate{ID: 1, ListeningPort: 6380, Out: outTo(conn)}, conn)

	err := master.detachAndClose(1)
	if err == nil || err.Error() != "close boom" {
		t.Fatalf("detachAndClose() error = %v, want close boom", err)
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
			master := newTestMaster()
			conns := make([]*streamRecorder, tt.replicas)
			for i := range conns {
				id := uint64(i + 1)
				conns[i] = &streamRecorder{}
				master.Add(Candidate{ID: id, ListeningPort: 6380 + i, Out: outTo(conns[i])}, conns[i])
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
						end := master.Propagate(encodeFrame(t, frame), true)
						mu.Lock()
						ends[tag] = end
						mu.Unlock()
					}
				}()
			}
			wg.Wait()

			// Stopping the feeds waits for each to write everything queued for it.
			if unfinished := master.StopFeeds(10 * time.Second); unfinished != 0 {
				t.Fatalf("%d replica feeds had not finished writing after 10s", unfinished)
			}

			counted := master.Offset()
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
// refuses a frame is dropped by one path, once: the drop handler, called
// after the master lock is released so that it can take the lock as
// dropReplica does, or the master's detachAndClose when no handler is set. The
// other replicas still receive the frame, and its offset is counted once.
func TestAReplicaThatRefusesAFrameIsDroppedOnce(t *testing.T) {
	tests := []struct {
		name        string
		withHandler bool
	}{
		{name: "through the drop handler", withHandler: true},
		{name: "by the master when no handler is set", withHandler: false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			master := newTestMaster()
			healthy := &streamRecorder{}
			master.Add(Candidate{ID: 1, ListeningPort: 6380, Out: outTo(healthy)}, healthy)
			// A replica with no writer refuses every frame queued for it.
			refusing := &closeCounter{}
			master.Add(Candidate{ID: 2, ListeningPort: 6381, Out: nil}, refusing)

			var handled sync.Mutex
			var dropped []uint64
			if tt.withHandler {
				master.drop = func(id uint64, _ error) {
					handled.Lock()
					dropped = append(dropped, id)
					handled.Unlock()
					// Takes the master lock, as the server's handler does.
					master.Detach(id)
				}
			}
			payload := []byte("*1\r\n$4\r\nPING\r\n")
			for round := 1; round <= 2; round++ {
				type outcome struct {
					end             int64
					queued, refused int
				}
				done := make(chan outcome, 1)
				go func() {
					end, queued, refused := master.countAndQueue(payload)
					done <- outcome{end, queued, refused}
				}()

				var got outcome
				select {
				case got = <-done:
				case <-time.After(5 * time.Second):
					t.Fatalf("round %d: Propagate did not return; dropping the replica waited for the master lock", round)
				}

				want := outcome{end: int64(round * len(payload)), queued: 1}
				if round == 1 {
					want.refused = 1
				}
				if got != want {
					t.Fatalf("round %d: Propagate() = %+v, want %+v", round, got, want)
				}
			}

			if got := master.Count(); got != 1 {
				t.Fatalf("Count() = %d, want 1: the refusing replica is still registered", got)
			}
			handled.Lock()
			gotDropped := append([]uint64(nil), dropped...)
			handled.Unlock()
			wantDropped, wantCloses := []uint64{2}, int32(1)
			if tt.withHandler {
				// Closing the socket is the handler's job; the master must not
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

			if unfinished := master.StopFeeds(5 * time.Second); unfinished != 0 {
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
	master := newTestMaster()
	conn := &streamRecorder{}
	master.Add(Candidate{ID: 1, ListeningPort: 6380, Out: outTo(conn)}, conn)
	// A backlog limit the first frame passes and the second does not.
	master.mu.RLock()
	feed := master.replicas[1].feed
	master.mu.RUnlock()
	feed.mu.Lock()
	feed.limit = 16
	feed.mu.Unlock()

	// The handler keeps the replica registered, as it is between a refusal and
	// the drop that follows it.
	master.drop = func(uint64, error) {}
	large := []byte("*1\r\n$20\r\nxxxxxxxxxxxxxxxxxxxx\r\n")
	small := []byte("*1\r\n$4\r\nPING\r\n")
	for _, payload := range [][]byte{large, small} {
		if _, queued, refused := master.countAndQueue(payload); queued != 0 || refused != 1 {
			t.Fatalf("Propagate(%q) queued %d and refused %d, want 0 and 1", payload, queued, refused)
		}
	}

	if got, want := master.Offset(), int64(len(large)+len(small)); got != want {
		t.Fatalf("MasterOffset() = %d, want %d", got, want)
	}
	if unfinished := master.StopFeeds(5 * time.Second); unfinished != 0 {
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
			master := newTestMaster()
			master.offset.Add(100)
			conn := &closeCounter{}
			master.Add(Candidate{ID: 1, ListeningPort: 6380, Out: outTo(conn)}, conn)
			defer master.StopFeeds(time.Second)

			if tt.acknowledged > 0 && !master.Ack(1, tt.acknowledged) {
				t.Fatal("Ack() = false, want true")
			}
			if got := master.Acked(tt.target); got != tt.want {
				t.Fatalf("Acked(%d) = %d, want %d", tt.target, got, tt.want)
			}
			if got, _ := master.ackedWithNotify(tt.target); got != tt.want {
				t.Fatalf("ackedWithNotify(%d) = %d, want %d", tt.target, got, tt.want)
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
// The replicas register as PSYNC registers them, through beginAttach, reserve and
// Start, but outside an attach cut, which is the command sequencer's. A client
// write skips the master lock while no replica is registered and relies on the
// cut instead, so it is covered where the cut is, by
// TestEveryFrameCountedWhileAReplicaAttachesIsBelowItsBaseOrSentToIt in the
// command package.
func TestAReplicaAttachedWhileWritesArePropagatedCountsAtTheMasterOffset(t *testing.T) {
	master := newTestMaster()
	frame := []byte("*2\r\n$3\r\nDEL\r\n$3\r\nkey\r\n")

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
				master.Propagate(frame, false)
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
	conns := make([]*streamRecorder, replicas)
	for i := range conns {
		waitForPropagated(propagated.Load() + 500)
		id := uint64(i + 1)
		conns[i] = &streamRecorder{}
		master.beginAttach()
		master.reserve(Candidate{ID: id, ListeningPort: 6380 + i, Out: outTo(conns[i])})
		if !master.Start(id, conns[i]) {
			t.Fatalf("Start(%d) = false, want true", id)
		}
	}
	waitForPropagated(propagated.Load() + 500)
	stopWriters()

	// Stopping the feeds waits for each to write everything queued for it.
	if unfinished := master.StopFeeds(10 * time.Second); unfinished != 0 {
		t.Fatalf("%d replica feeds had not finished writing after 10s", unfinished)
	}
	for i, conn := range conns {
		if !master.Ack(uint64(i+1), int64(len(conn.Bytes()))) {
			t.Fatalf("Ack(%d) = false, want true", i+1)
		}
	}

	counted := master.Offset()
	if got := master.Acked(counted); got != replicas {
		t.Errorf("Acked(%d) = %d, want %d", counted, got, replicas)
	}
	if got := master.Acked(counted + 1); got != 0 {
		t.Errorf("Acked(%d) = %d, want 0", counted+1, got)
	}
	if t.Failed() {
		for _, peer := range master.snapshot() {
			t.Logf("replica %d attached at %d and acknowledged %d, the master counted %d", peer.ID, peer.base, peer.ack.Load(), counted)
		}
	}
}

// TestWithNoReplicaRegisteredOnlyOrderedFramesSkipTheMastersLock holds the
// Master's lock, as a Replica registering holds it while it reads its base, and
// propagates a frame with no Replica registered. An ordered frame, a client
// write's, is counted without waiting: the sequencer orders it against the
// attach cut instead. An unordered one, such as an expiry DEL, must wait for the
// lock: the Recorder passes the server's own writes as unordered, so nothing
// else is known to order the DEL against the registration, and counted without the
// lock it could lie above the new Replica's base without being sent to it.
func TestWithNoReplicaRegisteredOnlyOrderedFramesSkipTheMastersLock(t *testing.T) {
	tests := []struct {
		name    string
		ordered bool
	}{
		{name: "an ordered frame", ordered: true},
		{name: "an unordered frame"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			master := newTestMaster()
			master.mu.Lock()
			locked := true
			unlock := func() {
				if locked {
					locked = false
					master.mu.Unlock()
				}
			}
			defer unlock()
			done := make(chan struct{})
			go func() {
				defer close(done)
				master.Propagate([]byte("*1\r\n$4\r\nPING\r\n"), tt.ordered)
			}()

			if !tt.ordered {
				select {
				case <-done:
					t.Fatal("the frame was counted while the Master's lock was held")
				case <-time.After(50 * time.Millisecond):
				}
				if got := master.Offset(); got != 0 {
					t.Fatalf("Offset() = %d while the frame waits for the Master's lock, want 0", got)
				}
				unlock()
			}
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("the frame was not counted within 5s")
			}
			if got := master.Offset(); got == 0 {
				t.Fatal("Offset() = 0 after the frame was propagated, want it counted")
			}
		})
	}
}

// TestAReplicaAttachesInThreeSteps walks a replica through beginAttach, reserve
// and Start, as PSYNC takes them, and checks what each step makes visible: the
// master is active from beginAttach, so writes are ordered; reserve registers
// the replica at the master offset of that moment with a feed that queues every
// frame counted after it without writing; Start writes what was queued and what
// follows, in order.
func TestAReplicaAttachesInThreeSteps(t *testing.T) {
	master := newTestMaster()
	payload := []byte("*1\r\n$4\r\nPING\r\n")

	if master.Active() {
		t.Fatal("Active() = true with no replica, want false")
	}
	master.beginAttach()
	if !master.Active() {
		t.Fatal("Active() = false after beginAttach, want true: writes must be ordered from then on")
	}
	master.countAndQueue(payload) // before the cut: in the snapshot, not the stream

	conn := &streamRecorder{}
	master.reserve(Candidate{ID: 1, ListeningPort: 6380, Out: outTo(conn)})
	if !master.Active() || master.Count() != 1 {
		t.Fatalf("after reserve: Active() = %v and Count() = %d, want true and 1", master.Active(), master.Count())
	}
	if _, queued, _ := master.countAndQueue(payload); queued != 1 {
		t.Fatalf("Propagate() during the transfer queued for %d replicas, want 1", queued)
	}
	// The feed holds the frame: it has no flusher yet to take it.
	feed := peerFeed(master, 1)
	if feedStarted(feed) || feed.backlog() != len(payload) {
		t.Fatalf("before Start: the feed started = %v with %d bytes waiting, want not started with %d", feedStarted(feed), feed.backlog(), len(payload))
	}
	if got := conn.Bytes(); len(got) != 0 {
		t.Fatalf("the replica was sent %q before its feed started", got)
	}

	if !master.Start(1, conn) {
		t.Fatal("Start() = false for a reserved replica, want true")
	}
	master.countAndQueue(payload)
	if unfinished := master.StopFeeds(5 * time.Second); unfinished != 0 {
		t.Fatalf("%d replica feeds had not finished writing after 5s", unfinished)
	}
	if got, want := conn.Bytes(), bytes.Repeat(payload, 2); !bytes.Equal(got, want) {
		t.Fatalf("the replica received %q, want the two frames counted after its base, %q", got, want)
	}

	// Acknowledging both frames puts it at the master's offset, since its base
	// is the frame counted before reserve.
	master.Ack(1, int64(2*len(payload)))
	if got := master.Acked(master.Offset()); got != 1 {
		t.Fatalf("Acked(%d) = %d, want 1", master.Offset(), got)
	}
	if got := master.Acked(master.Offset() + 1); got != 0 {
		t.Fatalf("Acked(%d) = %d, want 0", master.Offset()+1, got)
	}

	master.Detach(1)
	if master.Active() {
		t.Fatal("Active() = true after the only replica was removed, want false")
	}
}

// TestAnAttachThatEndsWithoutAReplicaLeavesTheMasterInactive has an attach
// end without reserve, as a PSYNC that fails inside its cut does, and checks
// that the master stops counting it, so writes stop being ordered for it.
func TestAnAttachThatEndsWithoutAReplicaLeavesTheMasterInactive(t *testing.T) {
	master := newTestMaster()
	master.beginAttach()
	master.beginAttach()
	master.endAttach()
	if !master.Active() {
		t.Fatal("Active() = false with one attach still open, want true")
	}
	master.endAttach()
	if master.Active() || master.Count() != 0 {
		t.Fatalf("Active() = %v and Count() = %d after both attaches ended, want false and 0", master.Active(), master.Count())
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
		drop func(t *testing.T, master *Master)
	}{
		{name: "detached when the client disconnects", drop: func(_ *testing.T, master *Master) { master.Detach(1) }},
		{name: "detached and closed by the Master", drop: func(t *testing.T, master *Master) {
			if err := master.detachAndClose(1); err != nil {
				t.Fatalf("detachAndClose() error = %v", err)
			}
		}},
		{name: "dropped for a backlog past the limit", drop: func(_ *testing.T, master *Master) {
			feed := peerFeed(master, 1)
			feed.mu.Lock()
			feed.limit = 4
			feed.mu.Unlock()
			// Refused by the feed, so the Master drops the Replica.
			master.Propagate([]byte("*1\r\n$4\r\nPING\r\n"), true)
		}},
		{name: "stopped by a shutdown", drop: func(t *testing.T, master *Master) {
			done := make(chan int, 1)
			go func() { done <- master.StopFeeds(time.Minute) }()
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
			master := newTestMaster()
			conn := &closeCounter{}
			written := &streamRecorder{}
			master.beginAttach()
			master.reserve(Candidate{ID: 1, ListeningPort: 6380, Out: outTo(written)})

			tt.drop(t, master)

			if master.Start(1, conn) {
				t.Fatal("Start() = true for a dropped replica, want false")
			}
			if got := conn.closes.Load(); got != 0 {
				t.Fatalf("the Master closed the connection %d times, want never: it was not the Master's yet, and Start's caller closes it", got)
			}
			master.StopFeeds(time.Second)
			if got := written.Bytes(); len(got) != 0 {
				t.Fatalf("a dropped replica was sent %q", got)
			}
		})
	}
}

// TestAFrameCountedWhileShutdownDrainsAFeedDoesNotDropItsReplica propagates a
// frame while StopFeeds waits for a replica that is slow to read to take what is
// queued for it. The frame reaches no replica, and the replica keeps its socket
// until it has taken everything queued before the shutdown. Its feed, closed for
// the drain, used to refuse the frame, and the refusal dropped the replica and
// closed its socket in the middle of the drain.
func TestAFrameCountedWhileShutdownDrainsAFeedDoesNotDropItsReplica(t *testing.T) {
	master := newTestMaster()
	var drops atomic.Int32
	master.drop = (func(uint64, error) { drops.Add(1) })

	serverConn, replicaConn := net.Pipe()
	defer func() { _ = serverConn.Close() }()
	defer func() { _ = replicaConn.Close() }()
	master.Add(Candidate{ID: 1, ListeningPort: 6380, Out: outTo(serverConn)}, serverConn)

	// The replica reads nothing yet, so the feed's write waits on the pipe.
	queued := bytes.Repeat([]byte("q"), 1<<10)
	if _, queuedFor, _ := master.countAndQueue(queued); queuedFor != 1 {
		t.Fatalf("Propagate() before the shutdown queued for %d replicas, want 1", queuedFor)
	}

	stopped := make(chan int, 1)
	go func() { stopped <- master.StopFeeds(10 * time.Second) }()
	master.mu.RLock()
	feed := master.replicas[1].feed
	master.mu.RUnlock()
	deadline := time.Now().Add(5 * time.Second)
	for {
		feed.mu.Lock()
		closed := feed.closed
		feed.mu.Unlock()
		if closed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("StopFeeds() had not closed the feed after 5s")
		}
		time.Sleep(time.Millisecond)
	}

	if _, queuedFor, refused := master.countAndQueue([]byte("during the drain")); queuedFor != 0 || refused != 0 {
		t.Fatalf("Propagate() during the drain: queued for %d replicas, refused by %d; want neither", queuedFor, refused)
	}

	received := make([]byte, len(queued))
	if _, err := io.ReadFull(replicaConn, received); err != nil {
		t.Fatalf("reading what was queued before the shutdown: %v", err)
	}
	if !bytes.Equal(received, queued) {
		t.Fatalf("the replica received %q, want what was queued before the shutdown", received)
	}
	if unfinished := <-stopped; unfinished != 0 {
		t.Fatalf("StopFeeds() = %d unfinished, want 0", unfinished)
	}
	if got := drops.Load(); got != 0 {
		t.Fatalf("the replica was dropped %d times, want never", got)
	}
}

// halfClosingConn is a replica connection that records CloseWrite.
type halfClosingConn struct {
	closeCounter
	closeWrites atomic.Int32
}

func (c *halfClosingConn) CloseWrite() error {
	c.closeWrites.Add(1)
	return nil
}

// TestEndStreamsHalfClosesEachReplicaAndWaitsForItToLeave ends the streams of
// the replicas at shutdown. Each started replica's connection is half-closed,
// never closed, so the end of its stream still reaches it, and EndStreams waits
// until the replica has closed its own side, which removes it from the
// master, or until the deadline.
func TestEndStreamsHalfClosesEachReplicaAndWaitsForItToLeave(t *testing.T) {
	tests := []struct {
		name          string
		replicaCloses bool
		wantRemaining int
	}{
		{name: "the replica closes its side", replicaCloses: true, wantRemaining: 0},
		{name: "the replica does not close its side before the deadline", wantRemaining: 1},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			master := newTestMaster()
			conn := &halfClosingConn{}
			master.Add(Candidate{ID: 1, ListeningPort: 6380, Out: outTo(&streamRecorder{})}, conn)
			if tt.replicaCloses {
				// As the replica's connection handler does when it reads the
				// replica's close.
				go func() {
					for conn.closeWrites.Load() == 0 {
						time.Sleep(time.Millisecond)
					}
					master.Detach(1)
				}()
			}

			remaining := master.EndStreams(time.Now().Add(200 * time.Millisecond))
			if remaining != tt.wantRemaining {
				t.Fatalf("EndStreams() = %d remaining, want %d", remaining, tt.wantRemaining)
			}
			if got := conn.closeWrites.Load(); got != 1 {
				t.Fatalf("CloseWrite() called %d times, want once", got)
			}
			if got := conn.closes.Load(); got != 0 {
				t.Fatalf("Close() called %d times, want never: closing would reset a replica still acknowledging", got)
			}
			master.StopFeeds(time.Second)
		})
	}
}

// TestWaitFeedsWaitsForTheDropHandler stops a replica's feed with a failed
// write while the drop handler is held up. WaitFeeds must not return until
// the handler has, so that nothing a feed logs comes after ListenAndServe, which
// calls it, has returned. The feed's goroutine used to outlive the server.
func TestWaitFeedsWaitsForTheDropHandler(t *testing.T) {
	master := newTestMaster()
	handling := make(chan struct{})
	release := make(chan struct{})
	master.drop = func(uint64, error) {
		close(handling)
		<-release
	}
	conn := &closeCounter{writeErr: io.ErrClosedPipe}
	master.Add(Candidate{ID: 1, ListeningPort: 6380, Out: outTo(conn)}, conn)
	master.countAndQueue([]byte("frame"))
	<-handling

	waited := make(chan struct{})
	go func() {
		master.WaitFeeds()
		close(waited)
	}()
	select {
	case <-waited:
		t.Fatal("WaitFeeds() returned while the drop handler was still running")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	select {
	case <-waited:
	case <-time.After(5 * time.Second):
		t.Fatal("WaitFeeds() had not returned 5s after the drop handler did")
	}
}

// peerFeed returns the feed of the registered replica id.
func peerFeed(master *Master, id uint64) *replicaFeed {
	master.mu.RLock()
	defer master.mu.RUnlock()
	return master.replicas[id].feed
}

func feedStarted(feed *replicaFeed) bool {
	feed.mu.Lock()
	defer feed.mu.Unlock()
	return feed.started
}

func newTestMaster() *Master {
	return NewMaster(storage.NewStore(), nil, nil)
}

// connOutput is a Replica's Output that writes its stream to w.
type connOutput struct{ w io.Writer }

func (o connOutput) WriteEncoded(payload []byte) error {
	_, err := o.w.Write(payload)
	return err
}

func outTo(w io.Writer) Output { return connOutput{w: w} }

// closeCounter is a Replica's connection that counts how often it is closed,
// and whose writes and closes fail when told to.
type closeCounter struct {
	writeErr error
	closeErr error
	closes   atomic.Int32
}

func (c *closeCounter) Write(p []byte) (int, error) {
	if c.writeErr != nil {
		return 0, c.writeErr
	}
	return len(p), nil
}

func (c *closeCounter) Close() error {
	c.closes.Add(1)
	return c.closeErr
}

// streamRecorder is a Replica's connection that keeps the stream written to it.
type streamRecorder struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (c *streamRecorder) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.Write(p)
}

func (c *streamRecorder) Close() error { return nil }

func (c *streamRecorder) Bytes() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return bytes.Clone(c.buf.Bytes())
}

// TestAttachCopiesTheSnapshotAndRegistersTheReplicaInsideItsCut attaches a
// Replica to a Master whose Store holds a key. The cut is told to switch
// recording on before it takes its locks, the Replica is registered before the
// cut is released, and the Master's lock is free while the cut runs: the cut
// here asks the Master whether it is active, which takes that lock. The reply
// is FULLRESYNC with the Master's replication ID and an RDB snapshot that holds
// the key. A frame propagated after the cut is held by the feed until Start.
func TestAttachCopiesTheSnapshotAndRegistersTheReplicaInsideItsCut(t *testing.T) {
	store := storage.NewStore()
	if err := store.Set(nil, "key", []byte("value"), 0); err != nil {
		t.Fatalf("Set() error = %v", err)
	}
	master := NewMaster(store, nil, nil)
	stream := &streamRecorder{}

	var activeInCut, registeredAtRelease bool
	cut := func(startRecording func()) (release func()) {
		startRecording()
		activeInCut = master.Active()
		return func() { registeredAtRelease = master.Count() == 1 }
	}
	attachment := master.Attach(Candidate{ID: 1, ListeningPort: 6380, Out: outTo(stream)}, cut)

	if !activeInCut {
		t.Fatal("Active() = false inside the cut after startRecording, want true: writes must be ordered from then on")
	}
	if !registeredAtRelease {
		t.Fatal("the Replica was not registered when the cut was released")
	}

	reply := attachment.Reply()
	if len(reply) != 2 {
		t.Fatalf("Reply() holds %d values, want FULLRESYNC and the snapshot", len(reply))
	}
	if line, ok := reply[0].(protocol.SimpleString); !ok || line.Value != "FULLRESYNC "+master.ReplicationID()+" 0" {
		t.Fatalf("Reply()[0] = %#v, want FULLRESYNC %s 0", reply[0], master.ReplicationID())
	}
	snapshot, ok := reply[1].(protocol.BulkString)
	if !ok {
		t.Fatalf("Reply()[1] = %#v, want the snapshot as a bulk string", reply[1])
	}
	loaded := storage.NewStore()
	if _, err := rdb.LoadReader(bytes.NewReader(snapshot.Data), loaded); err != nil {
		t.Fatalf("loading the snapshot: %v", err)
	}
	if value, found, err := loaded.Get("key"); err != nil || !found || string(value) != "value" {
		t.Fatalf("the snapshot holds key = %q (found %v, error %v), want value", value, found, err)
	}

	frame := []byte("*1\r\n$4\r\nPING\r\n")
	master.Propagate(frame, true)
	if got := stream.Bytes(); len(got) != 0 {
		t.Fatalf("the Replica was sent %q before Start", got)
	}
	if !master.Start(1, stream) {
		t.Fatal("Start() = false for an attached Replica, want true")
	}
	if unfinished := master.StopFeeds(5 * time.Second); unfinished != 0 {
		t.Fatalf("%d feeds had not finished writing after 5s", unfinished)
	}
	if got := stream.Bytes(); !bytes.Equal(got, frame) {
		t.Fatalf("the Replica received %q, want the frame propagated after its cut, %q", got, frame)
	}
}

// TestAnAttachWhoseCopyPanicsLeavesTheMasterInactive has the snapshot copy
// panic inside the cut. The attach ends, so Active reports what it did before
// and writes stop being ordered for a Replica that never registered, and the
// cut's locks are released.
func TestAnAttachWhoseCopyPanicsLeavesTheMasterInactive(t *testing.T) {
	master := NewMaster(nil, nil, nil) // copying a nil Store panics
	released := false
	cut := func(startRecording func()) (release func()) {
		startRecording()
		return func() { released = true }
	}

	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("Attach() did not panic on a nil Store")
			}
		}()
		master.Attach(Candidate{ID: 1, Out: outTo(io.Discard)}, cut)
	}()

	if master.Active() || master.Count() != 0 {
		t.Fatalf("Active() = %v and Count() = %d after the attach failed, want false and 0", master.Active(), master.Count())
	}
	if !released {
		t.Fatal("the cut was not released")
	}
}

// TestAwaitAsksForAcknowledgementsAndWaitsForThem runs WAIT's wait. Await
// sends every Replica REPLCONF GETACK *, counted into the offset like a write,
// and returns once enough Replicas have acknowledged the target in the Master's
// count, or when its timeout passes or its context ends.
func TestAwaitAsksForAcknowledgementsAndWaitsForThem(t *testing.T) {
	const target = 100
	tests := []struct {
		name string
		want int64
		// acknowledge is what the Replica acknowledges once Await has asked, in
		// its own count; 0 acknowledges nothing.
		acknowledge  int64
		cancel       bool
		timeout      time.Duration
		wantAcked    int
		wantTimedOut bool
		wantErr      error
	}{
		{name: "the Replica acknowledges the target", want: 1, acknowledge: target, timeout: time.Minute, wantAcked: 1},
		{name: "the Replica acknowledges less than the target", want: 1, acknowledge: target - 1, timeout: 50 * time.Millisecond, wantTimedOut: true},
		{name: "more Replicas are wanted than are attached", want: 2, acknowledge: target, timeout: 50 * time.Millisecond, wantAcked: 1, wantTimedOut: true},
		{name: "the context ends first", want: 1, cancel: true, timeout: time.Minute, wantErr: context.Canceled},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			master := newTestMaster()
			asked := make(chan struct{})
			out := &notifyingOutput{wrote: asked}
			master.Add(Candidate{ID: 1, ListeningPort: 6380, Out: out}, &closeCounter{})
			defer master.StopFeeds(time.Second)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go func() {
				select {
				case <-asked:
				case <-time.After(10 * time.Second):
					return
				}
				if tt.acknowledge > 0 {
					master.Ack(1, tt.acknowledge)
				}
				if tt.cancel {
					cancel()
				}
			}()

			acked, timedOut, err := master.Await(ctx, tt.want, target, tt.timeout)
			if !errors.Is(err, tt.wantErr) || acked != tt.wantAcked || timedOut != tt.wantTimedOut {
				t.Fatalf("Await() = (%d, %v, %v), want (%d, %v, %v)", acked, timedOut, err, tt.wantAcked, tt.wantTimedOut, tt.wantErr)
			}
			if got := out.Bytes(); !bytes.Equal(got, getAckPayload) {
				t.Fatalf("the Replica was sent %q, want %q", got, getAckPayload)
			}
			if got := master.Offset(); got != int64(len(getAckPayload)) {
				t.Fatalf("Offset() = %d after Await, want the %d bytes of its GETACK", got, len(getAckPayload))
			}
		})
	}
}

// TestInfoReportsEachReplicaInTheMastersOffsets attaches a Replica after 100
// bytes of the stream and has it acknowledge 31 of its own. INFO reports it at
// 131, the Master's count, and at 0 before it acknowledges anything.
func TestInfoReportsEachReplicaInTheMastersOffsets(t *testing.T) {
	master := newTestMaster()
	master.offset.Add(100)
	master.Add(Candidate{ID: 7, ListeningPort: 6380, Out: outTo(io.Discard)}, &closeCounter{})
	defer master.StopFeeds(time.Second)

	info := master.Info()
	want := MasterInfo{ReplicationID: master.ReplicationID(), Offset: 100, Replicas: []ReplicaInfo{{ID: 7, ListeningPort: 6380}}}
	if fmt.Sprint(info) != fmt.Sprint(want) {
		t.Fatalf("Info() = %+v before an acknowledgement, want %+v", info, want)
	}

	master.Ack(7, 31)
	want.Replicas[0].AckedMasterOffset = 131
	if info := master.Info(); fmt.Sprint(info) != fmt.Sprint(want) {
		t.Fatalf("Info() = %+v after acknowledging 31, want %+v", info, want)
	}
}

// notifyingOutput is a Replica's Output that keeps its stream and reports its
// first write.
type notifyingOutput struct {
	streamRecorder
	once  sync.Once
	wrote chan struct{}
}

func (o *notifyingOutput) WriteEncoded(payload []byte) error {
	_, err := o.Write(payload)
	o.once.Do(func() { close(o.wrote) })
	return err
}
