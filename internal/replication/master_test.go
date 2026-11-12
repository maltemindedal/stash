package replication

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"runtime"
	"strings"
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
	master.Add(Candidate{ID: 1, ListeningPort: 6380, Out: &fakeOutput{}})

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
			outs := make([]*fakeOutput, tt.replicas)
			for i := range outs {
				outs[i] = &fakeOutput{}
				master.Add(Candidate{ID: uint64(i + 1), ListeningPort: 6380 + i, Out: outs[i]})
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

			counted := master.Offset()
			for i, out := range outs {
				stream := out.Bytes()
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

// TestAReplicaWhoseOutputRefusesAFrameIsDetachedAndPushedNothingMore has one
// Replica's Output refuse a large frame it would not refuse a smaller one
// after, as an Output with a byte limit would if it stayed open. The Master
// detaches the Replica before it counts another frame, so the smaller frame
// is not pushed to it: it would arrive at the stream position of the refused
// frame instead of at the offset counted for it. The dropped handler hears of
// the Replica once, with why its Output was closed, on the propagating
// goroutine after the Master's lock is released, and finds the Replica
// detached already. The other Replica receives both frames, and each offset is
// counted once.
func TestAReplicaWhoseOutputRefusesAFrameIsDetachedAndPushedNothingMore(t *testing.T) {
	large := []byte("*1\r\n$20\r\nxxxxxxxxxxxxxxxxxxxx\r\n")
	small := []byte("*1\r\n$4\r\nPING\r\n")
	cause := errors.New("closed by the test")

	type report struct {
		id            uint64
		listeningPort int
		cause         error
		// registered is how many Replicas the Master counted when the handler
		// ran, and found is whether Detach still found the reported one.
		registered int
		found      bool
	}
	tests := []struct {
		name        string
		withHandler bool
		wantReports []report
	}{
		{name: "the dropped handler is told once", withHandler: true, wantReports: []report{{id: 2, listeningPort: 6381, cause: cause, registered: 1}}},
		{name: "with no handler nobody is told"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			master := newTestMaster()
			healthy := &fakeOutput{}
			master.Add(Candidate{ID: 1, ListeningPort: 6380, Out: healthy})
			refusing := &fakeOutput{refuse: func(frame []byte) error {
				if len(frame) > len(small) {
					return cause
				}
				return nil
			}}
			master.Add(Candidate{ID: 2, ListeningPort: 6381, Out: refusing})

			// Appended without a lock: the handler runs on this goroutine.
			var reports []report
			if tt.withHandler {
				master.dropped = func(replica *Replica, cause error) {
					reports = append(reports, report{
						id: replica.ID, listeningPort: replica.ListeningPort, cause: cause,
						// Both take the Master's lock, as a handler may.
						registered: master.Count(), found: master.Detach(replica.ID) != nil,
					})
				}
			}

			type outcome struct {
				end             int64
				queued, refused int
			}
			steps := []struct {
				frame []byte
				want  outcome
			}{
				{frame: large, want: outcome{end: int64(len(large)), queued: 1, refused: 1}},
				{frame: small, want: outcome{end: int64(len(large) + len(small)), queued: 1}},
			}
			for _, step := range steps {
				var got outcome
				got.end, got.queued, got.refused = master.countAndQueue(step.frame)
				if got != step.want {
					t.Fatalf("propagating %q = %+v, want %+v", step.frame, got, step.want)
				}
			}

			if got := master.Count(); got != 1 {
				t.Fatalf("Count() = %d, want 1: the refusing Replica is still registered", got)
			}
			if got := refusing.Bytes(); len(got) != 0 {
				t.Fatalf("the Replica was pushed %q after the frame it refused, want nothing", got)
			}
			if got, want := healthy.Bytes(), append(bytes.Clone(large), small...); !bytes.Equal(got, want) {
				t.Fatalf("the healthy Replica received %q, want %q", got, want)
			}
			if fmt.Sprint(reports) != fmt.Sprint(tt.wantReports) {
				t.Fatalf("the dropped handler was told %+v, want %+v", reports, tt.wantReports)
			}
		})
	}
}

// TestWritersThatRaceARefusalPushNothingToItsReplicaAfterIt has eight writers
// propagate to 200 Replicas whose Outputs each refuse one frame, a different
// one for each, and would take every frame after it. The Master detaches a
// Replica under the same hold of its lock as the push that was refused, so no
// writer finds it registered afterwards and each Replica holds exactly the
// frames before the one it refused. Detached once that lock was released, as
// a handler that takes the lock would do it, a writer waiting for the lock
// gets in first for some of the 200.
func TestWritersThatRaceARefusalPushNothingToItsReplicaAfterIt(t *testing.T) {
	master := newTestMaster()
	frame := []byte("*1\r\n$4\r\nPING\r\n")
	cause := errors.New("closed by the test")

	const replicas = 200
	refusedFrame := func(replica int) int { return 10 * (replica + 1) }
	outs := make([]*fakeOutput, replicas)
	for i := range outs {
		refuseAt, offered := refusedFrame(i), 0 // offered is guarded by the Output's lock
		outs[i] = &fakeOutput{refuse: func([]byte) error {
			offered++
			if offered == refuseAt {
				return cause
			}
			return nil
		}}
		master.Add(Candidate{ID: uint64(i + 1), ListeningPort: 6380, Out: outs[i]})
	}

	const writers, perWriter = 8, 500 // twice the frames the last Replica refuses at
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				master.Propagate(frame, false)
			}
		}()
	}
	wg.Wait()

	for i, out := range outs {
		if got, want := len(out.Bytes())/len(frame), refusedFrame(i)-1; got != want {
			t.Errorf("replica %d was pushed %d frames, want only the %d before the one it refused", i+1, got, want)
		}
	}
	if got := master.Count(); got != 0 {
		t.Fatalf("Count() = %d, want every Replica detached", got)
	}
	if got, want := master.Offset(), int64(writers*perWriter*len(frame)); got != want {
		t.Fatalf("Offset() = %d, want every frame counted: %d", got, want)
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
			master.Add(Candidate{ID: 1, ListeningPort: 6380, Out: &fakeOutput{}})

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
// The replicas register as PSYNC registers them, through beginAttach and
// reserve, but outside an attach cut, which is the command sequencer's. A client
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
	outs := make([]*fakeOutput, replicas)
	for i := range outs {
		waitForPropagated(propagated.Load() + 500)
		outs[i] = &fakeOutput{}
		master.beginAttach()
		master.reserve(Candidate{ID: uint64(i + 1), ListeningPort: 6380 + i, Out: outs[i]})
	}
	waitForPropagated(propagated.Load() + 500)
	stopWriters()

	for i, out := range outs {
		if !master.Ack(uint64(i+1), int64(len(out.Bytes()))) {
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

// TestAReplicaAttachesInTwoSteps walks a replica through beginAttach and
// reserve, as PSYNC takes them, and checks what each step makes visible: the
// master is active from beginAttach, so writes are ordered; reserve marks the
// replica's Output a replica's, tells it to hold, and registers the replica at
// the master offset of that moment, so that every frame counted after it is
// pushed to that Output and none counted before it is.
func TestAReplicaAttachesInTwoSteps(t *testing.T) {
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

	out := &fakeOutput{}
	master.reserve(Candidate{ID: 1, ListeningPort: 6380, Out: out})
	if !master.Active() || master.Count() != 1 {
		t.Fatalf("after reserve: Active() = %v and Count() = %d, want true and 1", master.Active(), master.Count())
	}
	if marks, holds := out.asked("MarkReplica"), out.asked("HoldPushes"); marks != 1 || holds != 1 {
		t.Fatalf("after reserve: the Output was marked a replica's %d times and told to hold %d times, want once each", marks, holds)
	}
	for i := 0; i < 2; i++ {
		if _, queued, _ := master.countAndQueue(payload); queued != 1 {
			t.Fatalf("a frame counted after reserve was queued for %d replicas, want 1", queued)
		}
	}
	if got, want := out.Bytes(), bytes.Repeat(payload, 2); !bytes.Equal(got, want) {
		t.Fatalf("the replica was pushed %q, want the two frames counted after its base, %q", got, want)
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

	if detached := master.Detach(1); detached == nil || detached.ID != 1 || detached.ListeningPort != 6380 {
		t.Fatalf("Detach(1) = %+v, want the replica", detached)
	}
	if again := master.Detach(1); again != nil {
		t.Fatalf("a second Detach(1) = %+v, want nil: only the first finds it", again)
	}
	if master.Active() {
		t.Fatal("Active() = true after the only replica was removed, want false")
	}
	master.countAndQueue(payload)
	if got, want := out.Bytes(), bytes.Repeat(payload, 2); !bytes.Equal(got, want) {
		t.Fatalf("a detached replica was pushed a frame: it holds %q, want %q", got, want)
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

// TestAFrameCountedAfterPropagationStopsIsPushedToNoReplica stops propagation,
// as shutdown does before it ends the Replicas' streams, and then propagates.
// The frame is counted and reaches no Replica: an ended stream must not be
// written past. A Replica whose Output is closed by then is left for its
// connection's teardown: nothing is pushed to it, so nothing is refused and
// nobody is told it was dropped.
func TestAFrameCountedAfterPropagationStopsIsPushedToNoReplica(t *testing.T) {
	master := newTestMaster()
	drops := 0
	master.dropped = func(*Replica, error) { drops++ }
	open, closed := &fakeOutput{}, &fakeOutput{}
	master.Add(Candidate{ID: 1, ListeningPort: 6380, Out: open})
	master.Add(Candidate{ID: 2, ListeningPort: 6381, Out: closed})

	before := []byte("before the shutdown")
	if _, queued, _ := master.countAndQueue(before); queued != 2 {
		t.Fatalf("a frame propagated before the stop was queued for %d Replicas, want 2", queued)
	}
	if got := master.StopPropagating(); got != 2 {
		t.Fatalf("StopPropagating() = %d, want the 2 Replicas registered", got)
	}
	closed.close(errors.New("closed by the test"))

	after := []byte("after the stop")
	end, queued, refused := master.countAndQueue(after)
	if want := int64(len(before) + len(after)); end != want || queued != 0 || refused != 0 {
		t.Fatalf("a frame propagated after the stop: ends at %d, queued for %d, refused by %d; want it counted to %d and neither", end, queued, refused, want)
	}
	for name, out := range map[string]*fakeOutput{"open": open, "closed": closed} {
		if got := out.Bytes(); !bytes.Equal(got, before) {
			t.Fatalf("the Replica whose Output is %s holds %q, want only what was propagated before the stop", name, got)
		}
		if got := out.asked("Push"); got != 1 {
			t.Fatalf("the Replica whose Output is %s was pushed to %d times, want once, before the stop", name, got)
		}
	}
	if got := master.Count(); got != 2 || drops != 0 {
		t.Fatalf("after the stop: %d Replicas registered and %d reported dropped, want 2 and none", got, drops)
	}
}

// TestEndStreamsEndsEveryReplicasStreamAndWaitsForThemToLeave ends the streams
// of the Replicas at shutdown. Each Replica's Output is told once to end its
// stream, which shuts the write side after what is queued and leaves the
// connection open, so the end of the stream still reaches the Replica.
// EndStreams then waits until every Replica has closed its own side, which
// detaches it, or until the deadline, and stops propagation first, so nothing
// is pushed past the end.
func TestEndStreamsEndsEveryReplicasStreamAndWaitsForThemToLeave(t *testing.T) {
	tests := []struct {
		name string
		// leaving are the Replicas that close their side once their stream ends.
		leaving       []uint64
		wait          time.Duration
		wantRemaining int
	}{
		{name: "every replica closes its side", leaving: []uint64{1, 2}, wait: time.Minute},
		{name: "one replica does not close its side before the deadline", leaving: []uint64{1}, wait: 100 * time.Millisecond, wantRemaining: 1},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			master := newTestMaster()
			outs := map[uint64]*fakeOutput{1: {}, 2: {}}
			for id, out := range outs {
				master.Add(Candidate{ID: id, ListeningPort: 6380, Out: out})
			}
			for _, id := range tt.leaving {
				id, out := id, outs[id]
				// As the Replica's connection is torn down when the server reads
				// the Replica's close.
				out.ended = func() { go master.Detach(id) }
			}

			done := make(chan int, 1)
			go func() { done <- master.EndStreams(time.Now().Add(tt.wait)) }()
			select {
			case remaining := <-done:
				if remaining != tt.wantRemaining {
					t.Fatalf("EndStreams() = %d remaining, want %d", remaining, tt.wantRemaining)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("EndStreams() had not returned after 10s: it did not notice the Replicas leaving")
			}

			for id, out := range outs {
				if got := out.asked("EndStream"); got != 1 {
					t.Fatalf("Replica %d's stream was ended %d times, want once", id, got)
				}
			}
			if _, queued, refused := master.countAndQueue([]byte("past the end")); queued != 0 || refused != 0 {
				t.Fatalf("a frame propagated after EndStreams was queued for %d Replicas and refused by %d, want neither", queued, refused)
			}
			for id, out := range outs {
				if got := out.Bytes(); len(got) != 0 {
					t.Fatalf("Replica %d was pushed %q past the end of its stream", id, got)
				}
			}
		})
	}
}

func newTestMaster() *Master {
	return NewMaster(storage.NewStore(), nil, nil)
}

// fakeOutput is a Replica's Output in memory: it keeps the stream pushed to it
// and counts what the Master asked of it.
type fakeOutput struct {
	mu     sync.Mutex
	calls  map[string]int
	stream []byte
	// closed is why the Output refuses every frame, nil while it is open.
	closed error
	// last is why the latest frame was refused.
	last error

	// refuse, when set, decides for each frame offered to an open Output: the
	// frame it returns an error for is refused with that cause. Unlike a real
	// Output it accepts the frames after it again, so a test can tell whether
	// the Master offered it one. It is called under the Output's lock.
	refuse func(frame []byte) error
	// before, when set, runs at the start of every call, with the method's name.
	before func(method string)
	// pushed and ended, when set, run after a frame was accepted and after the
	// stream was ended.
	pushed func()
	ended  func()
}

func (o *fakeOutput) called(method string) {
	if o.before != nil {
		o.before(method)
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.calls == nil {
		o.calls = make(map[string]int)
	}
	o.calls[method]++
}

func (o *fakeOutput) MarkReplica() { o.called("MarkReplica") }
func (o *fakeOutput) HoldPushes()  { o.called("HoldPushes") }

func (o *fakeOutput) Push(frame []byte) bool {
	o.called("Push")
	if !o.accept(frame) {
		return false
	}
	if o.pushed != nil {
		o.pushed()
	}
	return true
}

func (o *fakeOutput) accept(frame []byte) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.last = o.closed
	if o.last == nil && o.refuse != nil {
		o.last = o.refuse(frame)
	}
	if o.last != nil {
		return false
	}
	o.stream = append(o.stream, frame...)
	return true
}

func (o *fakeOutput) Err() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.last
}

func (o *fakeOutput) EndStream() {
	o.called("EndStream")
	if o.ended != nil {
		o.ended()
	}
}

// close closes the Output, as the server's does when the connection ends or
// the Replica falls behind.
func (o *fakeOutput) close(cause error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.closed, o.last = cause, cause
}

// asked reports how often method was called.
func (o *fakeOutput) asked(method string) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.calls[method]
}

// Bytes is the stream the Output accepted, in the order it was pushed.
func (o *fakeOutput) Bytes() []byte {
	o.mu.Lock()
	defer o.mu.Unlock()
	return bytes.Clone(o.stream)
}

// TestAttachHoldsTheReplicasOutputAndRegistersItInsideItsCut attaches a Replica
// to a Master whose Store holds a key. The cut is told to switch recording on
// before it takes its locks, and the Master's lock is free while the cut runs:
// the cut here asks the Master whether it is active, which takes that lock.
//
// Inside the cut the Replica's Output is marked a Replica's and told to hold
// what is pushed to it, both before the Replica is registered, and the Replica
// is registered before the cut is released. The first frame propagated after
// the cut is therefore pushed to an Output that already holds it behind the
// reply and counts it against a Replica's limit. Told after the Replica was
// registered, a frame could reach the Output first and leave ahead of the
// reply.
//
// The reply is FULLRESYNC with the Master's replication ID and an RDB snapshot
// that holds the key.
func TestAttachHoldsTheReplicasOutputAndRegistersItInsideItsCut(t *testing.T) {
	store := storage.NewStore()
	if err := store.Set(nil, "key", []byte("value"), 0); err != nil {
		t.Fatalf("Set() error = %v", err)
	}
	master := NewMaster(store, nil, nil)

	// What was true each time the Master called the Output. Everything here
	// runs on the test's goroutine.
	type moment struct {
		method     string
		inCut      bool
		registered int64
	}
	var moments []moment
	inCut := false
	out := &fakeOutput{before: func(method string) {
		// Read as an ordered Propagate reads it, without the Master's lock.
		moments = append(moments, moment{method: method, inCut: inCut, registered: master.registered.Load()})
	}}

	var activeInCut, registeredAtRelease bool
	cut := func(startRecording func()) (release func()) {
		startRecording()
		activeInCut = master.Active()
		inCut = true
		return func() {
			inCut = false
			registeredAtRelease = master.Count() == 1
		}
	}
	attachment, err := master.Attach(Candidate{ID: 1, ListeningPort: 6380, Out: out}, cut)
	if err != nil {
		t.Fatalf("Attach() error = %v", err)
	}

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
	if got := out.Bytes(); !bytes.Equal(got, frame) {
		t.Fatalf("the Replica's Output was pushed %q, want the frame propagated after its cut, %q", got, frame)
	}

	seen := make(map[string]int)
	for _, m := range moments {
		seen[m.method]++
		switch m.method {
		case "MarkReplica", "HoldPushes":
			if !m.inCut || m.registered != 0 {
				t.Fatalf("%s: called inside the cut = %v with %d Replicas registered, want inside it and before the Replica is registered", m.method, m.inCut, m.registered)
			}
		case "Push":
			if seen["MarkReplica"] != 1 || seen["HoldPushes"] != 1 {
				t.Fatalf("a frame was pushed to an Output marked %d times and told to hold %d times, want once each first", seen["MarkReplica"], seen["HoldPushes"])
			}
		}
	}
	if fmt.Sprint(seen) != fmt.Sprint(map[string]int{"MarkReplica": 1, "HoldPushes": 1, "Push": 1}) {
		t.Fatalf("the Master called the Output %v, want it marked, told to hold and pushed to once each", seen)
	}
}

// TestAnAttachWhoseCopyPanicsLeavesTheMasterInactive has the snapshot copy
// panic inside the cut. The attach ends, so Active reports what it did before
// and writes stop being ordered for a Replica that never registered, and the
// cut's locks are released. The client's Output is not told to hold: no reply
// of an attach would release it.
func TestAnAttachWhoseCopyPanicsLeavesTheMasterInactive(t *testing.T) {
	master := NewMaster(nil, nil, nil) // copying a nil Store panics
	out := &fakeOutput{}
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
		_, _ = master.Attach(Candidate{ID: 1, Out: out}, cut)
	}()

	if master.Active() || master.Count() != 0 {
		t.Fatalf("Active() = %v and Count() = %d after the attach failed, want false and 0", master.Active(), master.Count())
	}
	if !released {
		t.Fatal("the cut was not released")
	}
	if got := out.asked("HoldPushes"); got != 0 {
		t.Fatalf("the Output was told to hold %d times by an attach that failed, want never", got)
	}
}

// TestAnAttachWhoseSnapshotExceedsTheLimitIsRefusedWithoutAReplica sizes the
// snapshot of a Store against a limit just below, at and above it. Over the
// limit the Master refuses with the size and the limit, encodes no snapshot,
// registers no Replica and asks nothing of the Output, so the connection stays
// an ordinary client with nothing held; the cut is released and Active reports
// false again. At the limit the snapshot is served, since a Replica accepts a
// bulk string of exactly that many bytes.
func TestAnAttachWhoseSnapshotExceedsTheLimitIsRefusedWithoutAReplica(t *testing.T) {
	store := storage.NewStore()
	if err := store.Set(nil, "key", bytes.Repeat([]byte("v"), 1000), 0); err != nil {
		t.Fatalf("Set() error = %v", err)
	}
	entries, _ := store.SnapshotStrings()
	size := rdb.SnapshotSize(entries)

	tests := []struct {
		name    string
		limit   int
		refused bool
	}{
		{name: "one byte over the limit", limit: size - 1, refused: true},
		{name: "exactly at the limit", limit: size, refused: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var logs bytes.Buffer
			master := NewMaster(store, nil, slog.New(slog.NewTextHandler(&logs, nil)))
			master.SetSnapshotLimit(tt.limit)
			out := &fakeOutput{}
			released := false
			cut := func(startRecording func()) (release func()) {
				startRecording()
				return func() { released = true }
			}

			attachment, err := master.Attach(Candidate{ID: 7, Out: out}, cut)

			if !released {
				t.Fatal("the cut was not released")
			}
			if !tt.refused {
				if err != nil {
					t.Fatalf("Attach() error = %v, want the snapshot served", err)
				}
				snapshot, ok := attachment.Reply()[1].(protocol.BulkString)
				if !ok || len(snapshot.Data) != size {
					t.Fatalf("Reply()[1] = %#v, want a snapshot of %d bytes", attachment.Reply()[1], size)
				}
				if master.Count() != 1 {
					t.Fatalf("Count() = %d after a served attach, want 1", master.Count())
				}
				return
			}

			var tooLarge *SnapshotTooLargeError
			if !errors.As(err, &tooLarge) || tooLarge.Size != size || tooLarge.Limit != tt.limit {
				t.Fatalf("Attach() error = %v, want a *SnapshotTooLargeError of %d bytes over %d", err, size, tt.limit)
			}
			if !strings.HasPrefix(err.Error(), "replication: ") {
				t.Fatalf("Attach() error = %q, want it to start with the package", err)
			}
			if attachment != nil {
				t.Fatalf("Attach() returned an attachment %#v with its error", attachment)
			}
			if master.Count() != 0 || master.Active() {
				t.Fatalf("Count() = %d and Active() = %v after a refusal, want 0 and false", master.Count(), master.Active())
			}
			for _, method := range []string{"MarkReplica", "HoldPushes", "Push"} {
				if got := out.asked(method); got != 0 {
					t.Fatalf("the Output was asked %s %d times by a refused attach, want never", method, got)
				}
			}
			master.Propagate([]byte("*1\r\n$4\r\nPING\r\n"), true)
			if got := out.asked("Push"); got != 0 {
				t.Fatalf("a frame propagated after the refusal was pushed %d times to the refused Output, want never", got)
			}
			for _, want := range []string{"replica_id=7", fmt.Sprintf("snapshot_size_bytes=%d", size), fmt.Sprintf("limit_bytes=%d", tt.limit)} {
				if !strings.Contains(logs.String(), want) {
					t.Fatalf("the Master logged %q, want it to name %s", logs.String(), want)
				}
			}
		})
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
			// Await pushes its GETACK once.
			out := &fakeOutput{pushed: func() { close(asked) }}
			master.Add(Candidate{ID: 1, ListeningPort: 6380, Out: out})

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
	master.Add(Candidate{ID: 7, ListeningPort: 6380, Out: &fakeOutput{}})

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

// TestARefusedAttachDoesNotCopyTheKeyspace refuses the snapshot of a Store
// that holds one large value, and measures what the Master allocates to do it.
// The refusal needs the snapshot's size alone, so it must allocate far less
// than the value: a copy of the keyspace, which every writer would wait for
// inside the cut, allocates at least the value's bytes.
func TestARefusedAttachDoesNotCopyTheKeyspace(t *testing.T) {
	const valueBytes = 16 << 20
	store := storage.NewStore()
	if err := store.Set(nil, "key", bytes.Repeat([]byte("v"), valueBytes), 0); err != nil {
		t.Fatalf("Set() error = %v", err)
	}
	master := NewMaster(store, nil, slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)))
	master.SetSnapshotLimit(valueBytes / 2)
	cut := func(startRecording func()) (release func()) {
		startRecording()
		return func() {}
	}

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, err := master.Attach(Candidate{ID: 1, Out: &fakeOutput{}}, cut)
	runtime.ReadMemStats(&after)

	var tooLarge *SnapshotTooLargeError
	if !errors.As(err, &tooLarge) {
		t.Fatalf("Attach() error = %v, want a *SnapshotTooLargeError", err)
	}
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > valueBytes/4 {
		t.Fatalf("a refused attach allocated %d bytes for a keyspace of %d, want under a quarter of it", allocated, valueBytes)
	}
}
