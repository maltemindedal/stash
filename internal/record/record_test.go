package record_test

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/record"
	"github.com/maltemindedal/stash/internal/record/recordtest"
)

var errDiskFull = errors.New("no space left on device")

func command(parts ...string) protocol.Array {
	elements := make([]protocol.Value, len(parts))
	for i, part := range parts {
		elements[i] = protocol.BulkString{Data: []byte(part)}
	}
	return protocol.Array{Elements: elements}
}

func encoded(t *testing.T, frames ...protocol.Array) string {
	t.Helper()
	values := make([]protocol.Value, len(frames))
	for i, frame := range frames {
		values[i] = frame
	}
	payload, err := protocol.EncodeValues(values)
	if err != nil {
		t.Fatalf("EncodeValues() error = %v", err)
	}
	return string(payload)
}

func payloads(got [][]byte) []string {
	strs := make([]string, len(got))
	for i, payload := range got {
		strs[i] = string(payload)
	}
	return strs
}

func assertPayloads(t *testing.T, what string, got [][]byte, want ...string) {
	t.Helper()
	if fmt.Sprint(payloads(got)) != fmt.Sprint(want) {
		t.Fatalf("%s = %q, want %q", what, payloads(got), want)
	}
}

// newRecorder returns a Recorder with an open log, and the stand-ins it writes to.
func newRecorder() (*record.Recorder, *recordtest.Log, *recordtest.Stream) {
	log := &recordtest.Log{}
	stream := &recordtest.Stream{}
	recorder := record.New(stream, slog.New(slog.NewTextHandler(io.Discard, nil)))
	recorder.SetLog(log)
	return recorder, log, stream
}

func write(origin record.Origin, frames ...protocol.Array) *record.Write {
	w := &record.Write{Origin: origin}
	for _, frame := range frames {
		w.Add(frame, record.ToBoth)
	}
	return w
}

func TestAClientWriteIsLoggedAndPropagated(t *testing.T) {
	recorder, log, stream := newRecorder()
	set := command("SET", "k", "v")

	outcome := recorder.Record(write(record.OriginClient, set))

	if outcome.Err != nil || outcome.Withheld {
		t.Fatalf("Record() = %+v, want no error and not withheld", outcome)
	}
	assertPayloads(t, "AOF", log.Payloads(), encoded(t, set))
	assertPayloads(t, "stream", stream.Payloads(), encoded(t, set))
	if want := int64(len(encoded(t, set))); outcome.End != want {
		t.Fatalf("Record().End = %d, want %d", outcome.End, want)
	}
	if got := stream.Ordered(); len(got) != 1 || !got[0] {
		t.Fatalf("stream ordered = %v, want [true]: a client write's caller holds its ordering", got)
	}
}

func TestAClientWriteWhoseFsyncFailsUnderAlwaysIsNotPropagated(t *testing.T) {
	recorder, log, stream := newRecorder()
	log.Fail(errDiskFull, true)

	outcome := recorder.Record(write(record.OriginClient, command("SET", "k", "v")))

	if !outcome.Withheld || !errors.Is(outcome.Err, errDiskFull) || outcome.End != 0 {
		t.Fatalf("Record() = %+v, want withheld with %v and no end offset", outcome, errDiskFull)
	}
	if got := stream.Payloads(); len(got) != 0 {
		t.Fatalf("stream = %q, want nothing propagated", payloads(got))
	}
}

func TestAFailedAppendUnderADeferredPolicyIsStillPropagated(t *testing.T) {
	for _, origin := range []record.Origin{record.OriginClient, record.OriginServer} {
		t.Run(fmt.Sprintf("origin %d", origin), func(t *testing.T) {
			recorder, log, stream := newRecorder()
			log.Fail(errDiskFull, false)
			del := command("DEL", "k")

			outcome := recorder.Record(write(origin, del))

			if outcome.Withheld || !errors.Is(outcome.Err, errDiskFull) {
				t.Fatalf("Record() = %+v, want %v and not withheld", outcome, errDiskFull)
			}
			assertPayloads(t, "stream", stream.Payloads(), encoded(t, del))
		})
	}
}

func TestAServerWriteWhoseFsyncFailsUnderAlwaysIsNotPropagated(t *testing.T) {
	recorder, log, stream := newRecorder()
	log.Fail(errDiskFull, true)

	outcome := recorder.Record(write(record.OriginServer, record.DeleteFrame([]string{"k"})))

	if !outcome.Withheld || !errors.Is(outcome.Err, errDiskFull) {
		t.Fatalf("Record() = %+v, want withheld with %v", outcome, errDiskFull)
	}
	if got := stream.Payloads(); len(got) != 0 {
		t.Fatalf("stream = %q, want nothing propagated", payloads(got))
	}
}

func TestAServerWriteIsPropagatedWithoutClaimingTheWritesOrdering(t *testing.T) {
	recorder, log, stream := newRecorder()
	del := record.DeleteFrame([]string{"a", "b"})

	recorder.Record(write(record.OriginServer, del))

	assertPayloads(t, "AOF", log.Payloads(), encoded(t, del))
	assertPayloads(t, "stream", stream.Payloads(), encoded(t, del))
	if got := stream.Ordered(); len(got) != 1 || got[0] {
		t.Fatalf("stream ordered = %v, want [false]: a Replica's full resync publishes expiries holding no sequencer lock", got)
	}
}

func TestCommandFramesFromTheMasterAreLoggedButNeverPropagated(t *testing.T) {
	recorder, log, stream := newRecorder()
	set := command("SET", "k", "v")

	outcome := recorder.Record(write(record.OriginMaster, set))

	if outcome != (record.Outcome{}) {
		t.Fatalf("Record() = %+v, want the zero Outcome", outcome)
	}
	assertPayloads(t, "AOF", log.Payloads(), encoded(t, set))
	if got := stream.Payloads(); len(got) != 0 {
		t.Fatalf("stream = %q, want nothing propagated", payloads(got))
	}
}

func TestAFailedAppendOfTheMastersFramesIsReturned(t *testing.T) {
	for _, withhold := range []bool{true, false} {
		t.Run(fmt.Sprintf("withhold %v", withhold), func(t *testing.T) {
			recorder, log, _ := newRecorder()
			log.Fail(errDiskFull, withhold)

			outcome := recorder.Record(write(record.OriginMaster, command("SET", "k", "v")))

			if !errors.Is(outcome.Err, errDiskFull) {
				t.Fatalf("Record().Err = %v, want %v, so the link ends", outcome.Err, errDiskFull)
			}
		})
	}
}

func TestAOFReplayRecordsNothing(t *testing.T) {
	recorder, log, stream := newRecorder()

	outcome := recorder.Record(write(record.OriginReplay, command("SET", "k", "v")))

	if outcome != (record.Outcome{}) || log.Appends() != 0 || len(stream.Payloads()) != 0 {
		t.Fatalf("Record() = %+v with %d appends and %d propagations, want nothing recorded", outcome, log.Appends(), len(stream.Payloads()))
	}
}

func TestAZeroOriginIsRefused(t *testing.T) {
	recorder, log, stream := newRecorder()

	outcome := recorder.Record(write(0, command("SET", "k", "v")))

	if !errors.Is(outcome.Err, record.ErrZeroOrigin) {
		t.Fatalf("Record().Err = %v, want %v", outcome.Err, record.ErrZeroOrigin)
	}
	if log.Appends() != 0 || len(stream.Payloads()) != 0 {
		t.Fatalf("%d appends and %d propagations, want nothing recorded", log.Appends(), len(stream.Payloads()))
	}
}

func TestEachDestinationGetsOnlyItsCommandFramesInOneCall(t *testing.T) {
	// XADD under maxmemory goes to the AOF only, and the DEL of the key it
	// evicted to both; PUBLISH goes to the Replicas only.
	xadd := command("XADD", "s", "1-1", "f", "v")
	del := record.DeleteFrame([]string{"cold"})
	publish := command("PUBLISH", "news", "hello")
	recorder, log, stream := newRecorder()
	w := &record.Write{Origin: record.OriginClient}
	w.Add(xadd, record.ToAOF)
	w.Add(del, record.ToBoth)
	w.Add(publish, record.ToReplicas)

	recorder.Record(w)

	assertPayloads(t, "AOF", log.Payloads(), encoded(t, xadd, del))
	assertPayloads(t, "stream", stream.Payloads(), encoded(t, del, publish))
}

func TestAMixedWriteOfManyFramesKeepsEachDestinationsOrder(t *testing.T) {
	// More frames than Record tracks without allocating, as a long EXEC has.
	recorder, log, stream := newRecorder()
	w := &record.Write{Origin: record.OriginClient}
	var toAOF, toStream []protocol.Array
	for i := 0; i < 20; i++ {
		frame := command("SET", fmt.Sprint("k", i), "v")
		to := []record.Dest{record.ToBoth, record.ToAOF, record.ToReplicas}[i%3]
		w.Add(frame, to)
		if to&record.ToAOF != 0 {
			toAOF = append(toAOF, frame)
		}
		if to&record.ToReplicas != 0 {
			toStream = append(toStream, frame)
		}
	}

	recorder.Record(w)

	assertPayloads(t, "AOF", log.Payloads(), encoded(t, toAOF...))
	assertPayloads(t, "stream", stream.Payloads(), encoded(t, toStream...))
}

func TestEachWriteIsEncodedOnce(t *testing.T) {
	// A write whose frames all go to both destinations is encoded once, and
	// the append-only file and the stream are handed the same bytes.
	log := &firstByteLog{}
	stream := &firstByteStream{}
	recorder := record.New(stream, nil)
	recorder.SetLog(log)

	recorder.Record(write(record.OriginClient, command("SET", "k", "v"), record.DeleteFrame([]string{"cold"})))

	if log.first == nil || log.first != stream.first {
		t.Fatalf("the AOF was handed %p and the stream %p, want the same encoded bytes", log.first, stream.first)
	}
}

// firstByteLog and firstByteStream keep where the payload they were handed
// starts.
type firstByteLog struct{ first *byte }

func (l *firstByteLog) Append(payload []byte) (bool, error) {
	l.first = &payload[0]
	return false, nil
}

type firstByteStream struct{ first *byte }

func (s *firstByteStream) Propagate(payload []byte, _ bool) int64 {
	s.first = &payload[0]
	return int64(len(payload))
}

func (s *firstByteStream) Active() bool { return true }

func TestAWriteThatCannotBeEncodedIsWithheldAndSentNowhere(t *testing.T) {
	recorder, log, stream := newRecorder()
	// A nil element is a value the encoder does not know.
	broken := protocol.Array{Elements: []protocol.Value{protocol.BulkString{Data: []byte("SET")}, nil}}

	outcome := recorder.Record(write(record.OriginClient, command("SET", "k", "v"), broken))

	if outcome.Err == nil || !outcome.Withheld {
		t.Fatalf("Record() = %+v, want an error and withheld", outcome)
	}
	if log.Appends() != 0 || len(stream.Payloads()) != 0 {
		t.Fatalf("%d appends and %d propagations, want none", log.Appends(), len(stream.Payloads()))
	}
}

func TestAWriteForTheReplicasOnlyAppendsNothing(t *testing.T) {
	recorder, log, stream := newRecorder()
	w := &record.Write{Origin: record.OriginClient}
	w.Add(command("PUBLISH", "news", "hello"), record.ToReplicas)

	recorder.Record(w)

	if log.Appends() != 0 || len(stream.Payloads()) != 1 {
		t.Fatalf("%d appends and %d propagations, want 0 and 1", log.Appends(), len(stream.Payloads()))
	}
}

func TestWithoutAnAppendOnlyFileAWriteIsOnlyPropagated(t *testing.T) {
	recorder, log, stream := newRecorder()
	recorder.SetLog(nil)
	set := command("SET", "k", "v")

	outcome := recorder.Record(write(record.OriginClient, set))

	if outcome.Err != nil || log.Appends() != 0 {
		t.Fatalf("Record() = %+v with %d appends, want no error and no append", outcome, log.Appends())
	}
	assertPayloads(t, "stream", stream.Payloads(), encoded(t, set))
}

func TestExtendAppendsTheQueuedCommandFramesInOrder(t *testing.T) {
	first, second := command("SET", "a", "1"), command("INCR", "b")
	exec := &record.Write{Origin: record.OriginClient}
	exec.Extend(write(record.OriginClient, first))
	exec.Extend(write(record.OriginClient, second))
	recorder, log, stream := newRecorder()

	recorder.Record(exec)

	assertPayloads(t, "AOF", log.Payloads(), encoded(t, first, second))
	assertPayloads(t, "stream", stream.Payloads(), encoded(t, first, second))
}

func TestRecordsReportsAnOpenLogOrAnActiveStream(t *testing.T) {
	tests := []struct {
		name        string
		log, active bool
		wantRecords bool
	}{
		{name: "neither", wantRecords: false},
		{name: "an append-only file", log: true, wantRecords: true},
		{name: "a Replica attached or attaching", active: true, wantRecords: true},
		{name: "both", log: true, active: true, wantRecords: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stream := &recordtest.Stream{}
			stream.SetActive(tt.active)
			recorder := record.New(stream, nil)
			if tt.log {
				recorder.SetLog(&recordtest.Log{})
			}
			if got := recorder.Records(); got != tt.wantRecords {
				t.Fatalf("Records() = %v, want %v", got, tt.wantRecords)
			}
		})
	}
}

func TestDeleteFrameNamesEveryKey(t *testing.T) {
	got := encoded(t, record.DeleteFrame([]string{"a", "b"}))
	if !strings.HasPrefix(got, "*3\r\n$3\r\nDEL\r\n$1\r\na\r\n$1\r\nb\r\n") {
		t.Fatalf("DeleteFrame() = %q, want DEL a b", got)
	}
}

// BenchmarkRecorderRecord records one SET from parallel writers, to stand-ins
// that copy what they are given, with nothing recording, an append-only file,
// a Replica, and both.
func BenchmarkRecorderRecord(b *testing.B) {
	set := command("SET", "key", "value")
	for _, sinks := range []struct {
		name        string
		log, active bool
	}{
		{name: "none"},
		{name: "aof", log: true},
		{name: "stream", active: true},
		{name: "both", log: true, active: true},
	} {
		b.Run(sinks.name, func(b *testing.B) {
			stream := &discardStream{active: sinks.active}
			recorder := record.New(stream, nil)
			if sinks.log {
				recorder.SetLog(discardLog{})
			}
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					w := record.Write{Origin: record.OriginClient}
					w.Add(set, record.ToBoth)
					if outcome := recorder.Record(&w); outcome.Err != nil {
						b.Error(outcome.Err)
						return
					}
				}
			})
		})
	}
}

// discardLog and discardStream read every byte of a payload and keep none, so
// the benchmark measures the Recorder rather than its stand-ins.
type discardLog struct{}

func (discardLog) Append(payload []byte) (bool, error) {
	if len(payload) == 0 {
		return false, errors.New("empty payload")
	}
	return false, nil
}

type discardStream struct {
	active bool
	end    atomic.Int64
}

func (s *discardStream) Propagate(payload []byte, _ bool) int64 {
	return s.end.Add(int64(len(payload)))
}

func (s *discardStream) Active() bool { return s.active }
