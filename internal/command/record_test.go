package command

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/record"
	"github.com/maltemindedal/stash/internal/server"
)

var errDiskFull = errors.New("no space left on device")

func TestAWriteThatCannotBeMadeDurableIsAnsweredWithPersistenceFailure(t *testing.T) {
	// Under appendfsync always the Recorder withholds a write whose append
	// fails: the client is answered ERR persistence failure and nothing reaches
	// the replicas, but the write stays applied in memory. A transaction is
	// withheld as one write.
	tests := []struct {
		name     string
		requests [][]string
	}{
		{name: "SET", requests: [][]string{{"SET", "k", "v"}}},
		{name: "EXEC", requests: [][]string{{"MULTI"}, {"SET", "k", "v"}, {"EXEC"}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			executor, log, stream := newRecordingTestExecutor(nil)
			log.Fail(errDiskFull, true)
			ctx := clientContext(executor)

			var err error
			for _, parts := range tt.requests {
				_, err = handle(ctx, executor, requestValue(parts...), true)
			}

			if !errors.Is(err, ErrPersistenceFailure) {
				t.Fatalf("%v error = %v, want %v", tt.requests[len(tt.requests)-1], err, ErrPersistenceFailure)
			}
			if got := responseErrorValue(err); got.Message != "ERR persistence failure" {
				t.Fatalf("reply = %q, want ERR persistence failure", got.Message)
			}
			if got := stream.Payloads(); len(got) != 0 {
				t.Fatalf("propagated %q, want nothing", got)
			}
			if _, stored, _ := executor.store.Get("k"); !stored {
				t.Fatal("k is not stored, want the write applied in memory")
			}
		})
	}
}

func TestAWriteFromTheMasterThatCannotBeLoggedEndsTheLink(t *testing.T) {
	for _, withhold := range []bool{true, false} {
		t.Run(fmt.Sprintf("withhold %v", withhold), func(t *testing.T) {
			executor, log, _ := newRecordingTestExecutor(nil)
			log.Fail(errDiskFull, withhold)

			_, err := handle(withOrigin(context.Background(), record.OriginMaster), executor, requestValue("SET", "k", "v"), true)

			if !errors.Is(err, server.ErrNotRecorded) || !errors.Is(err, errDiskFull) {
				t.Fatalf("SET from the Master error = %v, want server.ErrNotRecorded wrapping %v", err, errDiskFull)
			}
		})
	}
}

func TestAClientWriteSetsTheClientsLastWriteOffsetToItsEndInTheStream(t *testing.T) {
	executor, _, stream := newRecordingTestExecutor(nil)
	ctx := clientContext(executor)
	client, _ := clientOfContext(ctx)

	if _, err := handle(ctx, executor, requestValue("SET", "k", "v"), true); err != nil {
		t.Fatalf("SET error = %v", err)
	}

	if got, want := client.LastWriteReplicationOffset(), int64(len(stream.Bytes())); got != want || want == 0 {
		t.Fatalf("LastWriteReplicationOffset() = %d, want %d, the end of the SET in the stream", got, want)
	}
}

// stripeCheckingLog is an append-only file that, on each append, reports
// whether the stripe of key was held.
type stripeCheckingLog struct {
	executor *Executor
	key      []byte
	held     []bool
}

func (l *stripeCheckingLog) Append([]byte) (bool, error) {
	stripe := &l.executor.seq.stripes[stripeOf(l.key)]
	free := stripe.TryLock()
	if free {
		stripe.Unlock()
	}
	l.held = append(l.held, !free)
	return false, nil
}

func TestBlpopRecordsItsPopBeforeReleasingItsStripe(t *testing.T) {
	// The pop is a write to the key: until it is in the AOF and on its way to
	// the replicas, no other write to the key may run, or the two could reach
	// them in the other order. BLPOP orders itself, so it records its pop inside
	// the attempt that holds the key's stripe.
	log := &stripeCheckingLog{key: []byte("jobs")}
	executor, _, stream := newRecordingTestExecutor(func(s *server.Services) { s.Recorder.SetLog(log) })
	log.executor = executor
	ctx := clientContext(executor)
	if _, err := handle(ctx, executor, requestValue("RPUSH", "jobs", "job-1"), true); err != nil {
		t.Fatalf("RPUSH error = %v", err)
	}

	result, err := handle(ctx, executor, requestValue("BLPOP", "jobs"), true)
	if err != nil {
		t.Fatalf("BLPOP error = %v", err)
	}

	assertValueEqual(t, result.Responses[0], protocol.Array{Elements: []protocol.Value{
		protocol.BulkString{Data: []byte("jobs")},
		protocol.BulkString{Data: []byte("job-1")},
	}})
	if len(log.held) != 2 || !log.held[1] {
		t.Fatalf("stripe held at each append = %v, want the BLPOP's append (the second) made holding it", log.held)
	}
	if got := stream.Payloads(); len(got) != 2 || !bytes.Contains(got[1], []byte("LPOP")) {
		t.Fatalf("propagated %q, want RPUSH then LPOP", got)
	}
}

func TestTwoWritesToOneKeyReachBothDestinationsInExecutionOrder(t *testing.T) {
	// Writes to one key do not commute, and the sequencer holds the key's stripe
	// from before each executes until it is recorded. Concurrent writers to one
	// key must therefore reach the AOF and the replicas in the order they were
	// applied: the same order in both, and ending with the value stored.
	executor, log, stream := newRecordingTestExecutor(nil)
	const writers, writes = 8, 200

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		ctx := clientContext(executor)
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < writes; i++ {
				if _, err := handle(ctx, executor, requestValue("SET", "k", fmt.Sprintf("%d-%d", w, i)), true); err != nil {
					t.Errorf("SET error = %v", err)
					return
				}
			}
		}(w)
	}
	wg.Wait()

	aofValues, streamValues := setValues(t, log.Bytes()), setValues(t, stream.Bytes())
	if len(aofValues) != writers*writes {
		t.Fatalf("the AOF holds %d SETs, want %d", len(aofValues), writers*writes)
	}
	if fmt.Sprint(aofValues) != fmt.Sprint(streamValues) {
		t.Fatal("the AOF and the replication stream hold the SETs in different orders")
	}
	stored, _, _ := executor.store.Get("k")
	if last := aofValues[len(aofValues)-1]; last != string(stored) {
		t.Fatalf("the last SET recorded is %q, but k holds %q", last, stored)
	}
}

// setValues parses payload as SET commands and returns the value each sets.
func setValues(t *testing.T, payload []byte) []string {
	t.Helper()
	parser := protocol.NewParser(bytes.NewReader(payload))
	var values []string
	for {
		value, err := parser.Parse()
		if err != nil {
			break
		}
		request, err := DecodeRequest(value)
		if err != nil || request.Name != "SET" || len(request.Args) != 2 {
			t.Fatalf("recorded %#v (%v), want SET k <value>", value, err)
		}
		values = append(values, string(request.Args[1]))
	}
	return values
}
