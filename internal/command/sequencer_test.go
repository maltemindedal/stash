package command

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// blocked reports whether f is still waiting after a short time.
func blocked(f func()) (stillBlocked bool, finished func() bool) {
	done := make(chan struct{})
	go func() {
		f()
		close(done)
	}()
	select {
	case <-done:
		return false, func() bool { return true }
	case <-time.After(50 * time.Millisecond):
		return true, func() bool {
			select {
			case <-done:
				return true
			case <-time.After(2 * time.Second):
				return false
			}
		}
	}
}

// keysInDifferentStripes finds two keys that hash to different stripes, and one
// that shares a stripe with the first.
func keysInStripes(t *testing.T) (a, sameAsA, otherThanA []byte) {
	t.Helper()

	a = []byte("key-0")
	for i := 1; i < 10000 && (sameAsA == nil || otherThanA == nil); i++ {
		candidate := []byte("key-" + string(rune('0'+i%10)) + string(rune('a'+i%26)) + string(rune('A'+(i/26)%26)) + string(rune('0'+(i/676)%10)))
		if stripeOf(candidate) == stripeOf(a) && sameAsA == nil {
			sameAsA = candidate
		}
		if stripeOf(candidate) != stripeOf(a) && otherThanA == nil {
			otherThanA = candidate
		}
	}
	if sameAsA == nil || otherThanA == nil {
		t.Fatal("could not find keys in the stripes the test needs")
	}
	return a, sameAsA, otherThanA
}

func TestSequencerOrdersWritesToTheSameStripeOnly(t *testing.T) {
	q := newSequencer()
	a, sameAsA, otherThanA := keysInStripes(t)

	release := q.beginWrite(keysFirstArg, [][]byte{a})

	if isBlocked, _ := blocked(func() { q.beginWrite(keysFirstArg, [][]byte{otherThanA})() }); isBlocked {
		t.Fatal("a write to a key in another stripe waited for a write it does not conflict with")
	}

	isBlocked, finished := blocked(func() { q.beginWrite(keysFirstArg, [][]byte{sameAsA})() })
	if !isBlocked {
		t.Fatal("a second write to a key in the same stripe did not wait for the first")
	}
	release()
	if !finished() {
		t.Fatal("the second write never proceeded after the first was released")
	}
}

func TestSequencerOrdersWritesWithUnknownOrManyKeysAgainstTheRelevantWrites(t *testing.T) {
	q := newSequencer()
	a, _, otherThanA := keysInStripes(t)

	t.Run("a write whose keys are unknown waits for every other write", func(t *testing.T) {
		release := q.beginWrite(keysFirstArg, [][]byte{otherThanA})
		isBlocked, finished := blocked(func() { q.beginWrite(keysUnknown, nil)() })
		if !isBlocked {
			t.Fatal("a write with unknown keys did not wait for a write in progress")
		}
		release()
		if !finished() {
			t.Fatal("the write with unknown keys never proceeded")
		}
	})

	t.Run("a write to every argument waits for the writes to any of them", func(t *testing.T) {
		release := q.beginWrite(keysFirstArg, [][]byte{a})
		isBlocked, finished := blocked(func() { q.beginWrite(keysEveryArg, [][]byte{otherThanA, a})() })
		if !isBlocked {
			t.Fatal("a multi-key write did not wait for a write to one of its keys")
		}
		release()
		if !finished() {
			t.Fatal("the multi-key write never proceeded")
		}
	})

	t.Run("a multi-key write leaves writes to other keys alone", func(t *testing.T) {
		release := q.beginWrite(keysEveryArg, [][]byte{a})
		defer release()
		if isBlocked, _ := blocked(func() { q.beginWrite(keysFirstArg, [][]byte{otherThanA})() }); isBlocked {
			t.Fatal("a write waited for a multi-key write that does not touch its key")
		}
	})
}

func TestSequencerExclusiveGateWaitsForAndBlocksEverything(t *testing.T) {
	q := newSequencer()
	key := []byte("k")

	release := q.beginWrite(keysFirstArg, [][]byte{key})
	var entered atomic.Bool
	isBlocked, finished := blocked(func() {
		q.gate.Lock()
		entered.Store(true)
		q.gate.Unlock()
	})
	if !isBlocked {
		t.Fatal("EXEC did not wait for a write in progress")
	}
	release()
	if !finished() || !entered.Load() {
		t.Fatal("EXEC never proceeded after the write finished")
	}

	q.gate.Lock()
	var read atomic.Bool
	if isBlocked, _ := blocked(func() { q.gate.RLock(); read.Store(true); q.gate.RUnlock() }); !isBlocked {
		t.Fatal("a read ran while a transaction held the gate")
	}
	q.gate.Unlock()
}

func TestSequencerSkipsWriteOrderingWhenNothingRecordsWrites(t *testing.T) {
	q := newSequencer()
	recording := false
	q.needsOrder = func() bool { return recording }
	key := []byte("k")

	first := q.beginWrite(keysFirstArg, [][]byte{key})
	if isBlocked, _ := blocked(func() { q.beginWrite(keysFirstArg, [][]byte{key})() }); isBlocked {
		t.Fatal("two writes to one key were ordered although nothing records writes")
	}
	first()

	recording = true
	first = q.beginWrite(keysFirstArg, [][]byte{key})
	if isBlocked, _ := blocked(func() { q.beginWrite(keysFirstArg, [][]byte{key})() }); !isBlocked {
		t.Fatal("two writes to one key were not ordered although a log is recording them")
	}
	first()
}

func TestEveryDurableCommandDeclaresHowToOrderItsWrites(t *testing.T) {
	// A command added without a key shape is ordered against every write, which
	// is correct but slow. This lists the shapes the current commands have, so a
	// change to one is a decision rather than an accident.
	executor := NewExecutor(nil, nil)
	want := map[string]keyShape{
		"SET": keysFirstArg, "SETBIT": keysFirstArg, "PFADD": keysFirstArg, "INCR": keysFirstArg,
		"LPUSH": keysFirstArg, "RPUSH": keysFirstArg, "LPOP": keysFirstArg, "RPOP": keysFirstArg,
		"ZADD": keysFirstArg, "GEOADD": keysFirstArg, "XADD": keysFirstArg, "HSET": keysFirstArg,
		"HDEL": keysFirstArg, "SADD": keysFirstArg, "SREM": keysFirstArg, "PUBLISH": keysFirstArg,
		"DEL": keysEveryArg,
	}

	for name, spec := range executor.commands {
		if !spec.durable && !spec.propagates {
			continue
		}
		shape, listed := want[name]
		if !listed {
			t.Errorf("%s is durable or propagates but this test does not know its key shape; add it (a new command is ordered against every write until it declares one)", name)
			continue
		}
		if spec.keys != shape {
			t.Errorf("%s key shape = %d, want %d", name, spec.keys, shape)
		}
	}
	for name := range want {
		if spec, ok := executor.commands[name]; !ok || (!spec.durable && !spec.propagates) {
			t.Errorf("%s is listed here but is not a durable or propagating command any more", name)
		}
	}
}

func TestSequenceModeOfRequests(t *testing.T) {
	executor := NewExecutor(nil, nil)
	tests := []struct {
		name string
		want sequenceMode
	}{
		{"GET", sequenceRead},
		{"PING", sequenceRead},
		{"SET", sequenceWrite},
		{"DEL", sequenceWrite},
		{"EXEC", sequenceExclusive},
		{"BLPOP", sequenceSelf},
		{"WAIT", sequenceSelf},
		{"NOSUCHCOMMAND", sequenceRead},
	}
	for _, tt := range tests {
		if got := executor.sequenceModeFor(context.Background(), &Request{Name: tt.name}); got != tt.want {
			t.Errorf("sequenceModeFor(%s) = %d, want %d", tt.name, got, tt.want)
		}
	}
}
