package storage

import (
	"errors"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"
)

// TestStoreMaxMemoryEvictsLeastRecentlyUsedKeysToMakeRoom pins the behaviour of a
// write that does not fit: the least recently used keys go, oldest first, until
// it does, and the store ends within its limit. The sample is larger than the
// keyspace, so the choice of victim is deterministic.
func TestStoreMaxMemoryEvictsLeastRecentlyUsedKeysToMakeRoom(t *testing.T) {
	store := NewStore()
	payload := []byte(strings.Repeat("x", 64))
	keys := make([]string, 20)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%02d", i)
		if _, err := store.Set(keys[i], payload, 0); err != nil {
			t.Fatalf("Set(%s) error = %v", keys[i], err)
		}
	}
	store.ConfigureMaxMemory(1<<40, 1000)
	limit := store.UsedMemory()
	store.ConfigureMaxMemory(limit, 1000)
	for i, key := range keys {
		store.valueObjectForTest(key).touch(int64(1000 + i)) // key-00 is the stalest
	}

	evicted, err := store.Set("a-new-key", payload, 0)
	if err != nil {
		t.Fatalf("Set() at the limit error = %v, want it to evict and succeed", err)
	}
	if len(evicted) == 0 {
		t.Fatal("Set() at the limit evicted nothing")
	}
	for i, key := range evicted {
		if key != keys[i] {
			t.Fatalf("evicted[%d] = %q, want %q: victims must go least recently used first (evicted %v)", i, key, keys[i], evicted)
		}
	}
	if used := store.UsedMemory(); used > limit {
		t.Fatalf("UsedMemory() = %d after eviction, want at most the limit %d", used, limit)
	}
	if _, ok, _ := store.Get("a-new-key"); !ok {
		t.Fatal("the new key is missing after the write that evicted to make room for it")
	}
	for _, key := range evicted {
		if _, ok, _ := store.Get(key); ok {
			t.Fatalf("evicted key %q is still readable", key)
		}
	}
	for _, key := range keys[len(evicted):] {
		if _, ok, _ := store.Get(key); !ok {
			t.Fatalf("key %q was evicted although the write did not need its memory", key)
		}
	}
}

// TestStoreMaxMemoryRefusesAnImpossibleWriteWithoutEvicting pins that a write
// whose own key cannot fit is refused up front: nothing is evicted in a doomed
// attempt to make room, and the key keeps its old value.
func TestStoreMaxMemoryRefusesAnImpossibleWriteWithoutEvicting(t *testing.T) {
	store := NewStore()
	if _, err := store.Set("target", []byte("small"), 0); err != nil {
		t.Fatalf("Set(target) error = %v", err)
	}
	for i := 0; i < 10; i++ {
		if _, err := store.Set(fmt.Sprintf("bystander-%d", i), []byte("v"), 0); err != nil {
			t.Fatalf("Set(bystander) error = %v", err)
		}
	}
	store.ConfigureMaxMemory(1<<40, 1000)
	store.ConfigureMaxMemory(store.UsedMemory(), 1000)
	before := store.Len()

	evicted, err := store.Set("target", []byte(strings.Repeat("y", 1<<16)), 0)
	if !errors.Is(err, ErrMemoryLimitExceeded) {
		t.Fatalf("Set() error = %v, want ErrMemoryLimitExceeded", err)
	}
	if len(evicted) != 0 {
		t.Fatalf("a refused write reported evictions %v", evicted)
	}
	if store.Len() != before {
		t.Fatalf("Len() = %d after a refused write, want %d: nothing may be evicted for a write that cannot succeed", store.Len(), before)
	}
	if got, _, _ := store.Get("target"); string(got) != "small" {
		t.Fatalf("Get(target) = %q, want the old value kept", got)
	}
}

// TestStoreAccountedWriteSweepsAKeyOnceItsDeadlinePasses covers a key stored with
// a deadline still in the future. An accounted write sweeps expired keys, but only
// once one can have expired, so the sweep has to happen on the first write after
// the deadline and not before it.
func TestStoreAccountedWriteSweepsAKeyOnceItsDeadlinePasses(t *testing.T) {
	store := NewStore()
	store.ConfigureMaxMemory(1<<20, 16)
	reported := make(chan []string, 4)
	store.SetExpirationListener(func(keys []string) { reported <- keys })

	if _, err := store.Set("soon", []byte("v"), time.Now().Add(60*time.Millisecond).UnixMilli()); err != nil {
		t.Fatalf("Set(soon) error = %v", err)
	}
	if _, err := store.Set("later", []byte("v"), time.Now().Add(time.Hour).UnixMilli()); err != nil {
		t.Fatalf("Set(later) error = %v", err)
	}

	// Before the deadline: still there, nothing swept.
	if _, err := store.Set("other-1", []byte("v"), 0); err != nil {
		t.Fatalf("Set(other-1) error = %v", err)
	}
	select {
	case keys := <-reported:
		t.Fatalf("a write before any deadline swept %v", keys)
	default:
	}
	if store.valueObjectForTest("soon") == nil {
		t.Fatal("the key was swept before its deadline")
	}

	time.Sleep(90 * time.Millisecond)
	if _, err := store.Set("other-2", []byte("v"), 0); err != nil {
		t.Fatalf("Set(other-2) error = %v", err)
	}
	select {
	case keys := <-reported:
		if len(keys) != 1 || keys[0] != "soon" {
			t.Fatalf("swept %v, want [soon]", keys)
		}
	case <-time.After(time.Second):
		t.Fatal("the first write after the deadline did not sweep the expired key")
	}
	if store.valueObjectForTest("later") == nil {
		t.Fatal("the swept keyspace lost a key whose deadline had not passed")
	}
	if used, want := store.UsedMemory(), recountUsedMemory(store); used != want {
		t.Fatalf("UsedMemory() = %d after the sweep, recount = %d", used, want)
	}
}

// TestAWriteUnderMaxmemoryDoesNotSweepTheKeyItFoundLive covers a write that finds
// its key live, with a TTL deadline that passes before the write commits. The
// write is anchored to the one clock reading writeKey took, so the sweep it runs
// to make room must use that reading too: with a second one it removed the very
// key being written, reported it to the expiration listener (which logs a DEL
// ahead of the write), and left the memory counter short by the size of the
// value it then replaced.
//
// The write is parked on the last shard lock after it holds the first, which is
// after its clock reading, so the deadline passes at a point the test chooses.
func TestAWriteUnderMaxmemoryDoesNotSweepTheKeyItFoundLive(t *testing.T) {
	tests := []struct {
		name  string
		write func(store *Store, deadline int64) error
		want  string
	}{
		{
			name: "INCR that adds a digit",
			write: func(store *Store, _ int64) error {
				_, _, err := store.Increment("k")
				return err
			},
			want: "10",
		},
		{
			name: "SETBIT past the end of the string",
			write: func(store *Store, _ int64) error {
				_, _, err := store.SetBit("k", 15, 1)
				return err
			},
			want: "9\x01",
		},
		{
			name: "SET to a longer value with a TTL",
			write: func(store *Store, deadline int64) error {
				_, err := store.Set("k", []byte("a longer value"), deadline)
				return err
			},
			want: "a longer value",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			// A machine stalled for longer than the margin below can run the write's
			// clock reading after the deadline, which is a different test: start
			// that attempt over rather than judge it.
			const attempts = 5
			for attempt := 0; attempt < attempts; attempt++ {
				store := NewStore()
				store.ConfigureMaxMemory(1<<20, 16)
				var heard []string
				store.SetExpirationListener(func(keys []string) { heard = append(heard, keys...) })

				deadline := time.Now().UnixMilli() + 100
				if _, err := store.Set("k", []byte("9"), deadline); err != nil {
					t.Fatalf("Set(k) error = %v", err)
				}

				// The write takes the shard locks in ascending order, so once it holds
				// shard 0 it has read its clock and waits for the last one.
				last := &store.shards[len(store.shards)-1]
				last.mu.Lock()
				done := make(chan error, 1)
				go func() { done <- tc.write(store, deadline) }()
				for store.shards[0].mu.TryLock() {
					store.shards[0].mu.Unlock()
					runtime.Gosched()
				}
				readClockBeforeDeadline := time.Now().UnixMilli() <= deadline
				for time.Now().UnixMilli() <= deadline {
					runtime.Gosched()
				}
				last.mu.Unlock()

				if err := <-done; err != nil {
					t.Fatalf("write error = %v", err)
				}
				if !readClockBeforeDeadline {
					continue
				}

				if len(heard) != 0 {
					t.Errorf("the expiration listener heard %v, want nothing: the write found %q live", heard, "k")
				}
				stored := store.valueObjectForTest("k")
				if stored == nil || string(stored.String) != tc.want {
					t.Errorf("stored value = %+v, want %q", stored, tc.want)
				}
				if used, want := store.UsedMemory(), recountUsedMemory(store); used != want {
					t.Errorf("UsedMemory() = %d, recount = %d (drift %d)", used, want, used-want)
				}
				return
			}
			t.Fatalf("the write was not parked before its deadline in %d attempts", attempts)
		})
	}
}

// TestStoreExpiryLowerBound pins how the earliest-deadline bound is kept: unknown
// (zero) until the first recount, exact after one, lowered by an earlier deadline
// and untouched by a later one, and not tracked at all while maxmemory is off.
func TestStoreExpiryLowerBound(t *testing.T) {
	soon := time.Now().Add(time.Hour).UnixMilli()
	later := soon + 60_000

	store := NewStore()
	if _, err := store.Set("off", []byte("v"), soon); err != nil {
		t.Fatalf("Set() error = %v", err)
	}
	if got := store.nextExpiry.Load(); got != 0 {
		t.Fatalf("nextExpiry = %d with maxmemory off, want it untracked (0)", got)
	}

	store.ConfigureMaxMemory(1<<20, 16)
	if got := store.nextExpiry.Load(); got != soon {
		t.Fatalf("nextExpiry = %d after enabling maxmemory, want the exact earliest deadline %d", got, soon)
	}
	if _, err := store.Set("later", []byte("v"), later); err != nil {
		t.Fatalf("Set() error = %v", err)
	}
	if got := store.nextExpiry.Load(); got != soon {
		t.Fatalf("nextExpiry = %d after storing a later deadline, want it unchanged at %d", got, soon)
	}
	earlier := soon - 1000
	if _, err := store.Set("earlier", []byte("v"), earlier); err != nil {
		t.Fatalf("Set() error = %v", err)
	}
	if got := store.nextExpiry.Load(); got != earlier {
		t.Fatalf("nextExpiry = %d after storing an earlier deadline, want %d", got, earlier)
	}

	store.ConfigureMaxMemory(0, 16)
	store.ConfigureMaxMemory(1<<20, 16)
	store.Delete("earlier")
	store.Delete("off")
	store.ConfigureMaxMemory(1<<20, 16)
	if got := store.nextExpiry.Load(); got != later {
		t.Fatalf("nextExpiry = %d after a recount without the earlier keys, want %d", got, later)
	}
}

// TestAccountedInsertsDoNotScanTheKeyspace guards the cost of a write under
// maxmemory. Each growing write used to measure every key, about 7 ms at 100,000
// keys, so 300 inserts took about two seconds; they now cost microseconds
// whatever the keyspace holds.
func TestAccountedInsertsDoNotScanTheKeyspace(t *testing.T) {
	store := NewStore()
	value := []byte("value-of-about-thirty-two-bytes!")
	for i := 0; i < 100_000; i++ {
		if _, err := store.Set(fmt.Sprintf("seed-%d", i), value, 0); err != nil {
			t.Fatalf("Set() error = %v", err)
		}
	}
	store.ConfigureMaxMemory(1<<40, 16)

	start := time.Now()
	for i := 0; i < 300; i++ {
		if _, err := store.Set(fmt.Sprintf("new-%d", i), value, 0); err != nil {
			t.Fatalf("Set() error = %v", err)
		}
	}
	if elapsed := time.Since(start); elapsed > 400*time.Millisecond {
		t.Fatalf("300 inserts into a 100,000 key store with maxmemory on took %v, want them independent of the keyspace size (about 2 s when each scanned it)", elapsed)
	}
}
