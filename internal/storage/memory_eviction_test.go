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

// TestAWriteUnderMaxmemoryDoesNotSweepTheKeyItFoundLive covers a growing write
// to a key whose TTL deadline passes while the write runs. Under maxmemory the
// write sweeps expired keys to make room, and that sweep judges expiry by the
// clock reading at which the write found its key live: the key is neither
// reported to the expiration listener nor counted out of used memory.
func TestAWriteUnderMaxmemoryDoesNotSweepTheKeyItFoundLive(t *testing.T) {
	tests := []struct {
		name  string
		write func(store *Store, key string) error
	}{
		{
			name: "INCR that adds a digit",
			write: func(store *Store, key string) error {
				_, _, err := store.Increment(key)
				return err
			},
		},
		{
			name: "SETBIT past the end",
			write: func(store *Store, key string) error {
				_, _, err := store.SetBit(key, 15, 1)
				return err
			},
		},
		{
			name: "SET to a longer value with a TTL",
			write: func(store *Store, key string) error {
				_, err := store.Set(key, []byte("longer"), time.Now().Add(time.Hour).UnixMilli())
				return err
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			store, key, heard := writeAcrossItsKeysDeadline(t, tt.write)

			if len(heard) != 0 {
				t.Errorf("the expiration listener heard %v, want nothing: the write found %s live", heard, key)
			}
			if used, want := store.UsedMemory(), recountUsedMemory(store); used != want {
				t.Errorf("UsedMemory() = %d after the write, recount = %d", used, want)
			}
		})
	}
}

// writeAcrossItsKeysDeadline stores "9" under a key on the last shard, with a
// TTL deadline an hour out, under maxmemory. It runs write on that key so that
// the write takes its clock reading at or before the key's deadline and makes
// room after it, and returns the store, the key, and the keys the expiration
// listener heard.
//
// An accounted write reads the clock and then takes every shard lock in
// ascending order, so while the test holds the last shard's lock the write
// parks there, holding shard 0 and past its clock reading. The test then moves
// the key's deadline to a clock reading of its own, which cannot precede the
// write's, under that same lock, and lets the write go on once the clock has
// passed it.
func writeAcrossItsKeysDeadline(t *testing.T, write func(store *Store, key string) error) (*Store, string, []string) {
	t.Helper()

	store := NewStore()
	store.ConfigureMaxMemory(1<<20, 16)
	var heard []string
	store.SetExpirationListener(func(keys []string) { heard = append(heard, keys...) })

	lastIndex := len(store.shards) - 1
	key := ""
	for i := 0; key == ""; i++ {
		if candidate := fmt.Sprintf("k%d", i); store.shardIndex(candidate) == lastIndex {
			key = candidate
		}
	}
	if _, err := store.Set(key, []byte("9"), time.Now().Add(time.Hour).UnixMilli()); err != nil {
		t.Fatalf("Set(%s) error = %v", key, err)
	}

	last := &store.shards[lastIndex]
	last.mu.Lock()
	done := make(chan error, 1)
	go func() { done <- write(store, key) }()
	for store.shards[0].mu.TryLock() {
		store.shards[0].mu.Unlock()
		runtime.Gosched()
	}
	deadline := time.Now().UnixMilli()
	last.data[key].ExpiresAt = deadline
	store.noteExpiry(deadline)
	for time.Now().UnixMilli() <= deadline {
		runtime.Gosched()
	}
	last.mu.Unlock()

	if err := <-done; err != nil {
		t.Fatalf("the write failed: %v", err)
	}
	return store, key, heard
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
