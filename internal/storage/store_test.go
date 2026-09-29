package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestStoreValueBehavior(t *testing.T) {
	tests := []struct {
		name string
		run  func(*testing.T, *Store)
	}{
		{
			name: "Set/Get copies input value",
			run: func(t *testing.T, store *Store) {
				t.Helper()
				payload := []byte("hello")
				_, _ = store.Set("greeting", payload, 0)
				payload[0] = 'H'

				got, ok, err := store.Get("greeting")
				if err != nil {
					t.Fatalf("Get() error = %v", err)
				}
				if !ok {
					t.Fatal("Get() ok = false, want true")
				}
				if string(got) != "hello" {
					t.Fatalf("Get() value = %q, want %q", string(got), "hello")
				}
			},
		},
		{
			name: "Get returns defensive copy",
			run: func(t *testing.T, store *Store) {
				t.Helper()
				_, _ = store.Set("greeting", []byte("hello"), 0)

				got, ok, err := store.Get("greeting")
				if err != nil {
					t.Fatalf("Get() error = %v", err)
				}
				if !ok {
					t.Fatal("Get() ok = false, want true")
				}

				got[0] = 'H'

				again, ok, err := store.Get("greeting")
				if err != nil {
					t.Fatalf("second Get() error = %v", err)
				}
				if !ok {
					t.Fatal("second Get() ok = false, want true")
				}
				if string(again) != "hello" {
					t.Fatalf("second Get() value = %q, want %q", string(again), "hello")
				}
			},
		},
		{
			name: "Get passively evicts expired key",
			run: func(t *testing.T, store *Store) {
				t.Helper()
				_, _ = store.Set("expired", []byte("value"), time.Now().Add(-time.Millisecond).UnixMilli())

				if _, ok, err := store.Get("expired"); err != nil {
					t.Fatalf("Get() error = %v", err)
				} else if ok {
					t.Fatal("Get() ok = true, want false for expired key")
				}
				if store.Len() != 0 {
					t.Fatalf("Len() = %d, want 0 after passive eviction", store.Len())
				}
			},
		},
		{
			name: "Delete treats expired key as absent",
			run: func(t *testing.T, store *Store) {
				t.Helper()
				_, _ = store.Set("expired", []byte("value"), time.Now().Add(-time.Millisecond).UnixMilli())

				if ok := store.Delete("expired"); ok {
					t.Fatal("Delete() ok = true, want false for expired key")
				}
				if store.Len() != 0 {
					t.Fatalf("Len() = %d, want 0 after deleting expired key", store.Len())
				}
			},
		},
		{
			name: "Increment initializes and updates integer strings",
			run: func(t *testing.T, store *Store) {
				t.Helper()

				first, _, err := store.Increment("counter")
				if err != nil {
					t.Fatalf("Increment() first error = %v", err)
				}
				if first != 1 {
					t.Fatalf("Increment() first = %d, want 1", first)
				}

				second, _, err := store.Increment("counter")
				if err != nil {
					t.Fatalf("Increment() second error = %v", err)
				}
				if second != 2 {
					t.Fatalf("Increment() second = %d, want 2", second)
				}
			},
		},
		{
			name: "Increment rejects non-integer strings",
			run: func(t *testing.T, store *Store) {
				t.Helper()
				_, _ = store.Set("counter", []byte("hello"), 0)

				if _, _, err := store.Increment("counter"); !errors.Is(err, ErrValueNotInteger) {
					t.Fatalf("Increment() error = %v, want ErrValueNotInteger", err)
				}
			},
		},
		{
			name: "Get rejects list values with wrong type",
			run: func(t *testing.T, store *Store) {
				t.Helper()
				if _, _, err := store.LeftPush("numbers", [][]byte{[]byte("one")}); err != nil {
					t.Fatalf("LeftPush() error = %v", err)
				}

				if _, _, err := store.Get("numbers"); !errors.Is(err, ErrWrongType) {
					t.Fatalf("Get() error = %v, want ErrWrongType", err)
				}
			},
		},
		{
			name: "SetBit expands sparse strings and GetBit reads unset bits",
			run: func(t *testing.T, store *Store) {
				t.Helper()

				previous, _, err := store.SetBit("bitmap", 16, 1)
				if err != nil {
					t.Fatalf("SetBit() error = %v", err)
				}
				if previous != 0 {
					t.Fatalf("SetBit() previous = %d, want 0", previous)
				}

				got, ok, err := store.Get("bitmap")
				if err != nil {
					t.Fatalf("Get() error = %v", err)
				}
				if !ok {
					t.Fatal("Get() ok = false, want true")
				}
				if want := []byte{0, 0, 0x80}; !bytes.Equal(got, want) {
					t.Fatalf("Get() value = %v, want %v", got, want)
				}

				bit, _, err := store.GetBit("bitmap", 15)
				if err != nil {
					t.Fatalf("GetBit() error = %v", err)
				}
				if bit != 0 {
					t.Fatalf("GetBit(15) = %d, want 0", bit)
				}

				bit, _, err = store.GetBit("bitmap", 16)
				if err != nil {
					t.Fatalf("GetBit() error = %v", err)
				}
				if bit != 1 {
					t.Fatalf("GetBit(16) = %d, want 1", bit)
				}
			},
		},
		{
			name: "SetBit returns previous bit when overwriting",
			run: func(t *testing.T, store *Store) {
				t.Helper()

				if previous, _, err := store.SetBit("bitmap", 7, 1); err != nil || previous != 0 {
					t.Fatalf("first SetBit() previous = %d, error = %v, want 0 nil", previous, err)
				}
				if previous, _, err := store.SetBit("bitmap", 7, 0); err != nil || previous != 1 {
					t.Fatalf("second SetBit() previous = %d, error = %v, want 1 nil", previous, err)
				}
			},
		},
		{
			name: "GetBit and BitCount return zero for missing keys",
			run: func(t *testing.T, store *Store) {
				t.Helper()

				bit, ok, err := store.GetBit("missing", 12)
				if err != nil {
					t.Fatalf("GetBit() error = %v", err)
				}
				if ok || bit != 0 {
					t.Fatalf("GetBit() = %d, %v, want 0 false", bit, ok)
				}

				count, ok, err := store.BitCount("missing", nil, nil)
				if err != nil {
					t.Fatalf("BitCount() error = %v", err)
				}
				if ok || count != 0 {
					t.Fatalf("BitCount() = %d, %v, want 0 false", count, ok)
				}
			},
		},
		{
			name: "BitCount supports inclusive and negative byte ranges",
			run: func(t *testing.T, store *Store) {
				t.Helper()
				_, _ = store.Set("bitmap", []byte{0xff, 0x00, 0x0f}, 0)

				count, _, err := store.BitCount("bitmap", nil, nil)
				if err != nil {
					t.Fatalf("BitCount() error = %v", err)
				}
				if count != 12 {
					t.Fatalf("BitCount() = %d, want 12", count)
				}

				start, end := int64(1), int64(2)
				count, _, err = store.BitCount("bitmap", &start, &end)
				if err != nil {
					t.Fatalf("BitCount(range) error = %v", err)
				}
				if count != 4 {
					t.Fatalf("BitCount(1,2) = %d, want 4", count)
				}

				start, end = -1, -1
				count, _, err = store.BitCount("bitmap", &start, &end)
				if err != nil {
					t.Fatalf("BitCount(negative range) error = %v", err)
				}
				if count != 4 {
					t.Fatalf("BitCount(-1,-1) = %d, want 4", count)
				}

				start, end = 5, 1
				count, _, err = store.BitCount("bitmap", &start, &end)
				if err != nil {
					t.Fatalf("BitCount(empty range) error = %v", err)
				}
				if count != 0 {
					t.Fatalf("BitCount(5,1) = %d, want 0", count)
				}
			},
		},
		{
			name: "Bitmap ops reject wrong value type",
			run: func(t *testing.T, store *Store) {
				t.Helper()
				if _, _, err := store.LeftPush("numbers", [][]byte{[]byte("one")}); err != nil {
					t.Fatalf("LeftPush() error = %v", err)
				}

				if _, _, err := store.GetBit("numbers", 0); !errors.Is(err, ErrWrongType) {
					t.Fatalf("GetBit() error = %v, want ErrWrongType", err)
				}
				if _, _, err := store.SetBit("numbers", 0, 1); !errors.Is(err, ErrWrongType) {
					t.Fatalf("SetBit() error = %v, want ErrWrongType", err)
				}
				if _, _, err := store.BitCount("numbers", nil, nil); !errors.Is(err, ErrWrongType) {
					t.Fatalf("BitCount() error = %v, want ErrWrongType", err)
				}
			},
		},
		{
			name: "Bitmap ops reject invalid direct arguments",
			run: func(t *testing.T, store *Store) {
				t.Helper()

				if _, _, err := store.GetBit("bitmap", -1); !errors.Is(err, ErrValueNotInteger) {
					t.Fatalf("GetBit() error = %v, want ErrValueNotInteger", err)
				}
				if _, _, err := store.SetBit("bitmap", -1, 1); !errors.Is(err, ErrValueNotInteger) {
					t.Fatalf("SetBit(negative offset) error = %v, want ErrValueNotInteger", err)
				}
				if _, _, err := store.SetBit("bitmap", 0, 2); !errors.Is(err, ErrValueNotInteger) {
					t.Fatalf("SetBit(invalid bit) error = %v, want ErrValueNotInteger", err)
				}
				if _, _, err := store.SetBit("bitmap", MaxBitmapOffset+1, 1); !errors.Is(err, ErrValueNotInteger) {
					t.Fatalf("SetBit() error = %v, want ErrValueNotInteger", err)
				}
			},
		},
		{
			name: "SetBit leaves the stored payload intact while accounting",
			run: func(t *testing.T, store *Store) {
				t.Helper()
				if _, err := store.Set("bitmap", []byte{0x00}, 0); err != nil {
					t.Fatalf("Set() error = %v", err)
				}
				store.ConfigureMaxMemory(1<<20, 16)
				stored := store.valueObjectForTest("bitmap").String

				if _, _, err := store.SetBit("bitmap", 0, 1); err != nil {
					t.Fatalf("SetBit() error = %v", err)
				}

				// Accounting sizes a write against the value already under the
				// key, so an in-range SETBIT must commit a replacement rather
				// than edit that value where it lies.
				if stored[0] != 0x00 {
					t.Fatalf("pre-write payload = %#x, want 0x00 untouched", stored[0])
				}
				if got, _, err := store.Get("bitmap"); err != nil {
					t.Fatalf("Get() error = %v", err)
				} else if got[0] != 0x80 {
					t.Fatalf("Get() = %#x, want 0x80", got[0])
				}
			},
		},
		{
			name: "SetBit checks memory before sparse allocation",
			run: func(t *testing.T, store *Store) {
				t.Helper()
				store.ConfigureMaxMemory(1, 16)

				// The rejected payload is half a gigabyte. Allocating it before
				// checking would zero it under every shard write lock, so the
				// test measures that it was never allocated, not only that the
				// call failed.
				var before, after runtime.MemStats
				runtime.ReadMemStats(&before)
				_, _, err := store.SetBit("bitmap", MaxBitmapOffset, 1)
				runtime.ReadMemStats(&after)

				if !errors.Is(err, ErrMemoryLimitExceeded) {
					t.Fatalf("SetBit() error = %v, want ErrMemoryLimitExceeded", err)
				}
				const allocationBudget = 8 << 20
				if allocated := after.TotalAlloc - before.TotalAlloc; allocated > allocationBudget {
					t.Fatalf("SetBit() allocated %d bytes, want at most %d for a rejected write", allocated, allocationBudget)
				}
				if _, ok, err := store.Get("bitmap"); err != nil {
					t.Fatalf("Get() error = %v", err)
				} else if ok {
					t.Fatal("Get() ok = true, want false after rejected sparse SetBit")
				}
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			store := NewStore()
			tt.run(t, store)
		})
	}
}

func TestStoreAccessTracking(t *testing.T) {
	tests := []struct {
		name string
		run  func(*testing.T, *Store, string)
	}{
		{
			name: "Get refreshes string last accessed",
			run: func(t *testing.T, store *Store, key string) {
				t.Helper()
				_, _ = store.Set(key, []byte("Stash"), 0)

				before := store.lastAccessedAtForTest(key)
				time.Sleep(2 * time.Millisecond)

				if _, ok, err := store.Get(key); err != nil {
					t.Fatalf("Get() error = %v", err)
				} else if !ok {
					t.Fatal("Get() ok = false, want true")
				}

				after := store.lastAccessedAtForTest(key)
				if after <= before {
					t.Fatalf("lastAccessedAt after Get = %d, want > %d", after, before)
				}
			},
		},
		{
			name: "ListRange refreshes list last accessed",
			run: func(t *testing.T, store *Store, key string) {
				t.Helper()
				if _, _, err := store.RightPush(key, [][]byte{[]byte("a"), []byte("b")}); err != nil {
					t.Fatalf("RightPush() error = %v", err)
				}

				before := store.lastAccessedAtForTest(key)
				time.Sleep(2 * time.Millisecond)

				if _, err := store.ListRange(key, 0, -1); err != nil {
					t.Fatalf("ListRange() error = %v", err)
				}

				after := store.lastAccessedAtForTest(key)
				if after <= before {
					t.Fatalf("lastAccessedAt after ListRange = %d, want > %d", after, before)
				}
			},
		},
		{
			name: "HGet refreshes hash last accessed even for missing fields",
			run: func(t *testing.T, store *Store, key string) {
				t.Helper()
				if _, _, err := store.HSet(key, []HashFieldValue{{Field: "lang", Value: []byte("go")}}); err != nil {
					t.Fatalf("HSet() error = %v", err)
				}

				before := store.lastAccessedAtForTest(key)
				time.Sleep(2 * time.Millisecond)

				if _, _, err := store.HGet(key, "missing"); err != nil {
					t.Fatalf("HGet() error = %v", err)
				}

				after := store.lastAccessedAtForTest(key)
				if after <= before {
					t.Fatalf("lastAccessedAt after HGet = %d, want > %d", after, before)
				}
			},
		},
		{
			name: "SIsMember refreshes set last accessed for misses",
			run: func(t *testing.T, store *Store, key string) {
				t.Helper()
				if _, _, err := store.SAdd(key, [][]byte{[]byte("alpha")}); err != nil {
					t.Fatalf("SAdd() error = %v", err)
				}

				before := store.lastAccessedAtForTest(key)
				time.Sleep(2 * time.Millisecond)

				if _, err := store.SIsMember(key, []byte("beta")); err != nil {
					t.Fatalf("SIsMember() error = %v", err)
				}

				after := store.lastAccessedAtForTest(key)
				if after <= before {
					t.Fatalf("lastAccessedAt after SIsMember = %d, want > %d", after, before)
				}
			},
		},
		{
			name: "ZRange refreshes sorted set last accessed",
			run: func(t *testing.T, store *Store, key string) {
				t.Helper()
				if _, _, err := store.ZAdd(key, []ZSetEntry{{Member: []byte("alpha"), Score: 1}}); err != nil {
					t.Fatalf("ZAdd() error = %v", err)
				}

				before := store.lastAccessedAtForTest(key)
				time.Sleep(2 * time.Millisecond)

				if _, err := store.ZRange(key, 0, -1); err != nil {
					t.Fatalf("ZRange() error = %v", err)
				}

				after := store.lastAccessedAtForTest(key)
				if after <= before {
					t.Fatalf("lastAccessedAt after ZRange = %d, want > %d", after, before)
				}
			},
		},
		{
			name: "XRead refreshes stream last accessed when no newer entries exist",
			run: func(t *testing.T, store *Store, key string) {
				t.Helper()
				if _, _, err := store.XAdd(key, "1-0", [][]byte{[]byte("field"), []byte("value")}); err != nil {
					t.Fatalf("XAdd() error = %v", err)
				}

				before := store.lastAccessedAtForTest(key)
				time.Sleep(2 * time.Millisecond)

				if _, err := store.XRead(key, "$"); err != nil {
					t.Fatalf("XRead() error = %v", err)
				}

				after := store.lastAccessedAtForTest(key)
				if after <= before {
					t.Fatalf("lastAccessedAt after XRead = %d, want > %d", after, before)
				}
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			store := NewStore()
			tt.run(t, store, "phase11")
		})
	}
}

func TestStoreMaxMemoryAccountingOnLegacyMutators(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, *Store)
	}{
		{
			name: "Set updates approximate usage",
			mutate: func(t *testing.T, store *Store) {
				t.Helper()
				_, _ = store.Set("name", []byte("Stash"), 0)
			},
		},
		{
			name: "Increment updates approximate usage",
			mutate: func(t *testing.T, store *Store) {
				t.Helper()
				if _, _, err := store.Increment("counter"); err != nil {
					t.Fatalf("Increment() error = %v", err)
				}
			},
		},
		{
			name: "RightPush updates approximate usage",
			mutate: func(t *testing.T, store *Store) {
				t.Helper()
				if _, _, err := store.RightPush("jobs", [][]byte{[]byte("build")}); err != nil {
					t.Fatalf("RightPush() error = %v", err)
				}
			},
		},
		{
			name: "HSet updates approximate usage",
			mutate: func(t *testing.T, store *Store) {
				t.Helper()
				if _, _, err := store.HSet("profile", []HashFieldValue{{Field: "lang", Value: []byte("go")}}); err != nil {
					t.Fatalf("HSet() error = %v", err)
				}
			},
		},
		{
			name: "SAdd updates approximate usage",
			mutate: func(t *testing.T, store *Store) {
				t.Helper()
				if _, _, err := store.SAdd("tags", [][]byte{[]byte("fast")}); err != nil {
					t.Fatalf("SAdd() error = %v", err)
				}
			},
		},
		{
			name: "ZAdd updates approximate usage",
			mutate: func(t *testing.T, store *Store) {
				t.Helper()
				if _, _, err := store.ZAdd("leaders", []ZSetEntry{{Member: []byte("alpha"), Score: 1}}); err != nil {
					t.Fatalf("ZAdd() error = %v", err)
				}
			},
		},
		{
			name: "XAdd updates approximate usage",
			mutate: func(t *testing.T, store *Store) {
				t.Helper()
				if _, _, err := store.XAdd("events", "1-0", [][]byte{[]byte("field"), []byte("value")}); err != nil {
					t.Fatalf("XAdd() error = %v", err)
				}
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			store := NewStore()
			store.ConfigureMaxMemory(1<<30, 16)

			before := store.UsedMemory()
			tt.mutate(t, store)
			after := store.UsedMemory()
			if after <= before {
				t.Fatalf("UsedMemory() after mutation = %d, want > %d", after, before)
			}
		})
	}
}

func TestStoreMaxMemoryAccountingReclaimsExpiredBytesOnLegacyWrite(t *testing.T) {
	store := NewStore()
	store.ConfigureMaxMemory(1<<30, 16)

	_, _ = store.Set("stale", []byte(strings.Repeat("x", 4096)), time.Now().Add(-time.Millisecond).UnixMilli())
	before := store.UsedMemory()
	if before == 0 {
		t.Fatal("UsedMemory() before expired replacement = 0, want non-zero")
	}

	if _, _, err := store.ZAdd("stale", []ZSetEntry{{Member: []byte("fresh"), Score: 1}}); err != nil {
		t.Fatalf("ZAdd() error = %v", err)
	}

	after := store.UsedMemory()
	if after >= before {
		t.Fatalf("UsedMemory() after expired replacement = %d, want < %d", after, before)
	}
}

func TestStoreDropIfStillExpired(t *testing.T) {
	// dropIfStillExpired runs after a reader saw the key expired and released
	// its read lock. Taking the write lock is a separate acquisition, so the
	// key may have been deleted, rewritten or renewed in between and the
	// decision has to be remade from a fresh read rather than trusting the
	// reader. Each case below is one thing that can have happened in that gap.
	tests := []struct {
		name      string
		setup     func(*testing.T, *Store)
		wantFound bool
		wantValue string
	}{
		{
			name: "reclaims a key that is still expired",
			setup: func(t *testing.T, store *Store) {
				t.Helper()
				_, _ = store.Set("key", []byte("stale"), time.Now().Add(-time.Millisecond).UnixMilli())
			},
			wantFound: false,
		},
		{
			name: "keeps a key rewritten without an expiry",
			setup: func(t *testing.T, store *Store) {
				t.Helper()
				_, _ = store.Set("key", []byte("fresh"), 0)
			},
			wantFound: true,
			wantValue: "fresh",
		},
		{
			name: "keeps a key whose expiry moved into the future",
			setup: func(t *testing.T, store *Store) {
				t.Helper()
				_, _ = store.Set("key", []byte("renewed"), time.Now().Add(time.Hour).UnixMilli())
			},
			wantFound: true,
			wantValue: "renewed",
		},
		{
			name:      "tolerates a key already deleted",
			setup:     func(t *testing.T, store *Store) { t.Helper() },
			wantFound: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			store := NewStore()
			store.ConfigureMaxMemory(1<<30, 16)
			tt.setup(t, store)

			store.dropIfStillExpired(store.shardForKey("key"), "key")

			value, found, err := store.Get("key")
			if err != nil {
				t.Fatalf("Get() error = %v", err)
			}
			if found != tt.wantFound {
				t.Fatalf("Get() found = %v, want %v", found, tt.wantFound)
			}
			if found && string(value) != tt.wantValue {
				t.Fatalf("Get() = %q, want %q", value, tt.wantValue)
			}

			if tt.wantFound {
				return
			}
			// A reclaim must leave no accounting or key-kind residue behind.
			if used := store.UsedMemory(); used != 0 {
				t.Fatalf("UsedMemory() after reclaim = %d, want 0", used)
			}
			if total := store.KeyStats().TotalKeys; total != 0 {
				t.Fatalf("KeyStats().TotalKeys after reclaim = %d, want 0", total)
			}
		})
	}
}

func TestStoreDropIfStillExpiredNeverReclaimsALiveKey(t *testing.T) {
	store := NewStore()
	shard := store.shardForKey("key")

	// Every write below stores a live value, so a correct reclaim can never
	// remove the key no matter how the two goroutines interleave. Run under
	// -race, this also exercises the read-then-write-lock handoff.
	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		for i := 0; i < 2000; i++ {
			_, _ = store.Set("key", []byte("live"), 0)
		}
	}()
	go func() {
		defer workers.Done()
		for i := 0; i < 2000; i++ {
			store.dropIfStillExpired(shard, "key")
		}
	}()
	workers.Wait()

	if _, found, err := store.Get("key"); err != nil || !found {
		t.Fatalf("Get() found = %v, err = %v, want true, nil", found, err)
	}
}

func TestStoreMaxMemoryAccountingAfterExpiredKeyOverwrite(t *testing.T) {
	// Overwriting an expired key must leave exactly the memory the same write
	// would use on an empty Store. A mutator that sizes the expired value after
	// deleting it subtracts those bytes twice, which this comparison catches
	// without depending on the approximation constants.
	tests := []struct {
		name  string
		write func(*testing.T, *Store)
	}{
		{
			name: "Set",
			write: func(t *testing.T, store *Store) {
				t.Helper()
				_, _ = store.Set("stale", []byte("fresh"), 0)
			},
		},
		{
			name: "Increment",
			write: func(t *testing.T, store *Store) {
				t.Helper()
				if _, _, err := store.Increment("stale"); err != nil {
					t.Fatalf("Increment() error = %v", err)
				}
			},
		},
		{
			name: "LeftPush",
			write: func(t *testing.T, store *Store) {
				t.Helper()
				if _, _, err := store.LeftPush("stale", [][]byte{[]byte("job")}); err != nil {
					t.Fatalf("LeftPush() error = %v", err)
				}
			},
		},
		{
			name: "RightPush",
			write: func(t *testing.T, store *Store) {
				t.Helper()
				if _, _, err := store.RightPush("stale", [][]byte{[]byte("job")}); err != nil {
					t.Fatalf("RightPush() error = %v", err)
				}
			},
		},
		{
			name: "SetBit",
			write: func(t *testing.T, store *Store) {
				t.Helper()
				if _, _, err := store.SetBit("stale", 0, 1); err != nil {
					t.Fatalf("SetBit() error = %v", err)
				}
			},
		},
		{
			name: "HSet",
			write: func(t *testing.T, store *Store) {
				t.Helper()
				if _, _, err := store.HSet("stale", []HashFieldValue{{Field: "lang", Value: []byte("go")}}); err != nil {
					t.Fatalf("HSet() error = %v", err)
				}
			},
		},
		{
			name: "SAdd",
			write: func(t *testing.T, store *Store) {
				t.Helper()
				if _, _, err := store.SAdd("stale", [][]byte{[]byte("fast")}); err != nil {
					t.Fatalf("SAdd() error = %v", err)
				}
			},
		},
		{
			name: "ZAdd",
			write: func(t *testing.T, store *Store) {
				t.Helper()
				if _, _, err := store.ZAdd("stale", []ZSetEntry{{Member: []byte("alpha"), Score: 1}}); err != nil {
					t.Fatalf("ZAdd() error = %v", err)
				}
			},
		},
		{
			name: "XAdd",
			write: func(t *testing.T, store *Store) {
				t.Helper()
				if _, _, err := store.XAdd("stale", "1-0", [][]byte{[]byte("field"), []byte("value")}); err != nil {
					t.Fatalf("XAdd() error = %v", err)
				}
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			baseline := NewStore()
			baseline.ConfigureMaxMemory(1<<30, 16)
			tt.write(t, baseline)
			want := baseline.UsedMemory()

			store := NewStore()
			store.ConfigureMaxMemory(1<<30, 16)
			_, _ = store.Set("stale", []byte(strings.Repeat("x", 4096)), time.Now().Add(-time.Millisecond).UnixMilli())
			tt.write(t, store)

			if got := store.UsedMemory(); got != want {
				t.Fatalf("UsedMemory() after overwriting an expired key = %d, want %d", got, want)
			}
		})
	}
}

func TestStoreMaxMemoryAccountingDropsDrainedCollections(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*testing.T, *Store)
		drain func(*testing.T, *Store)
	}{
		{
			name: "LeftPop subtracts the full drained list size",
			setup: func(t *testing.T, store *Store) {
				t.Helper()
				if _, _, err := store.RightPush("jobs", [][]byte{[]byte("one")}); err != nil {
					t.Fatalf("RightPush() error = %v", err)
				}
			},
			drain: func(t *testing.T, store *Store) {
				t.Helper()
				value, ok, err := store.LeftPop("jobs")
				if err != nil {
					t.Fatalf("LeftPop() error = %v", err)
				}
				if !ok || string(value) != "one" {
					t.Fatalf("LeftPop() = (%q, %v), want (%q, true)", string(value), ok, "one")
				}
			},
		},
		{
			name: "RightPop subtracts the full drained list size",
			setup: func(t *testing.T, store *Store) {
				t.Helper()
				if _, _, err := store.RightPush("jobs", [][]byte{[]byte("one")}); err != nil {
					t.Fatalf("RightPush() error = %v", err)
				}
			},
			drain: func(t *testing.T, store *Store) {
				t.Helper()
				value, ok, err := store.RightPop("jobs")
				if err != nil {
					t.Fatalf("RightPop() error = %v", err)
				}
				if !ok || string(value) != "one" {
					t.Fatalf("RightPop() = (%q, %v), want (%q, true)", string(value), ok, "one")
				}
			},
		},
		{
			name: "LeftPopN subtracts the full drained list size",
			setup: func(t *testing.T, store *Store) {
				t.Helper()
				if _, _, err := store.RightPush("jobs", [][]byte{[]byte("one"), []byte("two")}); err != nil {
					t.Fatalf("RightPush() error = %v", err)
				}
			},
			drain: func(t *testing.T, store *Store) {
				t.Helper()
				values, ok, err := store.LeftPopN("jobs", 2)
				if err != nil {
					t.Fatalf("LeftPopN() error = %v", err)
				}
				if !ok || len(values) != 2 {
					t.Fatalf("LeftPopN() = (%v, %v), want (len=2, true)", values, ok)
				}
			},
		},
		{
			name: "RightPopN subtracts the full drained list size",
			setup: func(t *testing.T, store *Store) {
				t.Helper()
				if _, _, err := store.RightPush("jobs", [][]byte{[]byte("one"), []byte("two")}); err != nil {
					t.Fatalf("RightPush() error = %v", err)
				}
			},
			drain: func(t *testing.T, store *Store) {
				t.Helper()
				values, ok, err := store.RightPopN("jobs", 2)
				if err != nil {
					t.Fatalf("RightPopN() error = %v", err)
				}
				if !ok || len(values) != 2 {
					t.Fatalf("RightPopN() = (%v, %v), want (len=2, true)", values, ok)
				}
			},
		},
		{
			name: "HDel subtracts the full drained hash size",
			setup: func(t *testing.T, store *Store) {
				t.Helper()
				if _, _, err := store.HSet("hash", []HashFieldValue{{Field: "field", Value: []byte("value")}}); err != nil {
					t.Fatalf("HSet() error = %v", err)
				}
			},
			drain: func(t *testing.T, store *Store) {
				t.Helper()
				removed, err := store.HDel("hash", []string{"field"})
				if err != nil {
					t.Fatalf("HDel() error = %v", err)
				}
				if removed != 1 {
					t.Fatalf("HDel() = %d, want 1", removed)
				}
			},
		},
		{
			name: "SRem subtracts the full drained set size",
			setup: func(t *testing.T, store *Store) {
				t.Helper()
				if _, _, err := store.SAdd("set", [][]byte{[]byte("member")}); err != nil {
					t.Fatalf("SAdd() error = %v", err)
				}
			},
			drain: func(t *testing.T, store *Store) {
				t.Helper()
				removed, err := store.SRem("set", [][]byte{[]byte("member")})
				if err != nil {
					t.Fatalf("SRem() error = %v", err)
				}
				if removed != 1 {
					t.Fatalf("SRem() = %d, want 1", removed)
				}
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			store := NewStore()
			store.ConfigureMaxMemory(1<<30, 16)

			tt.setup(t, store)
			before := store.UsedMemory()
			if before == 0 {
				t.Fatal("UsedMemory() before drain = 0, want non-zero")
			}

			tt.drain(t, store)

			if got := store.UsedMemory(); got != 0 {
				t.Fatalf("UsedMemory() after drain = %d, want 0", got)
			}
			if got := store.Len(); got != 0 {
				t.Fatalf("Len() after drain = %d, want 0", got)
			}
		})
	}
}

func TestStoreMaxMemoryConcurrentConfiguration(t *testing.T) {
	store := NewStore()

	var wg sync.WaitGroup
	errCh := make(chan error, 1)

	wg.Add(3)
	go func() {
		defer wg.Done()
		for i := 0; i < 256; i++ {
			limit := int64(0)
			if i%2 == 1 {
				limit = 1 << 20
			}
			store.ConfigureMaxMemory(limit, 16)
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < 256; i++ {
			_ = store.MaxMemory()
			_ = store.maxMemoryEnabled()
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < 256; i++ {
			if _, err := store.Set("key", []byte("value"), 0); err != nil {
				errCh <- fmt.Errorf("Set() error = %w", err)
				return
			}
		}
	}()

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestStoreActiveEvictionRemovesExpiredKeys(t *testing.T) {
	store := NewStore()
	_, _ = store.Set("a", []byte("1"), time.Now().Add(-time.Millisecond).UnixMilli())
	_, _ = store.Set("b", []byte("2"), time.Now().Add(-time.Millisecond).UnixMilli())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store.StartEviction(ctx, 5*time.Millisecond, 10)

	deadline := time.Now().Add(250 * time.Millisecond)
	for time.Now().Before(deadline) {
		if store.Len() == 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}

	t.Fatalf("Len() = %d, want 0 after active eviction", store.Len())
}

// TestStoreActiveEvictionKeepsUpWithAKeyspaceFullOfExpiredKeys pins that the
// background loop does not clear expired keys at a fixed 20 per pass: with 50,000
// of them and a 10 ms interval that would take about 25 seconds, all of it spent
// serving expired data. Redis repeats the sample while a quarter of it had
// expired, and so does the loop now.
func TestStoreActiveEvictionKeepsUpWithAKeyspaceFullOfExpiredKeys(t *testing.T) {
	store := NewStore()
	expiredAt := time.Now().Add(-time.Second).UnixMilli()
	for i := 0; i < 50_000; i++ {
		_, _ = store.Set("key-"+strconv.Itoa(i), []byte("v"), expiredAt)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store.StartEviction(ctx, 10*time.Millisecond, 20)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if store.Len() == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("Len() = %d after 3s, want every expired key cleared", store.Len())
}

func TestEvictionPassRepeatsWhileMostOfTheSampleHadExpiredAndStopsAtItsBudget(t *testing.T) {
	store := NewStore()
	expiredAt := time.Now().Add(-time.Second).UnixMilli()
	for i := 0; i < 200_000; i++ {
		_, _ = store.Set("key-"+strconv.Itoa(i), []byte("v"), expiredAt)
	}

	const budget = 20 * time.Millisecond
	started := time.Now()
	samples := store.evictionPass(20, budget)
	elapsed := time.Since(started)

	if samples < 2 {
		t.Fatalf("evictionPass() took %d sample(s), want it to keep sampling while the keys it finds have expired", samples)
	}
	if store.Len() == 200_000 || store.Len() == 0 {
		t.Fatalf("Len() = %d after one pass, want some but not all removed (the budget ends the pass)", store.Len())
	}
	// One sample past the deadline at most, with a wide margin for a loaded machine.
	if elapsed > 10*budget {
		t.Fatalf("evictionPass() took %v, want it to stop near its %v budget", elapsed, budget)
	}
}

func TestEvictionPassTakesOneSampleWhenFewKeysHaveExpired(t *testing.T) {
	store := NewStore()
	for i := 0; i < 5_000; i++ {
		_, _ = store.Set("live-"+strconv.Itoa(i), []byte("v"), 0)
	}
	_, _ = store.Set("dead", []byte("v"), time.Now().Add(-time.Second).UnixMilli())

	// At most one key in any sample of 20 can have expired, far below the quarter
	// that would justify sampling again.
	if samples := store.evictionPass(20, time.Second); samples != 1 {
		t.Fatalf("evictionPass() took %d samples, want 1", samples)
	}
}

// TestStoreActiveEvictionReportsExpiredKeys pins that the background loop names
// the keys it removed rather than only counting them: the caller turns them into
// the DEL that reaches replicas, the AOF, and WATCH.
func TestStoreActiveEvictionReportsExpiredKeys(t *testing.T) {
	store := NewStore()
	_, _ = store.Set("gone", []byte("1"), time.Now().Add(-time.Millisecond).UnixMilli())
	_, _ = store.Set("kept", []byte("2"), 0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reported := make(chan []string, 8)
	store.SetExpirationListener(func(keys []string) {
		reported <- keys
	})
	store.StartEviction(ctx, 5*time.Millisecond, 10)

	select {
	case keys := <-reported:
		if len(keys) != 1 || keys[0] != "gone" {
			t.Fatalf("expired keys = %v, want [gone]", keys)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("active eviction did not report the expired key")
	}
}

// TestStoreActiveEvictionSurvivesAPanickingListener pins that a panic in one
// pass ends that pass, not the loop. The listener reaches replication and
// durability sinks, so a bug there is the likely source; before, the recover sat
// outside the loop and one panic silently stopped all active expiry for the life
// of the process.
func TestStoreActiveEvictionSurvivesAPanickingListener(t *testing.T) {
	store := NewStore()
	past := time.Now().Add(-time.Millisecond).UnixMilli()
	_, _ = store.Set("first", []byte("1"), past)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reported := make(chan []string, 8)
	var panicked atomic.Bool
	store.SetExpirationListener(func(keys []string) {
		reported <- keys
		if panicked.CompareAndSwap(false, true) {
			panic("listener bug")
		}
	})
	done := store.StartEviction(ctx, 5*time.Millisecond, 10)

	select {
	case keys := <-reported:
		if len(keys) != 1 || keys[0] != "first" {
			t.Fatalf("first pass reported %v, want [first]", keys)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("active eviction did not report the first expired key")
	}

	_, _ = store.Set("second", []byte("2"), past)
	select {
	case keys := <-reported:
		if len(keys) != 1 || keys[0] != "second" {
			t.Fatalf("pass after the panic reported %v, want [second]", keys)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("active eviction stopped for good after a listener panic")
	}
	select {
	case <-done:
		t.Fatal("the eviction loop exited while its context was still live")
	default:
	}
}

// TestStoreAccountedWriteReportsSweptExpiredKeys pins the second door expiry
// leaves by. With maxmemory on, every accounted write recalculates usage across
// the whole keyspace and drops the expired keys it passes on the way. That is a
// sweep, not this write's business, and it has to be reported like the
// background loop's. Otherwise, the deletions reach nobody whenever maxmemory is
// the thing driving them.
func TestStoreAccountedWriteReportsSweptExpiredKeys(t *testing.T) {
	store := NewStore()
	store.ConfigureMaxMemory(1<<20, 16)

	reported := make(chan []string, 8)
	store.SetExpirationListener(func(keys []string) {
		reported <- keys
	})

	// Stored while accounting is already on, so the sweep that removes it is the
	// one the next write runs rather than anything this one did.
	if _, err := store.Set("stale", []byte("value"), time.Now().Add(-time.Minute).UnixMilli()); err != nil {
		t.Fatalf("Set(stale) error = %v", err)
	}
	if len(reported) != 0 {
		t.Fatalf("expired keys reported = %v, want none before the key is swept", <-reported)
	}

	if _, err := store.Set("live", []byte("value"), 0); err != nil {
		t.Fatalf("Set(live) error = %v", err)
	}

	select {
	case keys := <-reported:
		if len(keys) != 1 || keys[0] != "stale" {
			t.Fatalf("expired keys = %v, want [stale]", keys)
		}
	default:
		t.Fatal("accounted write did not report the expired key it swept")
	}
}

func TestStoreKeyStatsTracksMutations(t *testing.T) {
	store := NewStore()
	_, _ = store.Set("name", []byte("Stash"), 0)
	if _, _, err := store.RightPush("jobs", [][]byte{[]byte("build")}); err != nil {
		t.Fatalf("RightPush() error = %v", err)
	}
	if _, _, err := store.HSet("profile", []HashFieldValue{{Field: "lang", Value: []byte("go")}}); err != nil {
		t.Fatalf("HSet() error = %v", err)
	}
	if _, _, err := store.SAdd("tags", [][]byte{[]byte("fast")}); err != nil {
		t.Fatalf("SAdd() error = %v", err)
	}
	if _, _, err := store.ZAdd("leaders", []ZSetEntry{{Member: []byte("alpha"), Score: 1}}); err != nil {
		t.Fatalf("ZAdd() error = %v", err)
	}
	if _, _, err := store.XAdd("events", "1-0", [][]byte{[]byte("field"), []byte("value")}); err != nil {
		t.Fatalf("XAdd() error = %v", err)
	}

	assertKeyStats(t, store, 6, map[ValueKind]int{
		ValueKindString: 1,
		ValueKindList:   1,
		ValueKindHash:   1,
		ValueKindSet:    1,
		ValueKindZSet:   1,
		ValueKindStream: 1,
	})

	_, _ = store.Set("name", []byte("database"), 0)
	store.Delete("jobs")
	if _, err := store.HDel("profile", []string{"lang"}); err != nil {
		t.Fatalf("HDel() error = %v", err)
	}
	if _, err := store.SRem("tags", [][]byte{[]byte("fast")}); err != nil {
		t.Fatalf("SRem() error = %v", err)
	}

	assertKeyStats(t, store, 3, map[ValueKind]int{
		ValueKindString: 1,
		ValueKindZSet:   1,
		ValueKindStream: 1,
	})

	_, _ = store.Set("soon-gone", []byte("ttl"), time.Now().Add(-time.Millisecond).UnixMilli())
	if _, ok, err := store.Get("soon-gone"); err != nil {
		t.Fatalf("Get() error = %v", err)
	} else if ok {
		t.Fatal("Get() ok = true for expired key, want false")
	}

	assertKeyStats(t, store, 3, map[ValueKind]int{
		ValueKindString: 1,
		ValueKindZSet:   1,
		ValueKindStream: 1,
	})
}

func TestStoreKeyStatsRecalculatesAfterReplaceWith(t *testing.T) {
	store := NewStore()
	_, _ = store.Set("old", []byte("value"), 0)

	replacement := NewStore()
	if _, _, err := replacement.RightPush("jobs", [][]byte{[]byte("build")}); err != nil {
		t.Fatalf("RightPush() error = %v", err)
	}
	if _, _, err := replacement.SAdd("tags", [][]byte{[]byte("fast")}); err != nil {
		t.Fatalf("SAdd() error = %v", err)
	}

	store.ReplaceWith(replacement)

	assertKeyStats(t, store, 2, map[ValueKind]int{
		ValueKindList: 1,
		ValueKindSet:  1,
	})
}

func assertKeyStats(t *testing.T, store *Store, total int, byKind map[ValueKind]int) {
	t.Helper()

	stats := store.KeyStats()
	if stats.TotalKeys != total {
		t.Fatalf("KeyStats().TotalKeys = %d, want %d", stats.TotalKeys, total)
	}
	for _, kind := range keyStatsKinds {
		if stats.ByKind[kind] != byKind[kind] {
			t.Fatalf("KeyStats().ByKind[%s] = %d, want %d (all stats: %+v)", kind, stats.ByKind[kind], byKind[kind], stats.ByKind)
		}
	}
}

func TestStoreSnapshotStrings(t *testing.T) {
	t.Run("returns defensive copies for supported string keys", func(t *testing.T) {
		store := NewStore()
		_, _ = store.Set("name", []byte("Stash"), 0)

		entries, stats := store.SnapshotStrings()
		if stats.TotalKeys != 1 {
			t.Fatalf("stats.TotalKeys = %d, want 1", stats.TotalKeys)
		}
		if stats.ExportedKeys != 1 {
			t.Fatalf("stats.ExportedKeys = %d, want 1", stats.ExportedKeys)
		}
		if len(entries) != 1 {
			t.Fatalf("len(entries) = %d, want 1", len(entries))
		}

		entries[0].Value[0] = 'r'
		got, ok, err := store.Get("name")
		if err != nil {
			t.Fatalf("Get() error = %v", err)
		}
		if !ok {
			t.Fatal("Get() ok = false, want true")
		}
		if string(got) != "Stash" {
			t.Fatalf("Get() value = %q, want %q", string(got), "Stash")
		}
	})

	t.Run("skips expired and unsupported keys", func(t *testing.T) {
		store := NewStore()
		_, _ = store.Set("alive", []byte("yes"), 0)
		_, _ = store.Set("expired", []byte("gone"), time.Now().Add(-time.Millisecond).UnixMilli())
		if _, _, err := store.RightPush("jobs", [][]byte{[]byte("one")}); err != nil {
			t.Fatalf("RightPush() error = %v", err)
		}

		entries, stats := store.SnapshotStrings()
		if len(entries) != 1 || entries[0].Key != "alive" {
			t.Fatalf("SnapshotStrings() entries = %#v, want only alive string key", entries)
		}
		if stats.SkippedExpiredKeys != 1 {
			t.Fatalf("stats.SkippedExpiredKeys = %d, want 1", stats.SkippedExpiredKeys)
		}
		if stats.SkippedUnsupportedKeys != 1 {
			t.Fatalf("stats.SkippedUnsupportedKeys = %d, want 1", stats.SkippedUnsupportedKeys)
		}
	})
}

func TestStoreSnapshotAll(t *testing.T) {
	t.Run("returns defensive string copies without aliasing sibling entries", func(t *testing.T) {
		store := NewStore()
		_, _ = store.Set("first", []byte("one"), 0)
		_, _ = store.Set("second", []byte("two"), 0)

		entries, stats := store.SnapshotAll()
		if stats.TotalKeys != 2 || stats.ExportedKeys != 2 {
			t.Fatalf("SnapshotAll() stats = %+v, want total/exported 2", stats)
		}

		var firstSnapshot []byte
		var secondSnapshot []byte
		for _, entry := range entries {
			if entry.Kind != ValueKindString {
				continue
			}
			switch entry.Key {
			case "first":
				firstSnapshot = entry.String
			case "second":
				secondSnapshot = entry.String
			}
		}

		if string(firstSnapshot) != "one" || string(secondSnapshot) != "two" {
			t.Fatalf("SnapshotAll() string entries = %q, %q, want %q, %q", string(firstSnapshot), string(secondSnapshot), "one", "two")
		}

		firstSnapshot[0] = 'O'
		if got := string(secondSnapshot); got != "two" {
			t.Fatalf("second snapshot after first mutation = %q, want %q", got, "two")
		}

		got, ok, err := store.Get("first")
		if err != nil {
			t.Fatalf("Get(first) error = %v", err)
		}
		if !ok {
			t.Fatal("Get(first) ok = false, want true")
		}
		if string(got) != "one" {
			t.Fatalf("Get(first) value = %q, want %q", string(got), "one")
		}
	})
}

func TestStoreConcurrentAccess(t *testing.T) {
	store := NewStore()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()

			key := fmt.Sprintf("key-%d", i)
			for j := 0; j < 100; j++ {
				_, _ = store.Set(key, []byte(strconv.Itoa(j)), 0)
				if _, ok, err := store.Get(key); err != nil {
					t.Errorf("Get(%q) error = %v", key, err)
					return
				} else if !ok {
					t.Errorf("Get(%q) ok = false, want true", key)
					return
				}
			}
		}()
	}

	wg.Wait()

	if store.Len() == 0 {
		t.Fatal("Len() = 0, want at least one stored key")
	}
}

func TestStoreDeleteMany(t *testing.T) {
	t.Run("removes present keys across distinct shards", func(t *testing.T) {
		store := NewStore()
		keys := keysInDistinctShards(t, store, 3)

		_, _ = store.Set(keys[0], []byte("one"), 0)
		_, _ = store.Set(keys[1], []byte("two"), 0)
		_, _ = store.Set(keys[2], []byte("stale"), time.Now().Add(-time.Millisecond).UnixMilli())

		removed := store.DeleteMany([]string{keys[0], "missing", keys[1], keys[0], keys[2]})
		if len(removed) != 2 {
			t.Fatalf("len(DeleteMany()) = %d, want 2", len(removed))
		}

		removedSet := make(map[string]struct{}, len(removed))
		for _, key := range removed {
			removedSet[key] = struct{}{}
		}
		for _, key := range keys[:2] {
			if _, ok := removedSet[key]; !ok {
				t.Fatalf("DeleteMany() removed = %v, want key %q", removed, key)
			}
			if _, ok, err := store.Get(key); err != nil {
				t.Fatalf("Get(%q) error = %v", key, err)
			} else if ok {
				t.Fatalf("Get(%q) ok = true, want false after delete", key)
			}
		}
		if _, ok, err := store.Get(keys[2]); err != nil {
			t.Fatalf("Get(%q) error = %v", keys[2], err)
		} else if ok {
			t.Fatalf("Get(%q) ok = true, want false for expired key", keys[2])
		}
	})

	t.Run("is safe under overlapping concurrent shard deletes", func(t *testing.T) {
		store := NewStore()
		keys := keysInDistinctShards(t, store, 4)
		for _, key := range keys {
			_, _ = store.Set(key, []byte(key), 0)
		}

		var wg sync.WaitGroup
		for i := 0; i < 32; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				if i%2 == 0 {
					_ = store.DeleteMany([]string{keys[0], keys[1], keys[2]})
					return
				}
				_ = store.DeleteMany([]string{keys[2], keys[1], keys[3]})
			}(i)
		}
		wg.Wait()

		if got := store.Len(); got != 0 {
			t.Fatalf("Len() = %d, want 0 after concurrent cross-shard deletes", got)
		}
	})
}

func TestStoreListBehavior(t *testing.T) {
	t.Run("LeftPush and RightPush preserve Redis-style ordering", func(t *testing.T) {
		store := NewStore()

		length, _, err := store.LeftPush("letters", [][]byte{[]byte("a"), []byte("b")})
		if err != nil {
			t.Fatalf("LeftPush() error = %v", err)
		}
		if length != 2 {
			t.Fatalf("LeftPush() length = %d, want 2", length)
		}

		length, _, err = store.RightPush("letters", [][]byte{[]byte("c"), []byte("d")})
		if err != nil {
			t.Fatalf("RightPush() error = %v", err)
		}
		if length != 4 {
			t.Fatalf("RightPush() length = %d, want 4", length)
		}

		values, err := store.ListRange("letters", 0, -1)
		if err != nil {
			t.Fatalf("ListRange() error = %v", err)
		}

		want := []string{"b", "a", "c", "d"}
		if len(values) != len(want) {
			t.Fatalf("len(ListRange()) = %d, want %d", len(values), len(want))
		}
		for i, got := range values {
			if string(got) != want[i] {
				t.Fatalf("ListRange()[%d] = %q, want %q", i, string(got), want[i])
			}
		}
	})

	t.Run("ListRange supports negative indexes", func(t *testing.T) {
		store := NewStore()
		if _, _, err := store.RightPush("numbers", [][]byte{[]byte("1"), []byte("2"), []byte("3"), []byte("4")}); err != nil {
			t.Fatalf("RightPush() error = %v", err)
		}

		values, err := store.ListRange("numbers", -2, -1)
		if err != nil {
			t.Fatalf("ListRange() error = %v", err)
		}
		want := []string{"3", "4"}
		for i, got := range values {
			if string(got) != want[i] {
				t.Fatalf("ListRange()[%d] = %q, want %q", i, string(got), want[i])
			}
		}
	})

	t.Run("ListRange returns defensive copies", func(t *testing.T) {
		store := NewStore()
		if _, _, err := store.RightPush("letters", [][]byte{[]byte("a"), []byte("b")}); err != nil {
			t.Fatalf("RightPush() error = %v", err)
		}

		values, err := store.ListRange("letters", 0, -1)
		if err != nil {
			t.Fatalf("ListRange() error = %v", err)
		}

		values[0][0] = 'A'
		values[1] = []byte("changed")

		again, err := store.ListRange("letters", 0, -1)
		if err != nil {
			t.Fatalf("second ListRange() error = %v", err)
		}
		if got := string(again[0]); got != "a" {
			t.Fatalf("second ListRange()[0] = %q, want %q", got, "a")
		}
		if got := string(again[1]); got != "b" {
			t.Fatalf("second ListRange()[1] = %q, want %q", got, "b")
		}
	})

	t.Run("LeftPop removes head and clears empty list key", func(t *testing.T) {
		store := NewStore()
		if _, _, err := store.RightPush("jobs", [][]byte{[]byte("one")}); err != nil {
			t.Fatalf("RightPush() error = %v", err)
		}

		value, ok, err := store.LeftPop("jobs")
		if err != nil {
			t.Fatalf("LeftPop() error = %v", err)
		}
		if !ok || string(value) != "one" {
			t.Fatalf("LeftPop() = (%q, %v), want (%q, true)", string(value), ok, "one")
		}
		if store.Len() != 0 {
			t.Fatalf("Len() = %d, want 0 after popping final list element", store.Len())
		}
	})

	t.Run("List push notifies one waiter and unsubscribe stops notifications", func(t *testing.T) {
		store := NewStore()
		waiter := store.SubscribeListPush("events")

		if _, _, err := store.RightPush("events", [][]byte{[]byte("first")}); err != nil {
			t.Fatalf("RightPush() error = %v", err)
		}

		select {
		case <-waiter:
		case <-time.After(200 * time.Millisecond):
			t.Fatal("waiter did not receive notification")
		}

		waiter = store.SubscribeListPush("events")
		store.UnsubscribeListPush("events", waiter)
		if _, _, err := store.RightPush("events", [][]byte{[]byte("second")}); err != nil {
			t.Fatalf("RightPush() error = %v", err)
		}

		select {
		case <-waiter:
			t.Fatal("received notification after unsubscribe")
		case <-time.After(50 * time.Millisecond):
		}
	})
}

func TestStoreSortedSetBehavior(t *testing.T) {
	t.Run("ZAdd and ZRange order by score then member", func(t *testing.T) {
		store := NewStore()

		added, _, err := store.ZAdd("leaders", []ZSetEntry{
			{Member: []byte("beta"), Score: 2},
			{Member: []byte("alpha"), Score: 1},
			{Member: []byte("aardvark"), Score: 2},
		})
		if err != nil {
			t.Fatalf("ZAdd() error = %v", err)
		}
		if added != 3 {
			t.Fatalf("ZAdd() added = %d, want 3", added)
		}

		values, err := store.ZRange("leaders", 0, -1)
		if err != nil {
			t.Fatalf("ZRange() error = %v", err)
		}

		wantMembers := []string{"alpha", "aardvark", "beta"}
		wantScores := []float64{1, 2, 2}
		if len(values) != len(wantMembers) {
			t.Fatalf("len(ZRange()) = %d, want %d", len(values), len(wantMembers))
		}
		for i, got := range values {
			if got.Member != wantMembers[i] {
				t.Fatalf("ZRange()[%d].Member = %q, want %q", i, got.Member, wantMembers[i])
			}
			if got.Score != wantScores[i] {
				t.Fatalf("ZRange()[%d].Score = %v, want %v", i, got.Score, wantScores[i])
			}
		}
	})

	t.Run("ZAdd updates existing members without increasing added count", func(t *testing.T) {
		store := NewStore()
		if _, _, err := store.ZAdd("leaders", []ZSetEntry{{Member: []byte("alpha"), Score: 1}, {Member: []byte("beta"), Score: 2}}); err != nil {
			t.Fatalf("ZAdd() initial error = %v", err)
		}

		added, _, err := store.ZAdd("leaders", []ZSetEntry{{Member: []byte("beta"), Score: 0.5}})
		if err != nil {
			t.Fatalf("ZAdd() update error = %v", err)
		}
		if added != 0 {
			t.Fatalf("ZAdd() added = %d, want 0", added)
		}

		values, err := store.ZRange("leaders", 0, -1)
		if err != nil {
			t.Fatalf("ZRange() error = %v", err)
		}
		want := []string{"beta", "alpha"}
		for i, got := range values {
			if got.Member != want[i] {
				t.Fatalf("ZRange()[%d].Member = %q, want %q", i, got.Member, want[i])
			}
		}
	})

	t.Run("ZRange supports negative indexes", func(t *testing.T) {
		store := NewStore()
		if _, _, err := store.ZAdd("leaders", []ZSetEntry{
			{Member: []byte("alpha"), Score: 1},
			{Member: []byte("beta"), Score: 2},
			{Member: []byte("charlie"), Score: 3},
		}); err != nil {
			t.Fatalf("ZAdd() error = %v", err)
		}

		values, err := store.ZRange("leaders", -2, -1)
		if err != nil {
			t.Fatalf("ZRange() error = %v", err)
		}
		want := []string{"beta", "charlie"}
		for i, got := range values {
			if got.Member != want[i] {
				t.Fatalf("ZRange()[%d].Member = %q, want %q", i, got.Member, want[i])
			}
		}
	})

	t.Run("ZRange rejects wrong value type", func(t *testing.T) {
		store := NewStore()
		_, _ = store.Set("leaders", []byte("hello"), 0)

		if _, err := store.ZRange("leaders", 0, -1); !errors.Is(err, ErrWrongType) {
			t.Fatalf("ZRange() error = %v, want ErrWrongType", err)
		}
	})

	t.Run("ZRangeByScores honors bounds across encodings", func(t *testing.T) {
		compactEntries := make([]ZSetEntry, 0, compactZSetMaxEntries)
		generalEntries := make([]ZSetEntry, 0, compactZSetMaxEntries*2)
		for i := 0; i < compactZSetMaxEntries; i++ {
			compactEntries = append(compactEntries, ZSetEntry{Member: []byte{'m', byte('0' + i)}, Score: float64(i)})
		}
		for i := 0; i < compactZSetMaxEntries*2; i++ {
			generalEntries = append(generalEntries, ZSetEntry{Member: []byte{'m', 'a' + byte(i)}, Score: float64(i)})
		}

		encodings := []struct {
			name    string
			entries []ZSetEntry
		}{
			{name: "compact", entries: compactEntries},
			{name: "general", entries: generalEntries},
		}
		for _, encoding := range encodings {
			t.Run(encoding.name, func(t *testing.T) {
				store := NewStore()
				if _, _, err := store.ZAdd("leaders", encoding.entries); err != nil {
					t.Fatalf("ZAdd() error = %v", err)
				}

				tests := []struct {
					name       string
					scoreRange ScoreRange
					want       []float64
				}{
					{name: "inclusive bounds", scoreRange: ScoreRange{Min: 2, Max: 4}, want: []float64{2, 3, 4}},
					{name: "exclusive bounds", scoreRange: ScoreRange{Min: 2, Max: 4, MinExclusive: true, MaxExclusive: true}, want: []float64{3}},
					{name: "unbounded", scoreRange: ScoreRange{Min: math.Inf(-1), Max: math.Inf(1)}, want: scoresOfEntries(encoding.entries)},
					{name: "empty interval", scoreRange: ScoreRange{Min: 2.5, Max: 2.75}, want: []float64{}},
					{name: "beyond maximum", scoreRange: ScoreRange{Min: 1000, Max: math.Inf(1)}, want: []float64{}},
				}
				for _, tt := range tests {
					values, err := store.ZRangeByScores("leaders", tt.scoreRange)
					if err != nil {
						t.Fatalf("ZRangeByScores(%s) error = %v", tt.name, err)
					}
					if len(values) != len(tt.want) {
						t.Fatalf("len(ZRangeByScores(%s)) = %d, want %d", tt.name, len(values), len(tt.want))
					}
					for i, got := range values {
						if got.Score != tt.want[i] {
							t.Fatalf("ZRangeByScores(%s)[%d].Score = %v, want %v", tt.name, i, got.Score, tt.want[i])
						}
					}
				}
			})
		}
	})

	t.Run("ZRangeByScores orders equal scores by member", func(t *testing.T) {
		store := NewStore()
		if _, _, err := store.ZAdd("leaders", []ZSetEntry{
			{Member: []byte("beta"), Score: 2},
			{Member: []byte("alpha"), Score: 2},
			{Member: []byte("omega"), Score: 3},
		}); err != nil {
			t.Fatalf("ZAdd() error = %v", err)
		}

		values, err := store.ZRangeByScores("leaders", ScoreRange{Min: 2, Max: 2})
		if err != nil {
			t.Fatalf("ZRangeByScores() error = %v", err)
		}
		want := []string{"alpha", "beta"}
		if len(values) != len(want) {
			t.Fatalf("len(ZRangeByScores()) = %d, want %d", len(values), len(want))
		}
		for i, got := range values {
			if got.Member != want[i] {
				t.Fatalf("ZRangeByScores()[%d].Member = %q, want %q", i, got.Member, want[i])
			}
		}
	})

	t.Run("ZRangeByScores concatenates disjoint ranges in order", func(t *testing.T) {
		store := NewStore()
		entries := make([]ZSetEntry, 0, 10)
		for i := 0; i < 10; i++ {
			entries = append(entries, ZSetEntry{Member: []byte{'m', byte('0' + i)}, Score: float64(i)})
		}
		if _, _, err := store.ZAdd("leaders", entries); err != nil {
			t.Fatalf("ZAdd() error = %v", err)
		}

		values, err := store.ZRangeByScores("leaders",
			ScoreRange{Min: math.Inf(-1), Max: 2, MaxExclusive: true},
			ScoreRange{Min: 5, Max: 6},
			ScoreRange{Min: 9, Max: math.Inf(1)},
		)
		if err != nil {
			t.Fatalf("ZRangeByScores() error = %v", err)
		}
		want := []float64{0, 1, 5, 6, 9}
		if len(values) != len(want) {
			t.Fatalf("len(ZRangeByScores()) = %d, want %d", len(values), len(want))
		}
		for i, got := range values {
			if got.Score != want[i] {
				t.Fatalf("ZRangeByScores()[%d].Score = %v, want %v", i, got.Score, want[i])
			}
		}
	})

	t.Run("ZRangeByScores returns empty for missing key", func(t *testing.T) {
		store := NewStore()

		values, err := store.ZRangeByScores("missing", ScoreRange{Min: math.Inf(-1), Max: math.Inf(1)})
		if err != nil {
			t.Fatalf("ZRangeByScores() error = %v", err)
		}
		if len(values) != 0 {
			t.Fatalf("len(ZRangeByScores()) = %d, want 0", len(values))
		}
	})

	t.Run("ZRangeByScores rejects wrong value type", func(t *testing.T) {
		store := NewStore()
		_, _ = store.Set("leaders", []byte("hello"), 0)

		if _, err := store.ZRangeByScores("leaders", ScoreRange{Min: 0, Max: 1}); !errors.Is(err, ErrWrongType) {
			t.Fatalf("ZRangeByScores() error = %v, want ErrWrongType", err)
		}
	})

	t.Run("ZScores returns scores across encodings", func(t *testing.T) {
		store := NewStore()
		if _, _, err := store.ZAdd("leaders", []ZSetEntry{{Member: []byte("alpha"), Score: 1.5}}); err != nil {
			t.Fatalf("ZAdd() error = %v", err)
		}

		scores, found, err := store.ZScores("leaders", [][]byte{[]byte("alpha")})
		if err != nil {
			t.Fatalf("ZScores() compact error = %v", err)
		}
		if !found[0] || scores[0] != 1.5 {
			t.Fatalf("ZScores() compact = %v, %v, want 1.5, true", scores[0], found[0])
		}

		entries := make([]ZSetEntry, 0, compactZSetMaxEntries+1)
		for i := 0; i <= compactZSetMaxEntries; i++ {
			entries = append(entries, ZSetEntry{Member: []byte{'m', byte('0' + i)}, Score: float64(i)})
		}
		if _, _, err := store.ZAdd("leaders", entries); err != nil {
			t.Fatalf("ZAdd() upgrade error = %v", err)
		}

		scores, found, err = store.ZScores("leaders", [][]byte{[]byte("alpha")})
		if err != nil {
			t.Fatalf("ZScores() general error = %v", err)
		}
		if !found[0] || scores[0] != 1.5 {
			t.Fatalf("ZScores() general = %v, %v, want 1.5, true", scores[0], found[0])
		}
	})

	t.Run("ZScores reports missing members and keys", func(t *testing.T) {
		store := NewStore()
		if _, _, err := store.ZAdd("leaders", []ZSetEntry{{Member: []byte("alpha"), Score: 1}}); err != nil {
			t.Fatalf("ZAdd() error = %v", err)
		}

		scores, found, err := store.ZScores("leaders", [][]byte{[]byte("alpha"), []byte("beta")})
		if err != nil {
			t.Fatalf("ZScores() error = %v", err)
		}
		if !found[0] || scores[0] != 1 {
			t.Fatalf("ZScores()[0] = %v, %v, want 1, true", scores[0], found[0])
		}
		if found[1] {
			t.Fatalf("ZScores()[1] found = true, want false")
		}

		if _, found, err := store.ZScores("missing", [][]byte{[]byte("alpha")}); err != nil || found[0] {
			t.Fatalf("ZScores() missing key = %v, %v, want false, nil", found[0], err)
		}
	})

	t.Run("ZScores rejects wrong value type", func(t *testing.T) {
		store := NewStore()
		_, _ = store.Set("leaders", []byte("hello"), 0)

		if _, _, err := store.ZScores("leaders", [][]byte{[]byte("alpha")}); !errors.Is(err, ErrWrongType) {
			t.Fatalf("ZScores() error = %v, want ErrWrongType", err)
		}
	})

	t.Run("ZAdd recreates expired key", func(t *testing.T) {
		store := NewStore()
		_, _ = store.Set("leaders", []byte("stale"), time.Now().Add(-time.Millisecond).UnixMilli())

		added, _, err := store.ZAdd("leaders", []ZSetEntry{{Member: []byte("fresh"), Score: 1}})
		if err != nil {
			t.Fatalf("ZAdd() error = %v", err)
		}
		if added != 1 {
			t.Fatalf("ZAdd() added = %d, want 1", added)
		}

		values, err := store.ZRange("leaders", 0, -1)
		if err != nil {
			t.Fatalf("ZRange() error = %v", err)
		}
		if len(values) != 1 || string(values[0].Member) != "fresh" {
			t.Fatalf("ZRange() = %#v, want fresh member", values)
		}
	})

	t.Run("Small sorted set uses compact encoding across commands and snapshots", func(t *testing.T) {
		store := NewStore()
		added, _, err := store.ZAdd("leaders", []ZSetEntry{
			{Member: []byte("beta"), Score: 2},
			{Member: []byte("alpha"), Score: 1},
			{Member: []byte("aardvark"), Score: 2},
		})
		if err != nil {
			t.Fatalf("ZAdd() error = %v", err)
		}
		if added != 3 {
			t.Fatalf("ZAdd() added = %d, want 3", added)
		}
		stored := store.valueObjectForTest("leaders")
		if stored == nil || stored.Kind != ValueKindZSet || stored.ZSetEncoding != ValueEncodingCompact || stored.CompactZSet == nil {
			t.Fatalf("stored zset = %#v, want compact zset", stored)
		}

		added, _, err = store.ZAdd("leaders", []ZSetEntry{{Member: []byte("beta"), Score: 0.5}})
		if err != nil || added != 0 {
			t.Fatalf("ZAdd() update = (%d, %v), want (0, nil)", added, err)
		}
		values, err := store.ZRange("leaders", 0, -1)
		if err != nil {
			t.Fatalf("ZRange() error = %v", err)
		}
		want := []string{"beta", "alpha", "aardvark"}
		for i, value := range values {
			if value.Member != want[i] {
				t.Fatalf("ZRange()[%d].Member = %q, want %q", i, value.Member, want[i])
			}
		}

		snapshot, stats := store.SnapshotAll()
		if stats.TotalKeys != 1 || stats.ExportedKeys != 1 {
			t.Fatalf("SnapshotAll() stats = %+v, want total/exported 1", stats)
		}
		if len(snapshot) != 1 || snapshot[0].Kind != ValueKindZSet || len(snapshot[0].ZSet) != 3 {
			t.Fatalf("SnapshotAll() = %#v, want one logical sorted set", snapshot)
		}
		for i, entry := range snapshot[0].ZSet {
			if entry.Member != want[i] {
				t.Fatalf("SnapshotAll().ZSet[%d].Member = %q, want %q", i, entry.Member, want[i])
			}
		}
	})

	t.Run("Sorted set creation chooses compact encoding by distinct members", func(t *testing.T) {
		store := NewStore()
		entries := make([]ZSetEntry, compactZSetMaxEntries+1)
		for i := range entries {
			entries[i] = ZSetEntry{Member: []byte("same"), Score: float64(i)}
		}

		if _, _, err := store.ZAdd("leaders", entries); err != nil {
			t.Fatalf("ZAdd() error = %v", err)
		}
		stored := store.valueObjectForTest("leaders")
		if stored == nil || stored.ZSetEncoding != ValueEncodingCompact {
			t.Fatalf("stored zset = %#v, want compact zset for one distinct member", stored)
		}
	})

	t.Run("Compact sorted set upgrades when member count exceeds threshold", func(t *testing.T) {
		store := NewStore()
		for i := 0; i < compactZSetMaxEntries; i++ {
			member := fmt.Appendf(nil, "m%d", i)
			if _, _, err := store.ZAdd("leaders", []ZSetEntry{{Member: member, Score: float64(i)}}); err != nil {
				t.Fatalf("ZAdd() seed error = %v", err)
			}
		}
		expiresAt := time.Now().Add(time.Hour).UnixMilli()
		if ok := store.expireKeyForTest("leaders", expiresAt); !ok {
			t.Fatal("expireKeyForTest() ok = false, want true")
		}

		added, _, err := store.ZAdd("leaders", []ZSetEntry{{Member: []byte("overflow"), Score: 99}})
		if err != nil || added != 1 {
			t.Fatalf("ZAdd() overflow = (%d, %v), want (1, nil)", added, err)
		}
		stored := store.valueObjectForTest("leaders")
		if stored == nil || stored.Kind != ValueKindZSet || stored.ZSetEncoding != ValueEncodingGeneral || stored.ZSet == nil {
			t.Fatalf("stored zset = %#v, want upgraded general zset", stored)
		}
		if stored.ExpiresAt != expiresAt {
			t.Fatalf("upgraded zset ExpiresAt = %d, want %d", stored.ExpiresAt, expiresAt)
		}
		values, err := store.ZRange("leaders", 0, -1)
		if err != nil {
			t.Fatalf("ZRange() after upgrade error = %v", err)
		}
		if len(values) != compactZSetMaxEntries+1 || values[len(values)-1].Member != "overflow" {
			t.Fatalf("ZRange() after upgrade = %#v, want preserved members plus overflow", values)
		}
	})

	t.Run("Compact sorted set upgrades when member length exceeds threshold", func(t *testing.T) {
		store := NewStore()
		if _, _, err := store.ZAdd("leaders", []ZSetEntry{{Member: []byte("short"), Score: 1}}); err != nil {
			t.Fatalf("ZAdd() seed error = %v", err)
		}
		longMember := make([]byte, compactZSetMaxMemberLen+1)
		for i := range longMember {
			longMember[i] = 'x'
		}

		added, _, err := store.ZAdd("leaders", []ZSetEntry{{Member: longMember, Score: 2}})
		if err != nil || added != 1 {
			t.Fatalf("ZAdd() long member = (%d, %v), want (1, nil)", added, err)
		}
		stored := store.valueObjectForTest("leaders")
		if stored == nil || stored.ZSetEncoding != ValueEncodingGeneral || stored.ZSet == nil {
			t.Fatalf("stored zset = %#v, want upgraded general zset", stored)
		}
		values, err := store.ZRange("leaders", 0, -1)
		if err != nil {
			t.Fatalf("ZRange() after long member error = %v", err)
		}
		if len(values) != 2 || values[1].Member != string(longMember) {
			t.Fatalf("ZRange() after long member = %#v, want long member preserved", values)
		}
	})
}

func keysInDistinctShards(t *testing.T, store *Store, count int) []string {
	t.Helper()
	if count <= 0 {
		t.Fatal("keysInDistinctShards() count must be positive")
	}
	if count > len(store.shards) {
		t.Fatalf("keysInDistinctShards() count = %d, want <= shard count %d", count, len(store.shards))
	}

	keys := make([]string, 0, count)
	seen := make(map[int]struct{}, count)
	for i := 0; len(keys) < count; i++ {
		key := fmt.Sprintf("shard-key-%d", i)
		shardID := store.shardIndex(key)
		if _, ok := seen[shardID]; ok {
			continue
		}

		seen[shardID] = struct{}{}
		keys = append(keys, key)
	}

	return keys
}

func TestStoreStreamBehavior(t *testing.T) {
	t.Run("XAdd and XRead preserve append order and field values", func(t *testing.T) {
		store := NewStore()

		firstID, _, err := store.XAdd("events", "1-0", [][]byte{[]byte("type"), []byte("start")})
		if err != nil {
			t.Fatalf("XAdd() first error = %v", err)
		}
		if firstID != "1-0" {
			t.Fatalf("XAdd() first ID = %q, want %q", firstID, "1-0")
		}

		secondID, _, err := store.XAdd("events", "2-0", [][]byte{[]byte("type"), []byte("finish"), []byte("user"), []byte("42")})
		if err != nil {
			t.Fatalf("XAdd() second error = %v", err)
		}
		if secondID != "2-0" {
			t.Fatalf("XAdd() second ID = %q, want %q", secondID, "2-0")
		}

		entries, err := store.XRead("events", "0-0")
		if err != nil {
			t.Fatalf("XRead() error = %v", err)
		}
		if len(entries) != 2 {
			t.Fatalf("len(XRead()) = %d, want 2", len(entries))
		}
		if entries[0].ID != "1-0" || entries[1].ID != "2-0" {
			t.Fatalf("XRead() IDs = [%q, %q], want [%q, %q]", entries[0].ID, entries[1].ID, "1-0", "2-0")
		}
		if got := string(entries[1].Values[0]); got != "type" {
			t.Fatalf("entries[1].Values[0] = %q, want %q", got, "type")
		}
		if got := string(entries[1].Values[1]); got != "finish" {
			t.Fatalf("entries[1].Values[1] = %q, want %q", got, "finish")
		}
		if got := string(entries[1].Values[2]); got != "user" {
			t.Fatalf("entries[1].Values[2] = %q, want %q", got, "user")
		}
		if got := string(entries[1].Values[3]); got != "42" {
			t.Fatalf("entries[1].Values[3] = %q, want %q", got, "42")
		}
	})

	t.Run("XAdd auto generated IDs increment sequence in same millisecond", func(t *testing.T) {
		stream := newStream()

		firstID, err := stream.add("*", [][]byte{[]byte("field"), []byte("one")}, 100)
		if err != nil {
			t.Fatalf("stream.add() first error = %v", err)
		}
		secondID, err := stream.add("*", [][]byte{[]byte("field"), []byte("two")}, 100)
		if err != nil {
			t.Fatalf("stream.add() second error = %v", err)
		}
		thirdID, err := stream.add("*", [][]byte{[]byte("field"), []byte("three")}, 101)
		if err != nil {
			t.Fatalf("stream.add() third error = %v", err)
		}

		if firstID != "100-0" {
			t.Fatalf("first auto ID = %q, want %q", firstID, "100-0")
		}
		if secondID != "100-1" {
			t.Fatalf("second auto ID = %q, want %q", secondID, "100-1")
		}
		if thirdID != "101-0" {
			t.Fatalf("third auto ID = %q, want %q", thirdID, "101-0")
		}
	})

	t.Run("XRead supports dollar special ID and bare millisecond IDs", func(t *testing.T) {
		store := NewStore()
		if _, _, err := store.XAdd("events", "1-0", [][]byte{[]byte("field"), []byte("one")}); err != nil {
			t.Fatalf("XAdd() first error = %v", err)
		}
		if _, _, err := store.XAdd("events", "2-0", [][]byte{[]byte("field"), []byte("two")}); err != nil {
			t.Fatalf("XAdd() second error = %v", err)
		}

		entries, err := store.XRead("events", "$")
		if err != nil {
			t.Fatalf("XRead($) error = %v", err)
		}
		if len(entries) != 0 {
			t.Fatalf("len(XRead($)) = %d, want 0", len(entries))
		}

		entries, err = store.XRead("events", "1")
		if err != nil {
			t.Fatalf("XRead(1) error = %v", err)
		}
		if len(entries) != 1 || entries[0].ID != "2-0" {
			t.Fatalf("XRead(1) = %#v, want one entry with ID 2-0", entries)
		}
	})

	t.Run("XAdd rejects malformed or non monotonic IDs", func(t *testing.T) {
		store := NewStore()

		if _, _, err := store.XAdd("events", "not-an-id", [][]byte{[]byte("field"), []byte("value")}); !errors.Is(err, ErrInvalidStreamID) {
			t.Fatalf("XAdd() invalid ID error = %v, want ErrInvalidStreamID", err)
		}
		if _, _, err := store.XAdd("events", "1-0", [][]byte{[]byte("field"), []byte("value")}); err != nil {
			t.Fatalf("XAdd() initial error = %v", err)
		}
		if _, _, err := store.XAdd("events", "1-0", [][]byte{[]byte("field"), []byte("value")}); !errors.Is(err, ErrStreamIDTooSmall) {
			t.Fatalf("XAdd() non monotonic error = %v, want ErrStreamIDTooSmall", err)
		}
	})

	t.Run("XRead rejects wrong value type", func(t *testing.T) {
		store := NewStore()
		_, _ = store.Set("events", []byte("plain"), 0)

		if _, err := store.XRead("events", "0-0"); !errors.Is(err, ErrWrongType) {
			t.Fatalf("XRead() error = %v, want ErrWrongType", err)
		}
	})

	t.Run("XAdd recreates expired stream key", func(t *testing.T) {
		store := NewStore()
		store.setValueObjectForTest("events", newStreamValue(newStream(), time.Now().Add(-time.Millisecond).UnixMilli()))

		id, _, err := store.XAdd("events", "5-0", [][]byte{[]byte("field"), []byte("value")})
		if err != nil {
			t.Fatalf("XAdd() error = %v", err)
		}
		if id != "5-0" {
			t.Fatalf("XAdd() ID = %q, want %q", id, "5-0")
		}

		entries, err := store.XRead("events", "0-0")
		if err != nil {
			t.Fatalf("XRead() error = %v", err)
		}
		if len(entries) != 1 || entries[0].ID != "5-0" {
			t.Fatalf("XRead() = %#v, want single fresh entry", entries)
		}
	})

	t.Run("concurrent XAdd keeps stream IDs strictly increasing", func(t *testing.T) {
		store := NewStore()

		const writers = 32
		var wg sync.WaitGroup
		ids := make(chan string, writers)
		errs := make(chan error, writers)

		for i := 0; i < writers; i++ {
			i := i
			wg.Add(1)
			go func() {
				defer wg.Done()
				id, _, err := store.XAdd("events", "*", [][]byte{[]byte("writer"), []byte(strconv.Itoa(i))})
				if err != nil {
					errs <- err
					return
				}
				ids <- id
			}()
		}

		wg.Wait()
		close(ids)
		close(errs)

		for err := range errs {
			if err != nil {
				t.Fatalf("concurrent XAdd() error = %v", err)
			}
		}

		seen := map[string]struct{}{}
		for id := range ids {
			if _, ok := seen[id]; ok {
				t.Fatalf("duplicate ID generated: %q", id)
			}
			seen[id] = struct{}{}
		}

		entries, err := store.XRead("events", "0-0")
		if err != nil {
			t.Fatalf("XRead() error = %v", err)
		}
		if len(entries) != writers {
			t.Fatalf("len(XRead()) = %d, want %d", len(entries), writers)
		}

		last := streamID{}
		for i, entry := range entries {
			current, err := parseStreamAddID(entry.ID)
			if err != nil {
				t.Fatalf("parseStreamAddID(%q) error = %v", entry.ID, err)
			}
			if i > 0 && compareStreamIDs(current, last) <= 0 {
				t.Fatalf("entry ID %q is not greater than previous ID %q", entry.ID, last.String())
			}
			last = current
		}
	})

	t.Run("concurrent XAdd and XRead do not race", func(t *testing.T) {
		store := NewStore()

		var wg sync.WaitGroup
		start := make(chan struct{})
		errCh := make(chan error, 2)

		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < 128; i++ {
				if _, _, err := store.XAdd("events", "*", [][]byte{[]byte("field"), []byte(strconv.Itoa(i))}); err != nil {
					errCh <- err
					return
				}
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < 128; i++ {
				if _, err := store.XRead("events", "0-0"); err != nil {
					errCh <- err
					return
				}
			}
		}()

		close(start)
		wg.Wait()
		close(errCh)

		for err := range errCh {
			if err != nil {
				t.Fatalf("concurrent stream access error = %v", err)
			}
		}
	})

	t.Run("XAdd copies caller provided values", func(t *testing.T) {
		store := NewStore()
		payload := [][]byte{[]byte("field"), []byte("value")}

		if _, _, err := store.XAdd("events", "1-0", payload); err != nil {
			t.Fatalf("XAdd() error = %v", err)
		}
		payload[0][0] = 'F'
		payload[1][0] = 'V'

		entries, err := store.XRead("events", "0-0")
		if err != nil {
			t.Fatalf("XRead() error = %v", err)
		}
		if got := string(entries[0].Values[0]); got != "field" {
			t.Fatalf("entries[0].Values[0] = %q, want %q", got, "field")
		}
		if got := string(entries[0].Values[1]); got != "value" {
			t.Fatalf("entries[0].Values[1] = %q, want %q", got, "value")
		}
	})

	t.Run("XRead returns defensive copies", func(t *testing.T) {
		store := NewStore()
		if _, _, err := store.XAdd("events", "1-0", [][]byte{[]byte("field"), []byte("value")}); err != nil {
			t.Fatalf("XAdd() error = %v", err)
		}

		entries, err := store.XRead("events", "0-0")
		if err != nil {
			t.Fatalf("XRead() error = %v", err)
		}

		entries[0].Values[0][0] = 'F'
		entries[0].Values[1] = []byte("changed")

		again, err := store.XRead("events", "0-0")
		if err != nil {
			t.Fatalf("second XRead() error = %v", err)
		}
		if got := string(again[0].Values[0]); got != "field" {
			t.Fatalf("second XRead()[0].Values[0] = %q, want %q", got, "field")
		}
		if got := string(again[0].Values[1]); got != "value" {
			t.Fatalf("second XRead()[0].Values[1] = %q, want %q", got, "value")
		}
	})

	t.Run("XRead returns empty for missing stream", func(t *testing.T) {
		store := NewStore()

		entries, err := store.XRead("missing", "0-0")
		if err != nil {
			t.Fatalf("XRead() error = %v", err)
		}
		if len(entries) != 0 {
			t.Fatalf("len(XRead()) = %d, want 0", len(entries))
		}
	})

	t.Run("XRead rejects malformed IDs", func(t *testing.T) {
		store := NewStore()
		if _, _, err := store.XAdd("events", "1-0", [][]byte{[]byte("field"), []byte("value")}); err != nil {
			t.Fatalf("XAdd() error = %v", err)
		}

		_, err := store.XRead("events", "bad-id")
		if !errors.Is(err, ErrInvalidStreamID) {
			t.Fatalf("XRead() error = %v, want ErrInvalidStreamID", err)
		}
	})

	t.Run("XAdd returns full millisecond sequence IDs", func(t *testing.T) {
		store := NewStore()
		id, _, err := store.XAdd("events", "*", [][]byte{[]byte("field"), []byte("value")})
		if err != nil {
			t.Fatalf("XAdd() error = %v", err)
		}
		if parts := strings.Split(id, "-"); len(parts) != 2 {
			t.Fatalf("generated ID = %q, want <milliseconds>-<sequence>", id)
		}
	})
}

// TestSnapshotAllCopiesValuesIntactAcrossArenaChunks snapshots values of every
// size around the arena's chunk boundaries and checks each comes back byte for
// byte, and that entries do not share room to grow into one another.
func TestSnapshotAllCopiesValuesIntactAcrossArenaChunks(t *testing.T) {
	sizes := []int{0, 1, 7, snapshotArenaMinChunk - 1, snapshotArenaMinChunk, snapshotArenaMinChunk + 1, 3000, 9000, 70_000, snapshotArenaMaxChunk - 1, snapshotArenaMaxChunk, snapshotArenaMaxChunk + 1, 2*snapshotArenaMaxChunk + 5}

	store := NewStore()
	want := make(map[string][]byte)
	for round := 0; round < 3; round++ {
		for _, size := range sizes {
			key := fmt.Sprintf("k-%d-%d", round, size)
			value := make([]byte, size)
			for i := range value {
				value[i] = byte(i*31 + size + round)
			}
			want[key] = value
			if _, err := store.Set(key, value, 0); err != nil {
				t.Fatalf("Set(%s) error = %v", key, err)
			}
		}
	}
	if _, _, err := store.RightPush("list", [][]byte{[]byte("a"), make([]byte, 5000), []byte("c")}); err != nil {
		t.Fatalf("RightPush() error = %v", err)
	}

	entries, _ := store.SnapshotAll()
	seen := 0
	for _, entry := range entries {
		if entry.Kind == ValueKindList {
			if len(entry.List) != 3 || string(entry.List[0]) != "a" || len(entry.List[1]) != 5000 || string(entry.List[2]) != "c" {
				t.Fatalf("list entry = %d values, want the three pushed", len(entry.List))
			}
			continue
		}
		expected, ok := want[entry.Key]
		if !ok {
			t.Fatalf("snapshot has unexpected key %q", entry.Key)
		}
		seen++
		if !bytes.Equal(entry.String, expected) {
			t.Fatalf("value of %q differs from what was stored (%d bytes, want %d)", entry.Key, len(entry.String), len(expected))
		}
		if cap(entry.String) != len(entry.String) {
			t.Fatalf("value of %q has spare capacity %d, so appending to it would overwrite a neighbour", entry.Key, cap(entry.String)-len(entry.String))
		}
	}
	if seen != len(want) {
		t.Fatalf("snapshot has %d string entries, want %d", seen, len(want))
	}
}
