package storage

import (
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"
)

// shrinkingWrite is a Store operation that goes through mutateKey.
type shrinkingWrite struct {
	name string
	// setup stores a value of the kind the write works on, with three values,
	// fields or members, at key.
	setup func(s *Store, key string) error
	// write runs the operation on key, which it leaves holding at least one
	// value unless it deletes the key, and reports whether it found a value to
	// work on.
	write func(s *Store, key string) (found bool, err error)
	// anyKind is set for a write that accepts a value of every kind.
	anyKind bool
	// deletes is set for a write that removes the key whatever it holds.
	deletes bool
}

func shrinkingWrites() []shrinkingWrite {
	a, b, c := []byte("a"), []byte("b"), []byte("c")
	list := func(s *Store, key string) error { _, _, err := s.RightPush(key, [][]byte{a, b, c}); return err }
	hash := func(s *Store, key string) error {
		_, _, err := s.HSet(key, []HashFieldValue{{Field: "f1", Value: a}, {Field: "f2", Value: b}, {Field: "f3", Value: c}})
		return err
	}
	set := func(s *Store, key string) error { _, _, err := s.SAdd(key, [][]byte{a, b, c}); return err }

	return []shrinkingWrite{
		{name: "LPOP", setup: list, write: func(s *Store, key string) (bool, error) {
			_, ok, err := s.LeftPop(key)
			return ok, err
		}},
		{name: "RPOP", setup: list, write: func(s *Store, key string) (bool, error) {
			_, ok, err := s.RightPop(key)
			return ok, err
		}},
		{name: "LPOP with a count", setup: list, write: func(s *Store, key string) (bool, error) {
			_, ok, err := s.LeftPopN(key, 2)
			return ok, err
		}},
		{name: "RPOP with a count", setup: list, write: func(s *Store, key string) (bool, error) {
			_, ok, err := s.RightPopN(key, 2)
			return ok, err
		}},
		{name: "HDEL", setup: hash, write: func(s *Store, key string) (bool, error) {
			removed, err := s.HDel(key, []string{"f1"})
			return removed > 0, err
		}},
		{name: "SREM", setup: set, write: func(s *Store, key string) (bool, error) {
			removed, err := s.SRem(key, [][]byte{a})
			return removed > 0, err
		}},
		{name: "DEL", setup: list, anyKind: true, deletes: true, write: func(s *Store, key string) (bool, error) {
			return s.Delete(key), nil
		}},
	}
}

// requireExactUsedMemory fails the test when the store accounts for memory and
// its counter differs from a fresh measurement of the keyspace.
func requireExactUsedMemory(t *testing.T, store *Store) {
	t.Helper()
	if !store.maxMemoryEnabled() {
		return
	}
	if used, want := store.UsedMemory(), recountUsedMemory(store); used != want {
		t.Fatalf("UsedMemory() = %d, recount = %d", used, want)
	}
}

func TestAWriteToAnExpiredKeySeesNoValue(t *testing.T) {
	const key = "k"
	for _, w := range shrinkingWrites() {
		w := w
		for _, maxMemory := range []bool{false, true} {
			maxMemory := maxMemory
			t.Run(fmt.Sprintf("%s, maxmemory %v", w.name, maxMemory), func(t *testing.T) {
				store, clock := newStoreWithManualClock()
				if maxMemory {
					store.ConfigureMaxMemory(1<<30, 16)
				}
				if err := w.setup(store, key); err != nil {
					t.Fatalf("setup error = %v", err)
				}
				expireForTest(store, clock, key)

				found, err := w.write(store, key)
				if err != nil {
					t.Fatalf("%s error = %v", w.name, err)
				}
				if found {
					t.Fatalf("%s found a value under a key whose TTL had passed", w.name)
				}
				if store.valueObjectForTest(key) != nil {
					t.Fatalf("%s left the expired key stored", w.name)
				}
				requireExactUsedMemory(t, store)
			})
		}
	}
}

func TestAWriteToAKeyOfAnotherKindChangesNothing(t *testing.T) {
	const key = "k"
	for _, w := range shrinkingWrites() {
		if w.anyKind {
			continue
		}
		w := w
		for _, maxMemory := range []bool{false, true} {
			maxMemory := maxMemory
			t.Run(fmt.Sprintf("%s, maxmemory %v", w.name, maxMemory), func(t *testing.T) {
				store, clock := newStoreWithManualClock()
				if maxMemory {
					store.ConfigureMaxMemory(1<<30, 16)
				}
				if _, err := store.Set(key, []byte("text"), 0); err != nil {
					t.Fatalf("Set() error = %v", err)
				}
				before := store.valueObjectForTest(key)
				stampedAt := store.lastAccessedAtForTest(key)
				clock.advance(time.Millisecond)

				if _, err := w.write(store, key); !errors.Is(err, ErrWrongType) {
					t.Fatalf("%s on a string error = %v, want ErrWrongType", w.name, err)
				}
				after := store.valueObjectForTest(key)
				if after != before || string(after.String) != "text" {
					t.Fatalf("%s on a string left %+v under the key, want the string %q unchanged", w.name, after, "text")
				}
				if got := store.lastAccessedAtForTest(key); got != stampedAt {
					t.Fatalf("%s on a string stamped its access at %d, want it left at %d", w.name, got, stampedAt)
				}
				requireExactUsedMemory(t, store)
			})
		}
	}
}

func TestAnEmptiedCollectionIsDeletedAndAnEmptyStringIsKept(t *testing.T) {
	const key = "k"
	a, b := []byte("a"), []byte("b")
	tenFields := make([]HashFieldValue, 10) // more than a compact hash holds
	tenNames := make([]string, len(tenFields))
	for i := range tenFields {
		tenNames[i] = "f" + strconv.Itoa(i)
		tenFields[i] = HashFieldValue{Field: tenNames[i], Value: a}
	}
	unchanged := func(cur *ValueObject, _ int64) (*ValueObject, struct{}, error) { return cur, struct{}{}, nil }

	tests := []struct {
		name  string
		setup func(*Store) error
		write func(*Store) error
		kept  bool
	}{
		{
			name:  "LPOP of the last value",
			setup: func(s *Store) error { _, _, err := s.RightPush(key, [][]byte{a}); return err },
			write: func(s *Store) error { _, _, err := s.LeftPop(key); return err },
		},
		{
			name:  "RPOP of the last value",
			setup: func(s *Store) error { _, _, err := s.RightPush(key, [][]byte{a}); return err },
			write: func(s *Store) error { _, _, err := s.RightPop(key); return err },
		},
		{
			name:  "LPOP with a count past the end",
			setup: func(s *Store) error { _, _, err := s.RightPush(key, [][]byte{a, b}); return err },
			write: func(s *Store) error { _, _, err := s.LeftPopN(key, 5); return err },
		},
		{
			name:  "RPOP with a count of the list's length",
			setup: func(s *Store) error { _, _, err := s.RightPush(key, [][]byte{a, b}); return err },
			write: func(s *Store) error { _, _, err := s.RightPopN(key, 2); return err },
		},
		{
			name: "HDEL of every field of a compact hash",
			setup: func(s *Store) error {
				_, _, err := s.HSet(key, []HashFieldValue{{Field: "f1", Value: a}, {Field: "f2", Value: b}})
				return err
			},
			write: func(s *Store) error { _, err := s.HDel(key, []string{"f1", "f2"}); return err },
		},
		{
			name:  "HDEL of every field of a hash table",
			setup: func(s *Store) error { _, _, err := s.HSet(key, tenFields); return err },
			write: func(s *Store) error { _, err := s.HDel(key, tenNames); return err },
		},
		{
			name:  "SREM of every member of an integer set",
			setup: func(s *Store) error { _, _, err := s.SAdd(key, [][]byte{[]byte("1"), []byte("2")}); return err },
			write: func(s *Store) error { _, err := s.SRem(key, [][]byte{[]byte("1"), []byte("2")}); return err },
		},
		{
			name:  "SREM of every member of a hash set",
			setup: func(s *Store) error { _, _, err := s.SAdd(key, [][]byte{a, b}); return err },
			write: func(s *Store) error { _, err := s.SRem(key, [][]byte{a, b}); return err },
		},
		{
			// No command removes a sorted-set member yet, so this stores an empty
			// sorted set and runs a mutation that leaves it as it is.
			name: "a mutation of an empty sorted set",
			setup: func(s *Store) error {
				s.setValueObjectForTest(key, newZSetValueForEntries(nil, 0))
				return nil
			},
			write: func(s *Store) error { _, err := mutateKey(s, key, ValueKindZSet, unchanged); return err },
		},
		{
			name:  "a mutation that empties a string",
			setup: func(s *Store) error { _, err := s.Set(key, []byte("text"), 0); return err },
			write: func(s *Store) error {
				_, err := mutateKey(s, key, ValueKindString, func(cur *ValueObject, _ int64) (*ValueObject, struct{}, error) {
					cur.String = cur.String[:0]
					return cur, struct{}{}, nil
				})
				return err
			},
			kept: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		for _, maxMemory := range []bool{false, true} {
			maxMemory := maxMemory
			t.Run(fmt.Sprintf("%s, maxmemory %v", tt.name, maxMemory), func(t *testing.T) {
				store, _ := newStoreWithManualClock()
				if err := tt.setup(store); err != nil {
					t.Fatalf("setup error = %v", err)
				}
				if maxMemory {
					store.ConfigureMaxMemory(1<<30, 16)
				}

				if err := tt.write(store); err != nil {
					t.Fatalf("write error = %v", err)
				}
				value := store.valueObjectForTest(key)
				if tt.kept {
					if value == nil || len(value.String) != 0 {
						t.Fatalf("the key holds %+v, want an empty string kept", value)
					}
				} else if value != nil {
					t.Fatalf("the key still holds %+v, want the emptied collection deleted", value)
				}
				requireExactUsedMemory(t, store)
			})
		}
	}
}

func TestWritesStampTheirKeyWithTheStoreClock(t *testing.T) {
	const key = "k"
	writes := shrinkingWrites()
	writes = append(writes,
		shrinkingWrite{
			name: "HDEL of a missing field",
			setup: func(s *Store, key string) error {
				_, _, err := s.HSet(key, []HashFieldValue{{Field: "f1", Value: []byte("v")}})
				return err
			},
			write: func(s *Store, key string) (bool, error) { _, err := s.HDel(key, []string{"missing"}); return true, err },
		},
		shrinkingWrite{
			name:  "SREM of a missing member",
			setup: func(s *Store, key string) error { _, _, err := s.SAdd(key, [][]byte{[]byte("a")}); return err },
			write: func(s *Store, key string) (bool, error) {
				_, err := s.SRem(key, [][]byte{[]byte("missing")})
				return true, err
			},
		},
	)

	for _, w := range writes {
		if w.deletes {
			continue
		}
		w := w
		for _, maxMemory := range []bool{false, true} {
			maxMemory := maxMemory
			t.Run(fmt.Sprintf("%s, maxmemory %v", w.name, maxMemory), func(t *testing.T) {
				store, clock := newStoreWithManualClock()
				if maxMemory {
					store.ConfigureMaxMemory(1<<30, 16)
				}
				if err := w.setup(store, key); err != nil {
					t.Fatalf("setup error = %v", err)
				}

				writtenAt := clock.advance(time.Millisecond)
				if _, err := w.write(store, key); err != nil {
					t.Fatalf("%s error = %v", w.name, err)
				}
				if got := store.lastAccessedAtForTest(key); got != writtenAt {
					t.Fatalf("%s stamped its key at %d, want its clock reading %d", w.name, got, writtenAt)
				}
				requireExactUsedMemory(t, store)
			})
		}
	}
}

// TestShrinkingWritesLockOnlyTheirOwnShardAndNeverEvict pins what makes a write
// shrinking: under maxmemory, at the limit, with an expired key elsewhere that a
// growing write would sweep, it still takes only its own key's shard lock, and
// it neither evicts nor sweeps. The test holds every other shard's lock while
// the write runs, so a write that took them all could not finish.
func TestShrinkingWritesLockOnlyTheirOwnShardAndNeverEvict(t *testing.T) {
	for _, w := range shrinkingWrites() {
		w := w
		t.Run(w.name, func(t *testing.T) {
			store, clock := newStoreWithManualClock()
			var heard []string
			store.SetExpirationListener(func(keys []string) { heard = append(heard, keys...) })

			key := keyOnShard(store, len(store.shards)-1)
			stale := keyOnShard(store, 0)
			if err := w.setup(store, key); err != nil {
				t.Fatalf("setup error = %v", err)
			}
			if _, err := store.Set(stale, []byte("v"), 0); err != nil {
				t.Fatalf("Set(%s) error = %v", stale, err)
			}
			store.ConfigureMaxMemory(1<<30, 16)
			store.ConfigureMaxMemory(store.UsedMemory(), 16) // at the limit
			expireForTest(store, clock, stale)               // after the recount, which would sweep it

			own := store.shardIndex(key)
			for i := range store.shards {
				if i != own {
					store.shards[i].mu.Lock()
				}
			}
			done := make(chan error, 1)
			go func() {
				_, err := w.write(store, key)
				done <- err
			}()
			var err error
			select {
			case err = <-done:
			case <-time.After(10 * time.Second):
				t.Fatalf("%s did not finish while the test held every other shard's lock", w.name)
			}
			for i := range store.shards {
				if i != own {
					store.shards[i].mu.Unlock()
				}
			}

			if err != nil {
				t.Fatalf("%s error = %v", w.name, err)
			}
			if len(heard) != 0 {
				t.Fatalf("%s swept %v; a shrinking write sweeps nothing", w.name, heard)
			}
			if store.valueObjectForTest(stale) == nil {
				t.Fatalf("%s removed %s from another shard", w.name, stale)
			}
			requireExactUsedMemory(t, store)
		})
	}
}

// keyOnShard returns a key that the store places on the shard at index.
func keyOnShard(store *Store, index int) string {
	for i := 0; ; i++ {
		if candidate := "k" + strconv.Itoa(i); store.shardIndex(candidate) == index {
			return candidate
		}
	}
}
