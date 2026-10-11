package storage

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
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

// growingWrite is a growing Store operation that goes through mutateKey.
type growingWrite struct {
	name string
	// setup stores a value of the kind the write works on at key.
	setup func(s *Store, key string) error
	// write runs the operation on key, which it grows, and returns its reply as
	// text with the keys it evicted.
	write func(s *Store, key string) (reply string, evicted []string, err error)
}

func growingWrites() []growingWrite {
	bulks := func(items ...string) [][]byte {
		out := make([][]byte, len(items))
		for i, item := range items {
			out[i] = []byte(item)
		}
		return out
	}
	long := strings.Repeat("l", compactHashMaxStringSize+1) // too long for a compact hash or sorted set
	reply := func(n int64, evicted []string, err error) (string, []string, error) {
		return strconv.FormatInt(n, 10), evicted, err
	}

	rpush := func(items ...string) func(*Store, string) error {
		return func(s *Store, key string) error { _, _, err := s.RightPush(key, bulks(items...)); return err }
	}
	hset := func(pairs ...string) func(*Store, string) error {
		return func(s *Store, key string) error {
			fields := make([]HashFieldValue, 0, len(pairs)/2)
			for i := 0; i < len(pairs); i += 2 {
				fields = append(fields, HashFieldValue{Field: pairs[i], Value: []byte(pairs[i+1])})
			}
			_, _, err := s.HSet(key, fields)
			return err
		}
	}
	sadd := func(members ...string) func(*Store, string) error {
		return func(s *Store, key string) error { _, _, err := s.SAdd(key, bulks(members...)); return err }
	}
	zadd := func(members ...string) func(*Store, string) error {
		return func(s *Store, key string) error {
			entries := make([]ZSetEntry, len(members))
			for i, member := range members {
				entries[i] = ZSetEntry{Member: []byte(member), Score: float64(i)}
			}
			_, _, err := s.ZAdd(key, entries)
			return err
		}
	}
	// then runs setups in turn, so that a row can shape a value with a
	// shrinking write after creating it.
	then := func(setups ...func(*Store, string) error) func(*Store, string) error {
		return func(s *Store, key string) error {
			for _, setup := range setups {
				if err := setup(s, key); err != nil {
					return err
				}
			}
			return nil
		}
	}
	lpop := func(s *Store, key string) error { _, _, err := s.LeftPop(key); return err }
	rpop := func(s *Store, key string) error { _, _, err := s.RightPop(key); return err }
	tenFields := make([]string, 0, 20)  // more fields than a compact hash holds
	tenMembers := make([]string, 0, 10) // more members than a compact sorted set holds
	for i := 0; i < 10; i++ {
		tenFields = append(tenFields, "f"+strconv.Itoa(i), "v")
		tenMembers = append(tenMembers, "m"+strconv.Itoa(i))
	}

	lpushX := func(s *Store, key string) (string, []string, error) { return reply(s.LeftPush(key, bulks("x"))) }
	rpushX := func(s *Store, key string) (string, []string, error) { return reply(s.RightPush(key, bulks("x"))) }
	hsetX := func(field, value string) func(*Store, string) (string, []string, error) {
		return func(s *Store, key string) (string, []string, error) {
			return reply(s.HSet(key, []HashFieldValue{{Field: field, Value: []byte(value)}}))
		}
	}
	saddX := func(member string) func(*Store, string) (string, []string, error) {
		return func(s *Store, key string) (string, []string, error) { return reply(s.SAdd(key, bulks(member))) }
	}
	zaddX := func(member string) func(*Store, string) (string, []string, error) {
		return func(s *Store, key string) (string, []string, error) {
			return reply(s.ZAdd(key, []ZSetEntry{{Member: []byte(member), Score: 0.5}}))
		}
	}

	return []growingWrite{
		{name: "LPUSH", setup: rpush("a", "b", "c"), write: lpushX},
		{name: "LPUSH into the dead slots ahead of a list", setup: then(rpush("a", "b", "c", "d"), lpop, lpop), write: lpushX},
		{name: "RPUSH", setup: rpush("a", "b", "c"), write: rpushX},
		{name: "RPUSH into the spare room behind a list", setup: then(rpush("a", "b", "c", "d"), rpop), write: rpushX},
		{name: "HSET of a new field on a compact hash", setup: hset("f1", "a", "f2", "b"), write: hsetX("f3", "x")},
		{name: "HSET of a longer value on a compact hash", setup: hset("f1", "a", "f2", "b"), write: hsetX("f1", "xxxx")},
		{name: "HSET that turns a compact hash into a table", setup: hset("f1", "a"), write: hsetX("f2", long)},
		{name: "HSET of a new field on a hash table", setup: hset(tenFields...), write: hsetX("f99", "x")},
		{name: "SADD to an integer set", setup: sadd("1", "2", "3"), write: saddX("4")},
		{name: "SADD that turns an integer set into a hash set", setup: sadd("1", "2", "3"), write: saddX("x")},
		{name: "SADD to a hash set", setup: sadd("a", "b", "c"), write: saddX("x")},
		{name: "ZADD to a compact sorted set", setup: zadd("a", "b"), write: zaddX("x")},
		{name: "ZADD that turns a compact sorted set into a skip list", setup: zadd("a"), write: zaddX(long)},
		{name: "ZADD to a skip-list sorted set", setup: zadd(tenMembers...), write: zaddX("x")},
		{
			name: "XADD",
			setup: func(s *Store, key string) error {
				_, _, err := s.XAdd(key, "1-1", bulks("f", "v"))
				return err
			},
			write: func(s *Store, key string) (string, []string, error) { return s.XAdd(key, "2-1", bulks("f", "x")) },
		},
	}
}

// describeValueForTest renders the value stored at key, with its encoding and
// TTL deadline, so that two values can be compared as text.
func describeValueForTest(store *Store, key string) string {
	shard := store.shardForKey(key)
	shard.mu.RLock()
	defer shard.mu.RUnlock()

	v := shard.data[key]
	if v == nil {
		return "absent"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%s, expires at %d: ", v.Kind, v.ExpiresAt)
	switch v.Kind {
	case ValueKindString:
		fmt.Fprintf(&b, "%q", v.String)
	case ValueKindList:
		fmt.Fprintf(&b, "%q", v.liveList())
	case ValueKindHash:
		entries, _ := v.hashEntries()
		pairs := make([]string, 0, len(entries))
		for _, entry := range entries {
			pairs = append(pairs, entry.Field+"="+string(entry.Value))
		}
		sort.Strings(pairs)
		fmt.Fprintf(&b, "encoding %d %q", v.HashEncoding, pairs)
	case ValueKindSet:
		members, _ := v.setMembers()
		names := make([]string, 0, len(members))
		for _, member := range members {
			names = append(names, string(member))
		}
		sort.Strings(names)
		fmt.Fprintf(&b, "encoding %d %q", v.SetEncoding, names)
	case ValueKindZSet:
		n, _ := v.zsetLen()
		entries, _ := v.zsetRangeByRank(0, n-1)
		fmt.Fprintf(&b, "encoding %d %v", v.ZSetEncoding, entries)
	case ValueKindStream:
		fmt.Fprintf(&b, "last ID %s", v.Stream.lastID)
		for _, record := range v.Stream.entries {
			fmt.Fprintf(&b, ", %s %q", record.idText, record.values)
		}
	}
	return b.String()
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

	// A growing write to a key whose TTL has passed must do what it does to a
	// key that was never written: reply the same and leave the same value, with
	// no TTL.
	for _, w := range growingWrites() {
		w := w
		for _, maxMemory := range []bool{false, true} {
			maxMemory := maxMemory
			t.Run(fmt.Sprintf("%s, maxmemory %v", w.name, maxMemory), func(t *testing.T) {
				fresh, _ := newStoreWithManualClock()
				store, clock := newStoreWithManualClock()
				if maxMemory {
					fresh.ConfigureMaxMemory(1<<30, 16)
					store.ConfigureMaxMemory(1<<30, 16)
				}
				if err := w.setup(store, key); err != nil {
					t.Fatalf("setup error = %v", err)
				}
				expireForTest(store, clock, key)

				want, _, err := w.write(fresh, key)
				if err != nil {
					t.Fatalf("%s on a new key error = %v", w.name, err)
				}
				got, _, err := w.write(store, key)
				if err != nil {
					t.Fatalf("%s error = %v", w.name, err)
				}
				if got != want {
					t.Fatalf("%s on an expired key replied %s, want %s as on a new key", w.name, got, want)
				}
				if got, want := describeValueForTest(store, key), describeValueForTest(fresh, key); got != want {
					t.Fatalf("%s on an expired key left %s, want %s as on a new key", w.name, got, want)
				}
				requireExactUsedMemory(t, store)
			})
		}
	}
}

func TestAWriteToAKeyOfAnotherKindChangesNothing(t *testing.T) {
	const key = "k"
	type write struct {
		name string
		run  func(s *Store, key string) error
	}
	var writes []write
	for _, w := range shrinkingWrites() {
		if w.anyKind {
			continue
		}
		w := w
		writes = append(writes, write{name: w.name, run: func(s *Store, key string) error { _, err := w.write(s, key); return err }})
	}
	for _, w := range growingWrites() {
		w := w
		writes = append(writes, write{name: w.name, run: func(s *Store, key string) error { _, _, err := w.write(s, key); return err }})
	}

	for _, w := range writes {
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

				if err := w.run(store, key); !errors.Is(err, ErrWrongType) {
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
			write: func(s *Store) error { _, _, err := mutateKey(s, key, ValueKindZSet, shrinks, unchanged); return err },
		},
		{
			name:  "a mutation that empties a string",
			setup: func(s *Store) error { _, err := s.Set(key, []byte("text"), 0); return err },
			write: func(s *Store) error {
				_, _, err := mutateKey(s, key, ValueKindString, shrinks, func(cur *ValueObject, _ int64) (*ValueObject, struct{}, error) {
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

	// stamped is a write that leaves its key holding a value; setup is nil for
	// a write to a new key.
	type stamped struct {
		name  string
		setup func(s *Store, key string) error
		run   func(s *Store, key string) error
	}
	var stampedWrites []stamped
	for _, w := range writes {
		if w.deletes {
			continue
		}
		w := w
		stampedWrites = append(stampedWrites, stamped{name: w.name, setup: w.setup, run: func(s *Store, key string) error { _, err := w.write(s, key); return err }})
	}
	for _, w := range growingWrites() {
		w := w
		run := func(s *Store, key string) error { _, _, err := w.write(s, key); return err }
		stampedWrites = append(stampedWrites,
			stamped{name: w.name, setup: w.setup, run: run},
			stamped{name: w.name + " to a new key", run: run},
		)
	}

	for _, w := range stampedWrites {
		w := w
		for _, maxMemory := range []bool{false, true} {
			maxMemory := maxMemory
			t.Run(fmt.Sprintf("%s, maxmemory %v", w.name, maxMemory), func(t *testing.T) {
				store, clock := newStoreWithManualClock()
				if maxMemory {
					store.ConfigureMaxMemory(1<<30, 16)
				}
				if w.setup != nil {
					if err := w.setup(store, key); err != nil {
						t.Fatalf("setup error = %v", err)
					}
				}

				writtenAt := clock.advance(time.Millisecond)
				if err := w.run(store, key); err != nil {
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

// TestARefusedGrowingWriteLeavesTheStoredValueAsItWas pins why an accounted
// growing write edits a copy of its key's value. At the limit, with nothing else
// to evict, every growing write of every collection kind and encoding is
// refused, and the value stored under its key keeps its contents, its encoding
// and its access stamp.
func TestARefusedGrowingWriteLeavesTheStoredValueAsItWas(t *testing.T) {
	const key = "k"
	for _, w := range growingWrites() {
		w := w
		t.Run(w.name, func(t *testing.T) {
			store, clock := newStoreWithManualClock()
			if err := w.setup(store, key); err != nil {
				t.Fatalf("setup error = %v", err)
			}
			store.ConfigureMaxMemory(1<<30, 16)
			store.ConfigureMaxMemory(store.UsedMemory(), 16) // at the limit
			before := describeValueForTest(store, key)
			stampedAt := store.lastAccessedAtForTest(key)
			clock.advance(time.Millisecond)

			_, evicted, err := w.write(store, key)
			if !errors.Is(err, ErrMemoryLimitExceeded) {
				t.Fatalf("%s at the limit error = %v, want ErrMemoryLimitExceeded", w.name, err)
			}
			if len(evicted) != 0 {
				t.Fatalf("%s was refused yet reported evicting %v", w.name, evicted)
			}
			if after := describeValueForTest(store, key); after != before {
				t.Fatalf("a refused %s left %s, want %s as it was", w.name, after, before)
			}
			if got := store.lastAccessedAtForTest(key); got != stampedAt {
				t.Fatalf("a refused %s stamped its key at %d, want it left at %d", w.name, got, stampedAt)
			}
			requireExactUsedMemory(t, store)
		})
	}
}

// TestGrowingWritesEvictTheStalestKeyToMakeRoom pins the other half of the
// growth class: under maxmemory, at the limit, every growing write makes room
// for itself by evicting the stalest other key, and leaves the counter within
// the limit.
func TestGrowingWritesEvictTheStalestKeyToMakeRoom(t *testing.T) {
	const key, victim = "k", "victim"
	for _, w := range growingWrites() {
		w := w
		t.Run(w.name, func(t *testing.T) {
			store, clock := newStoreWithManualClock()
			if _, err := store.Set(victim, bytes.Repeat([]byte("v"), 512), 0); err != nil {
				t.Fatalf("Set(%s) error = %v", victim, err)
			}
			clock.advance(time.Millisecond) // so that the victim is the stalest key
			if err := w.setup(store, key); err != nil {
				t.Fatalf("setup error = %v", err)
			}
			store.ConfigureMaxMemory(1<<30, 16)
			limit := store.UsedMemory()
			store.ConfigureMaxMemory(limit, 16)

			_, evicted, err := w.write(store, key)
			if err != nil {
				t.Fatalf("%s at the limit error = %v", w.name, err)
			}
			if len(evicted) != 1 || evicted[0] != victim {
				t.Fatalf("%s at the limit evicted %v, want [%s]", w.name, evicted, victim)
			}
			if store.valueObjectForTest(victim) != nil {
				t.Fatalf("%s reported evicting %s but left it stored", w.name, victim)
			}
			if used := store.UsedMemory(); used > limit {
				t.Fatalf("UsedMemory() = %d after %s, over the limit %d", used, w.name, limit)
			}
			requireExactUsedMemory(t, store)
		})
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
