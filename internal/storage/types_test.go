package storage

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestValueObjectAccessorsRejectWrongKinds(t *testing.T) {
	tests := []struct {
		name   string
		access func(*ValueObject) error
	}{
		{name: "ListValue", access: func(v *ValueObject) error { _, err := v.ListValue(); return err }},
		{name: "ZSetValue", access: func(v *ValueObject) error { _, err := v.ZSetValue(); return err }},
		{name: "StreamValue", access: func(v *ValueObject) error { _, err := v.StreamValue(); return err }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value := newStringValue([]byte("hello"), 0)

			if err := tt.access(value); !errors.Is(err, ErrWrongType) {
				t.Fatalf("%s() on a string value error = %v, want ErrWrongType", tt.name, err)
			}
		})
	}
}

func TestValueObjectAccessorsRejectInvalidTaggedUnionState(t *testing.T) {
	tests := []struct {
		name   string
		value  *ValueObject
		access func(*ValueObject) error
	}{
		{
			name:   "zset kind without a zset payload",
			value:  &ValueObject{Kind: ValueKindZSet},
			access: func(v *ValueObject) error { _, err := v.ZSetValue(); return err },
		},
		{
			name:   "stream kind without a stream payload",
			value:  &ValueObject{Kind: ValueKindStream},
			access: func(v *ValueObject) error { _, err := v.StreamValue(); return err },
		},
		{
			name:   "nil value object",
			value:  nil,
			access: func(v *ValueObject) error { _, err := v.StringValue(); return err },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.access(tt.value); !errors.Is(err, errInvalidValueObjectState) {
				t.Fatalf("accessor error = %v, want errInvalidValueObjectState", err)
			}
		})
	}
}

// TestValueObjectAccessorsMaterializeCompactValues checks that materializing a
// compact encoding hands back a copy: mutating what the accessor returned must
// not write through to the stored compact representation.
func TestValueObjectAccessorsMaterializeCompactValues(t *testing.T) {
	t.Run("hash", func(t *testing.T) {
		hashValue := newCompactHashValue(newCompactHash([]HashFieldValue{{Field: "f", Value: []byte("v")}}), 0)
		hash, err := hashValue.HashValue()
		if err != nil {
			t.Fatalf("HashValue() error = %v", err)
		}
		if string(hash["f"]) != "v" {
			t.Fatalf("HashValue()[f] = %q, want v", string(hash["f"]))
		}

		hash["f"][0] = 'V'
		got, ok, err := hashValue.hashGet("f")
		if err != nil || !ok || string(got) != "v" {
			t.Fatalf("compact hash after materialized mutation = (%q, %v, %v), want (v, true, nil)", string(got), ok, err)
		}
	})

	t.Run("sorted set", func(t *testing.T) {
		zsetValue := newCompactZSetValue(newCompactZSet([]ZSetEntry{{Member: []byte("m"), Score: 1}}), 0)
		zset, err := zsetValue.ZSetValue()
		if err != nil {
			t.Fatalf("ZSetValue() error = %v", err)
		}

		entries := zset.rangeByRank(0, 0)
		if len(entries) != 1 || entries[0].Member != "m" || entries[0].Score != 1 {
			t.Fatalf("ZSetValue().rangeByRank() = %#v, want m/1", entries)
		}
	})

	t.Run("intset", func(t *testing.T) {
		setValue := newIntSetValue(&IntSet{values: []int64{1}}, 0)
		set, err := setValue.SetValue()
		if err != nil {
			t.Fatalf("SetValue() error = %v", err)
		}
		if _, ok := set["1"]; !ok {
			t.Fatalf("SetValue()[1] missing")
		}

		set["2"] = struct{}{}
		contains, err := setValue.setContains([]byte("2"))
		if err != nil || contains {
			t.Fatalf("compact set after materialized mutation = (%v, %v), want (false, nil)", contains, err)
		}
	})
}

func TestStoreRejectsInvalidTaggedUnionState(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value *ValueObject
		read  func(*Store, string) error
	}{
		{
			name:  "ZRange over a zset kind without a payload",
			key:   "leaders",
			value: &ValueObject{Kind: ValueKindZSet},
			read:  func(s *Store, key string) error { _, err := s.ZRange(key, 0, -1); return err },
		},
		{
			name:  "XRead over a stream kind without a payload",
			key:   "events",
			value: &ValueObject{Kind: ValueKindStream},
			read:  func(s *Store, key string) error { _, err := s.XRead(key, "0-0"); return err },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := NewStore()
			store.setValueObjectForTest(tt.key, tt.value)

			if err := tt.read(store, tt.key); !errors.Is(err, errInvalidValueObjectState) {
				t.Fatalf("read error = %v, want errInvalidValueObjectState", err)
			}
		})
	}
}

// TestAWriteStampsTheValueItCreatesWithTheStoreClock pins where a new value's
// access stamp comes from. The constructors leave a value unstamped, and the
// write that stores it stamps it with its clock reading: a value stored with no
// stamp would be the first candidate for memory-pressure eviction. Each row
// reaches one constructor, and the store's manual clock reads years before the
// wall clock, so a stamp from anywhere else would not match.
func TestAWriteStampsTheValueItCreatesWithTheStoreClock(t *testing.T) {
	const key = "k"
	long := strings.Repeat("l", compactHashMaxStringSize+1) // too long for a compact hash or sorted set
	bulk := func(items ...string) [][]byte {
		out := make([][]byte, len(items))
		for i, item := range items {
			out[i] = []byte(item)
		}
		return out
	}
	hset := func(field, value string) func(*Store) error {
		return func(s *Store) error {
			_, _, err := s.HSet(key, []HashFieldValue{{Field: field, Value: []byte(value)}})
			return err
		}
	}
	zadd := func(member string) func(*Store) error {
		return func(s *Store) error {
			_, _, err := s.ZAdd(key, []ZSetEntry{{Member: []byte(member), Score: 1}})
			return err
		}
	}

	tests := []struct {
		name  string
		write func(*Store) error
	}{
		{name: "SET (newStringValue)", write: func(s *Store) error { _, err := s.Set(key, []byte("v"), 0); return err }},
		{name: "INCR (newOwnedStringValue)", write: func(s *Store) error { _, _, err := s.Increment(key); return err }},
		{name: "SETBIT (newOwnedStringValue)", write: func(s *Store) error { _, _, err := s.SetBit(key, 9, 1); return err }},
		{name: "PFADD (newOwnedStringValue)", write: func(s *Store) error { _, _, err := s.PFAdd(key, bulk("a")); return err }},
		{name: "LPUSH (newListValue)", write: func(s *Store) error { _, _, err := s.LeftPush(key, bulk("a")); return err }},
		{name: "RPUSH (newListValue)", write: func(s *Store) error { _, _, err := s.RightPush(key, bulk("a")); return err }},
		{name: "HSET of a short value (newCompactHashValue)", write: hset("f", "v")},
		{name: "HSET of a long value (newHashValue)", write: hset("f", long)},
		{name: "SADD of an integer (newIntSetValue)", write: func(s *Store) error { _, _, err := s.SAdd(key, bulk("1")); return err }},
		{name: "SADD of a string (newSetValue)", write: func(s *Store) error { _, _, err := s.SAdd(key, bulk("a")); return err }},
		{name: "ZADD of a short member (newCompactZSetValue)", write: zadd("m")},
		{name: "ZADD of a long member (newZSetValue)", write: zadd(long)},
		{name: "XADD (newStreamValue)", write: func(s *Store) error { _, _, err := s.XAdd(key, "*", bulk("f", "v")); return err }},
	}

	for _, tt := range tests {
		tt := tt
		for _, maxMemory := range []bool{false, true} {
			maxMemory := maxMemory
			t.Run(fmt.Sprintf("%s, maxmemory %v", tt.name, maxMemory), func(t *testing.T) {
				store, clock := newStoreWithManualClock()
				if maxMemory {
					store.ConfigureMaxMemory(1<<30, 16)
				}
				writtenAt := clock.advance(time.Millisecond)

				if err := tt.write(store); err != nil {
					t.Fatalf("write error = %v", err)
				}
				if got := store.lastAccessedAtForTest(key); got != writtenAt {
					t.Fatalf("the new value is stamped %d, want the write's clock reading %d", got, writtenAt)
				}
			})
		}
	}
}
