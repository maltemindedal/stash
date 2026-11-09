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
		{name: "listOf", access: func(v *ValueObject) error { _, err := listOf(v); return err }},
		{name: "hashOf", access: func(v *ValueObject) error { _, err := hashOf(v); return err }},
		{name: "setOf", access: func(v *ValueObject) error { _, err := setOf(v); return err }},
		{name: "zsetOf", access: func(v *ValueObject) error { _, err := zsetOf(v); return err }},
		{name: "streamOf", access: func(v *ValueObject) error { _, err := streamOf(v); return err }},
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
		{name: "LPUSH (a new list)", write: func(s *Store) error { _, _, err := s.LeftPush(key, bulk("a")); return err }},
		{name: "RPUSH (a new list)", write: func(s *Store) error { _, _, err := s.RightPush(key, bulk("a")); return err }},
		{name: "HSET of a short value (a compact hash)", write: hset("f", "v")},
		{name: "HSET of a long value (a table hash)", write: hset("f", long)},
		{name: "SADD of an integer (an integer set)", write: func(s *Store) error { _, _, err := s.SAdd(key, bulk("1")); return err }},
		{name: "SADD of a string (a table set)", write: func(s *Store) error { _, _, err := s.SAdd(key, bulk("a")); return err }},
		{name: "ZADD of a short member (a compact sorted set)", write: zadd("m")},
		{name: "ZADD of a long member (a general sorted set)", write: zadd(long)},
		{name: "XADD (a new stream)", write: func(s *Store) error { _, _, err := s.XAdd(key, "*", bulk("f", "v")); return err }},
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
