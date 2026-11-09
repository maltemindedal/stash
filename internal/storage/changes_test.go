package storage

import (
	"fmt"
	"slices"
	"testing"
	"time"
)

type keyChange struct {
	key   string
	cause Cause
}

// heardChanges is a Changes that keeps what it hears and notes every call made
// while a shard lock was held.
type heardChanges struct {
	store  *Store
	heard  []keyChange
	locked []keyChange
}

func (h *heardChanges) KeyChanged(key string, cause Cause) {
	change := keyChange{key: key, cause: cause}
	h.heard = append(h.heard, change)
	for i := range h.store.shards {
		if !h.store.shards[i].mu.TryLock() {
			h.locked = append(h.locked, change)
			return
		}
		h.store.shards[i].mu.Unlock()
	}
}

// evictedKeys is a Changes that keeps the keys a write evicted to make room.
type evictedKeys []string

func (e *evictedKeys) KeyChanged(key string, cause Cause) {
	if cause == MemoryEvicted {
		*e = append(*e, key)
	}
}

// expiredKeys is a Changes that keeps the keys a sweep removed.
type expiredKeys []string

func (e *expiredKeys) KeyChanged(key string, cause Cause) {
	if cause == Expired {
		*e = append(*e, key)
	}
}

func bytesOf(values ...string) [][]byte {
	out := make([][]byte, len(values))
	for i, value := range values {
		out[i] = []byte(value)
	}
	return out
}

// storeWrites is every exported write of one key, each with the state it needs
// to change that key and, where it can succeed without changing it, the state
// in which it does not.
var storeWrites = []struct {
	name      string
	setup     func(t *testing.T, s *Store)
	write     func(s *Store, changes Changes) error
	unchanged bool
}{
	{name: "Set", write: func(s *Store, c Changes) error { err := s.Set(c, "k", []byte("v"), 0); return err }},
	{name: "SetBit", write: func(s *Store, c Changes) error { _, err := s.SetBit(c, "k", 7, 1); return err }},
	{name: "PFAdd", write: func(s *Store, c Changes) error { _, err := s.PFAdd(c, "k", bytesOf("a")); return err }},
	{name: "Increment", write: func(s *Store, c Changes) error { _, err := s.Increment(c, "k"); return err }},
	{name: "LeftPush", write: func(s *Store, c Changes) error { _, err := s.LeftPush(c, "k", bytesOf("a")); return err }},
	{name: "RightPush", write: func(s *Store, c Changes) error { _, err := s.RightPush(c, "k", bytesOf("a")); return err }},
	{name: "ZAdd", write: func(s *Store, c Changes) error {
		_, err := s.ZAdd(c, "k", []ZSetEntry{{Member: []byte("a"), Score: 1}})
		return err
	}},
	{name: "XAdd", write: func(s *Store, c Changes) error { _, err := s.XAdd(c, "k", "1-1", bytesOf("f", "v")); return err }},
	{name: "HSet", write: func(s *Store, c Changes) error {
		_, err := s.HSet(c, "k", []HashFieldValue{{Field: "f", Value: []byte("v")}})
		return err
	}},
	{name: "SAdd", write: func(s *Store, c Changes) error { _, err := s.SAdd(c, "k", bytesOf("a")); return err }},

	{name: "LeftPop", setup: pushed("k", "a", "b"), write: func(s *Store, c Changes) error { _, _, err := s.LeftPop(c, "k"); return err }},
	{name: "LeftPop of a missing key", unchanged: true, write: func(s *Store, c Changes) error { _, _, err := s.LeftPop(c, "k"); return err }},
	{name: "RightPop", setup: pushed("k", "a", "b"), write: func(s *Store, c Changes) error { _, _, err := s.RightPop(c, "k"); return err }},
	{name: "RightPop of a missing key", unchanged: true, write: func(s *Store, c Changes) error { _, _, err := s.RightPop(c, "k"); return err }},
	{name: "LeftPopN", setup: pushed("k", "a", "b"), write: func(s *Store, c Changes) error { _, _, err := s.LeftPopN(c, "k", 2); return err }},
	{name: "LeftPopN of none", setup: pushed("k", "a", "b"), unchanged: true, write: func(s *Store, c Changes) error { _, _, err := s.LeftPopN(c, "k", 0); return err }},
	{name: "RightPopN", setup: pushed("k", "a", "b"), write: func(s *Store, c Changes) error { _, _, err := s.RightPopN(c, "k", 1); return err }},
	{name: "RightPopN of a missing key", unchanged: true, write: func(s *Store, c Changes) error { _, _, err := s.RightPopN(c, "k", 1); return err }},
	{name: "HDel", setup: hashed("k", "f"), write: func(s *Store, c Changes) error { _, err := s.HDel(c, "k", []string{"f"}); return err }},
	{name: "HDel of a missing field", setup: hashed("k", "f"), unchanged: true, write: func(s *Store, c Changes) error { _, err := s.HDel(c, "k", []string{"other"}); return err }},
	{name: "HDel of a missing key", unchanged: true, write: func(s *Store, c Changes) error { _, err := s.HDel(c, "k", []string{"f"}); return err }},
	{name: "SRem", setup: membered("k", "a"), write: func(s *Store, c Changes) error { _, err := s.SRem(c, "k", bytesOf("a")); return err }},
	{name: "SRem of a missing member", setup: membered("k", "a"), unchanged: true, write: func(s *Store, c Changes) error { _, err := s.SRem(c, "k", bytesOf("other")); return err }},
	{name: "SRem of a missing key", unchanged: true, write: func(s *Store, c Changes) error { _, err := s.SRem(c, "k", bytesOf("a")); return err }},
	{name: "Delete", setup: pushed("k", "a"), write: func(s *Store, c Changes) error { s.Delete(c, "k"); return nil }},
	{name: "Delete of a missing key", unchanged: true, write: func(s *Store, c Changes) error { s.Delete(c, "k"); return nil }},
	{name: "DeleteMany", setup: pushed("k", "a"), write: func(s *Store, c Changes) error { s.DeleteMany(c, []string{"absent", "k"}); return nil }},
	{name: "DeleteMany of missing keys", unchanged: true, write: func(s *Store, c Changes) error { s.DeleteMany(c, []string{"absent", "k"}); return nil }},
}

func pushed(key string, values ...string) func(*testing.T, *Store) {
	return func(t *testing.T, s *Store) {
		t.Helper()
		if _, err := s.RightPush(nil, key, bytesOf(values...)); err != nil {
			t.Fatal(err)
		}
	}
}

func hashed(key, field string) func(*testing.T, *Store) {
	return func(t *testing.T, s *Store) {
		t.Helper()
		if _, err := s.HSet(nil, key, []HashFieldValue{{Field: field, Value: []byte("v")}}); err != nil {
			t.Fatal(err)
		}
	}
}

func membered(key, member string) func(*testing.T, *Store) {
	return func(t *testing.T, s *Store) {
		t.Helper()
		if _, err := s.SAdd(nil, key, bytesOf(member)); err != nil {
			t.Fatal(err)
		}
	}
}

func TestAWriteReportsItsKeyAsWrittenOnlyWhenItChangedIt(t *testing.T) {
	for _, maxMemory := range []int64{0, 1 << 20} {
		for _, tt := range storeWrites {
			t.Run(fmt.Sprintf("%s, maxmemory %d", tt.name, maxMemory), func(t *testing.T) {
				store := NewStore()
				store.ConfigureMaxMemory(nil, maxMemory, 0)
				if tt.setup != nil {
					tt.setup(t, store)
				}

				changes := &heardChanges{store: store}
				if err := tt.write(store, changes); err != nil {
					t.Fatal(err)
				}

				var want []keyChange
				if !tt.unchanged {
					want = []keyChange{{key: "k", cause: Written}}
				}
				if !slices.Equal(changes.heard, want) {
					t.Fatalf("changes heard = %v, want %v", changes.heard, want)
				}
				if len(changes.locked) > 0 {
					t.Fatalf("changes heard while a shard lock was held: %v", changes.locked)
				}
			})
		}
	}
}

func TestAFailedWriteReportsNoChange(t *testing.T) {
	tests := []struct {
		name  string
		write func(s *Store, changes Changes) error
	}{
		{name: "RightPush to a string", write: func(s *Store, c Changes) error { _, err := s.RightPush(c, "k", bytesOf("a")); return err }},
		{name: "LeftPop of a string", write: func(s *Store, c Changes) error { _, _, err := s.LeftPop(c, "k"); return err }},
		{name: "Increment of a string that is no number", write: func(s *Store, c Changes) error { _, err := s.Increment(c, "k"); return err }},
	}
	for _, maxMemory := range []int64{0, 1 << 20} {
		for _, tt := range tests {
			t.Run(fmt.Sprintf("%s, maxmemory %d", tt.name, maxMemory), func(t *testing.T) {
				store := NewStore()
				store.ConfigureMaxMemory(nil, maxMemory, 0)
				if err := store.Set(nil, "k", []byte("text"), 0); err != nil {
					t.Fatal(err)
				}

				changes := &heardChanges{store: store}
				if err := tt.write(store, changes); err == nil {
					t.Fatal("the write succeeded, want an error")
				}
				if len(changes.heard) > 0 {
					t.Fatalf("changes heard = %v, want none", changes.heard)
				}
			})
		}
	}
}

// TestChangesAreHeardWithNoShardLockHeld runs every write under maxmemory with
// an expired key in the store, so that the write's sweep removes it, and has
// KeyChanged try every shard lock.
func TestChangesAreHeardWithNoShardLockHeld(t *testing.T) {
	for _, tt := range storeWrites {
		if tt.unchanged {
			continue
		}
		t.Run(tt.name, func(t *testing.T) {
			store, clock := newStoreWithManualClock()
			store.ConfigureMaxMemory(nil, 1<<20, 0)
			if tt.setup != nil {
				tt.setup(t, store)
			}
			if err := store.Set(nil, "stale", []byte("v"), store.deadlineAfter(time.Second)); err != nil {
				t.Fatal(err)
			}
			clock.advance(time.Minute)

			changes := &heardChanges{store: store}
			if err := tt.write(store, changes); err != nil {
				t.Fatal(err)
			}

			if len(changes.heard) == 0 {
				t.Fatal("no change heard")
			}
			if len(changes.locked) > 0 {
				t.Fatalf("changes heard while a shard lock was held: %v", changes.locked)
			}
		})
	}
}

func TestAnAccountedWriteReportsTheKeysItsSweepRemovedAsExpired(t *testing.T) {
	store, clock := newStoreWithManualClock()
	store.ConfigureMaxMemory(nil, 1<<20, 0)
	if err := store.Set(nil, "stale", []byte("v"), store.deadlineAfter(time.Second)); err != nil {
		t.Fatal(err)
	}
	clock.advance(time.Minute)

	changes := &heardChanges{store: store}
	if err := store.Set(changes, "k", []byte("v"), 0); err != nil {
		t.Fatal(err)
	}

	want := []keyChange{{key: "stale", cause: Expired}, {key: "k", cause: Written}}
	if !slices.Equal(changes.heard, want) {
		t.Fatalf("changes heard = %v, want %v", changes.heard, want)
	}
}
