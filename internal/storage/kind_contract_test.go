package storage

import (
	"strings"
	"testing"
)

// kindFixtureKey is the one key every kind fixture writes.
const kindFixtureKey = "k"

// kindFixture is one value kind in one encoding, built through the store's own
// writes. A new kind or encoding adds a row to kindFixtures.
type kindFixture struct {
	name  string
	build func(s *Store) error
	// size is the accounted size of the key: what it adds to UsedMemory.
	size int64
}

func kindFixtures() []kindFixture {
	long := strings.Repeat("l", compactHashMaxStringSize+1) // too long for a compact hash or sorted set
	bulk := func(items ...string) [][]byte {
		out := make([][]byte, len(items))
		for i, item := range items {
			out[i] = []byte(item)
		}
		return out
	}
	set := func(value string, ttl bool) func(*Store) error {
		return func(s *Store) error {
			expiresAt := int64(0)
			if ttl {
				expiresAt = s.now() + 3_600_000
			}
			_, err := s.Set(kindFixtureKey, []byte(value), expiresAt)
			return err
		}
	}
	hset := func(pairs ...string) func(*Store) error {
		return func(s *Store) error {
			fields := make([]HashFieldValue, 0, len(pairs)/2)
			for i := 0; i < len(pairs); i += 2 {
				fields = append(fields, HashFieldValue{Field: pairs[i], Value: []byte(pairs[i+1])})
			}
			_, _, err := s.HSet(kindFixtureKey, fields)
			return err
		}
	}
	sadd := func(members ...string) func(*Store) error {
		return func(s *Store) error {
			_, _, err := s.SAdd(kindFixtureKey, bulk(members...))
			return err
		}
	}
	zadd := func(members ...string) func(*Store) error {
		return func(s *Store) error {
			entries := make([]ZSetEntry, len(members))
			for i, member := range members {
				entries[i] = ZSetEntry{Member: []byte(member), Score: float64(i) + 0.5}
			}
			_, _, err := s.ZAdd(kindFixtureKey, entries)
			return err
		}
	}

	return []kindFixture{
		{name: "string", build: set("hello", false), size: 54},
		{name: "string with a TTL", build: set("hello", true), size: 62},
		{name: "empty string", build: set("", false), size: 49},
		{name: "list", size: 135, build: func(s *Store) error {
			_, _, err := s.RightPush(kindFixtureKey, bulk("a", "bb", "ccc"))
			return err
		}},
		{name: "list with dead slots at its front", size: 118, build: func(s *Store) error {
			if _, _, err := s.RightPush(kindFixtureKey, bulk("a", "bb", "ccc")); err != nil {
				return err
			}
			_, _, err := s.LeftPop(kindFixtureKey)
			return err
		}},
		{name: "compact hash", build: hset("f", "v", "gg", "www"), size: 136},
		{name: "table hash", build: hset("f", "v", "gg", long), size: 198},
		{name: "integer set", build: sadd("1", "22", "-333"), size: 105},
		{name: "table set", build: sadd("1", "bb", "ccc"), size: 135},
		{name: "compact sorted set", build: zadd("m", "nn", "ooo"), size: 159},
		{name: "skip-list sorted set", build: zadd("m", "nn", long), size: 221},
		{name: "stream", size: 160, build: func(s *Store) error {
			if _, _, err := s.XAdd(kindFixtureKey, "1-1", bulk("f", "v")); err != nil {
				return err
			}
			_, _, err := s.XAdd(kindFixtureKey, "1-2", bulk("gg", "www", "h", "x"))
			return err
		}},
	}
}

// TestEveryValueKindKeepsItsContract holds every value kind, in every encoding,
// to what the store relies on from it.
func TestEveryValueKindKeepsItsContract(t *testing.T) {
	for _, fixture := range kindFixtures() {
		fixture := fixture
		t.Run(fixture.name, func(t *testing.T) {
			t.Run("its accounted size is the golden value", func(t *testing.T) {
				store, _ := newStoreWithManualClock()
				store.ConfigureMaxMemory(1<<30, 16)
				if err := fixture.build(store); err != nil {
					t.Fatalf("build error = %v", err)
				}
				if got := store.UsedMemory(); got != fixture.size {
					t.Fatalf("UsedMemory() = %d, want %d", got, fixture.size)
				}
			})
		})
	}
}
