package storage

import (
	"reflect"
	"strconv"
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
	// write is a later write that changes the value build left.
	write func(s *Store) error
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
			err := s.Set(nil, kindFixtureKey, []byte(value), expiresAt)
			return err
		}
	}
	hset := func(pairs ...string) func(*Store) error {
		return func(s *Store) error {
			fields := make([]HashFieldValue, 0, len(pairs)/2)
			for i := 0; i < len(pairs); i += 2 {
				fields = append(fields, HashFieldValue{Field: pairs[i], Value: []byte(pairs[i+1])})
			}
			_, err := s.HSet(nil, kindFixtureKey, fields)
			return err
		}
	}
	sadd := func(members ...string) func(*Store) error {
		return func(s *Store) error {
			_, err := s.SAdd(nil, kindFixtureKey, bulk(members...))
			return err
		}
	}
	zadd := func(members ...string) func(*Store) error {
		return func(s *Store) error {
			entries := make([]ZSetEntry, len(members))
			for i, member := range members {
				entries[i] = ZSetEntry{Member: []byte(member), Score: float64(i) + 0.5}
			}
			_, err := s.ZAdd(nil, kindFixtureKey, entries)
			return err
		}
	}

	// many is more items than one recreation command carries.
	many := make([]string, 2*recreateItemsPerCommand+500)
	manyPairs := make([]string, 0, 2*len(many))
	for i := range many {
		many[i] = "item-" + strconv.Itoa(i)
		manyPairs = append(manyPairs, many[i], "v")
	}
	setBit := func(s *Store) error { _, err := s.SetBit(nil, kindFixtureKey, 7, 1); return err }
	rpush := func(s *Store) error { _, err := s.RightPush(nil, kindFixtureKey, bulk("z")); return err }
	xadd := func(s *Store) error { _, err := s.XAdd(nil, kindFixtureKey, "2-0", bulk("f", "v")); return err }

	return []kindFixture{
		{name: "string", build: set("hello", false), size: 54, write: setBit},
		{name: "string with a TTL", build: set("hello", true), size: 62, write: setBit},
		{name: "empty string", build: set("", false), size: 49, write: setBit},
		{name: "list", size: 135, write: rpush, build: func(s *Store) error {
			_, err := s.RightPush(nil, kindFixtureKey, bulk("a", "bb", "ccc"))
			return err
		}},
		{name: "list with dead slots at its front", size: 118, write: rpush, build: func(s *Store) error {
			if _, err := s.RightPush(nil, kindFixtureKey, bulk("a", "bb", "ccc")); err != nil {
				return err
			}
			_, _, err := s.LeftPop(nil, kindFixtureKey)
			return err
		}},
		{name: "list of more values than one command carries", size: 62671, write: rpush, build: func(s *Store) error {
			_, err := s.RightPush(nil, kindFixtureKey, bulk(many...))
			return err
		}},
		{name: "compact hash", build: hset("f", "v", "gg", "www"), size: 136, write: hset("f", "x")},
		{name: "table hash", build: hset("f", "v", "gg", long), size: 198, write: hset("f", "x")},
		{name: "hash of more fields than one command carries", build: hset(manyPairs...), size: 85603, write: hset("f", "x")},
		{name: "integer set", build: sadd("1", "22", "-333"), size: 105, write: sadd("4")},
		{name: "table set", build: sadd("1", "bb", "ccc"), size: 135, write: sadd("4")},
		{name: "set of more members than one command carries", build: sadd(many...), size: 62671, write: sadd("4")},
		{name: "compact sorted set", build: zadd("m", "nn", "ooo"), size: 159, write: zadd("nn")},
		{name: "skip-list sorted set", build: zadd("m", "nn", long), size: 221, write: zadd("nn")},
		{name: "sorted set of more members than one command carries", build: zadd(many...), size: 83055, write: zadd("nn")},
		{name: "stream", size: 160, write: xadd, build: func(s *Store) error {
			if _, err := s.XAdd(nil, kindFixtureKey, "1-1", bulk("f", "v")); err != nil {
				return err
			}
			_, err := s.XAdd(nil, kindFixtureKey, "1-2", bulk("gg", "www", "h", "x"))
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
			built := func(t *testing.T) *Store {
				t.Helper()
				store, _ := newStoreWithManualClock()
				store.ConfigureMaxMemory(nil, 1<<30, 16)
				if err := fixture.build(store); err != nil {
					t.Fatalf("build error = %v", err)
				}
				return store
			}

			t.Run("its accounted size is the golden value", func(t *testing.T) {
				if got := built(t).UsedMemory(); got != fixture.size {
					t.Fatalf("UsedMemory() = %d, want %d", got, fixture.size)
				}
			})

			t.Run("its image is unchanged by a later write", func(t *testing.T) {
				store := built(t)
				img := store.Image(nil)
				before := recreationCommandsOf(t, img)
				if err := fixture.write(store); err != nil {
					t.Fatalf("write error = %v", err)
				}
				if after := recreationCommandsOf(t, img); !reflect.DeepEqual(after, before) {
					t.Fatalf("the image recreates as %.200q after a later write, want %.200q", after, before)
				}
				if now := recreationCommands(t, store); reflect.DeepEqual(now, before) {
					t.Fatal("the write left the value as it was, so it tests nothing")
				}
			})

			t.Run("a clone and the value it was taken from change independently", func(t *testing.T) {
				// Without a memory limit a write edits the stored value in place,
				// so each write below reaches the very payload that was cloned.
				store, _ := newStoreWithManualClock()
				if err := fixture.build(store); err != nil {
					t.Fatalf("build error = %v", err)
				}
				data := store.valueObjectForTest(kindFixtureKey).data
				if data == nil {
					t.Skip("a string has no payload to clone: a write replaces its bytes")
				}
				if data.empty() {
					t.Fatal("empty() = true for a value that holds items")
				}
				before := recreationCommands(t, store)

				cloned, _ := newStoreWithManualClock()
				cloned.setValueObjectForTest(kindFixtureKey, data.clone(ValueObject{}))
				if got := recreationCommands(t, cloned); !reflect.DeepEqual(got, before) {
					t.Fatalf("the clone recreates as %.200q, want %.200q", got, before)
				}

				if err := fixture.write(cloned); err != nil {
					t.Fatalf("write to the clone error = %v", err)
				}
				if got := recreationCommands(t, store); !reflect.DeepEqual(got, before) {
					t.Fatalf("after a write to its clone the value recreates as %.200q, want %.200q", got, before)
				}
				after := recreationCommands(t, cloned)
				if reflect.DeepEqual(after, before) {
					t.Fatal("the write left the clone as it was, so it tests nothing")
				}

				if err := fixture.write(store); err != nil {
					t.Fatalf("write error = %v", err)
				}
				if got := recreationCommands(t, cloned); !reflect.DeepEqual(got, after) {
					t.Fatalf("after writes to the value its clone recreates as %.200q, want %.200q", got, after)
				}
			})

			t.Run("its recreation commands are the same every time and carry at most 1,024 items", func(t *testing.T) {
				store := built(t)
				first := recreationCommands(t, store)
				if len(first) == 0 {
					t.Fatal("the key recreates with no command")
				}
				if second := recreationCommands(t, store); !reflect.DeepEqual(second, first) {
					t.Fatalf("a second image recreates as %.200q, want %.200q", second, first)
				}
				_, err := store.Image(nil).Recreate(func(args [][]byte) error {
					argsPerItem := map[string]int{"RPUSH": 1, "SADD": 1, "HSET": 2, "ZADD": 2}[string(args[0])]
					if argsPerItem > 0 && (len(args)-2)/argsPerItem > 1024 {
						t.Fatalf("one %s carries %d items, want at most 1,024", args[0], (len(args)-2)/argsPerItem)
					}
					return nil
				})
				if err != nil {
					t.Fatalf("Recreate() error = %v", err)
				}
			})
		})
	}
}

// TestTheKindFixturesCoverEveryValueKind fails when a kind is added to
// kindNames without a row in kindFixtures, which is what holds it to the
// contract above.
func TestTheKindFixturesCoverEveryValueKind(t *testing.T) {
	covered := make(map[ValueKind]bool)
	for _, fixture := range kindFixtures() {
		store := NewStore()
		if err := fixture.build(store); err != nil {
			t.Fatalf("%s: build error = %v", fixture.name, err)
		}
		covered[kindNames[store.valueObjectForTest(kindFixtureKey).kind()]] = true
	}
	for _, kind := range ValueKinds() {
		if !covered[kind] {
			t.Errorf("no kind fixture builds a %s", kind)
		}
	}
}

// TestAnEmptiedValueIsDeletedUnlessItIsAStream pins empty for each kind and
// encoding with nothing in it. A write that leaves a value like this deletes its
// key, except that a stream key stays.
func TestAnEmptiedValueIsDeletedUnlessItIsAStream(t *testing.T) {
	tests := []struct {
		name  string
		value kindValue
		want  bool
	}{
		{name: "list", value: &listValue{}, want: true},
		{name: "list of dead slots only", value: &listValue{items: make([][]byte, 3), head: 3}, want: true},
		{name: "compact hash", value: &hashValue{compact: &CompactHash{}}, want: true},
		{name: "table hash", value: &hashValue{table: map[string][]byte{}}, want: true},
		{name: "integer set", value: &setValue{ints: &IntSet{}}, want: true},
		{name: "table set", value: &setValue{table: map[string]struct{}{}}, want: true},
		{name: "compact sorted set", value: &zsetValue{compact: &CompactZSet{}}, want: true},
		{name: "skip-list sorted set", value: &zsetValue{general: newSortedSet()}, want: true},
		{name: "stream", value: newStream(), want: false},
	}

	seen := make(map[kindID]bool)
	for _, tt := range tests {
		tt := tt
		seen[tt.value.kind()] = true
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.value.empty(); got != tt.want {
				t.Fatalf("empty() = %v, want %v", got, tt.want)
			}
			if got := (&ValueObject{data: tt.value}).emptied(); got != tt.want {
				t.Fatalf("emptied() = %v, want %v", got, tt.want)
			}
		})
	}
	for id := kindString + 1; id < kindCount; id++ {
		if !seen[id] {
			t.Errorf("no row holds an emptied %s", kindNames[id])
		}
	}
}
