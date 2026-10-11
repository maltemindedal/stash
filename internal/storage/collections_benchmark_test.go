package storage

import (
	"fmt"
	"testing"
)

func BenchmarkHash(b *testing.B) {
	payload := []byte("value")

	b.Run("HSet new fields", func(b *testing.B) {
		pairs := make([]HashFieldValue, 16)
		for i := range pairs {
			pairs[i] = HashFieldValue{Field: fmt.Sprintf("f%d", i), Value: payload}
		}
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			store := NewStore()
			if _, _, err := store.HSet("h", pairs); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("HSet overwrite", func(b *testing.B) {
		store := NewStore()
		pairs := make([]HashFieldValue, 16)
		for i := range pairs {
			pairs[i] = HashFieldValue{Field: fmt.Sprintf("f%d", i), Value: payload}
		}
		if _, _, err := store.HSet("h", pairs); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if _, _, err := store.HSet("h", pairs); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("HGet hit", func(b *testing.B) {
		store := NewStore()
		if _, _, err := store.HSet("h", []HashFieldValue{{Field: "f", Value: payload}}); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if _, _, err := store.HGet("h", "f"); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("HGetAll 64 fields", func(b *testing.B) {
		store := NewStore()
		pairs := make([]HashFieldValue, 64)
		for i := range pairs {
			pairs[i] = HashFieldValue{Field: fmt.Sprintf("f%d", i), Value: payload}
		}
		if _, _, err := store.HSet("h", pairs); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if _, err := store.HGetAll("h"); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkZSet(b *testing.B) {
	newLargeZSet := func(b *testing.B) *Store {
		b.Helper()
		store := NewStore()
		entries := make([]ZSetEntry, 16384)
		for i := range entries {
			entries[i] = ZSetEntry{Member: fmt.Appendf(nil, "m%d", i), Score: float64(i)}
		}
		if _, _, err := store.ZAdd("z", entries); err != nil {
			b.Fatal(err)
		}
		return store
	}

	b.Run("ZRangeByScores narrow range of 16384", func(b *testing.B) {
		store := newLargeZSet(b)
		scoreRange := ScoreRange{Min: 8192, Max: 8200, MaxExclusive: true}
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if _, err := store.ZRangeByScores("z", scoreRange); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("ZRange full scan of 16384", func(b *testing.B) {
		store := newLargeZSet(b)
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if _, err := store.ZRange("z", 0, -1); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkSet(b *testing.B) {
	b.Run("SAdd unique batch", func(b *testing.B) {
		members := make([][]byte, 16)
		for i := range members {
			members[i] = fmt.Appendf(nil, "m%d", i)
		}
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			store := NewStore()
			if _, _, err := store.SAdd("s", members); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("SAdd all duplicates", func(b *testing.B) {
		store := NewStore()
		members := make([][]byte, 16)
		for i := range members {
			members[i] = fmt.Appendf(nil, "m%d", i)
		}
		if _, _, err := store.SAdd("s", members); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if _, _, err := store.SAdd("s", members); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("SIsMember hit", func(b *testing.B) {
		store := NewStore()
		if _, _, err := store.SAdd("s", [][]byte{[]byte("m")}); err != nil {
			b.Fatal(err)
		}
		member := []byte("m")
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if _, err := store.SIsMember("s", member); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("SRem hit batch", func(b *testing.B) {
		members := make([][]byte, 16)
		for i := range members {
			members[i] = fmt.Appendf(nil, "m%d", i)
		}
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			store := NewStore()
			if _, _, err := store.SAdd("s", members); err != nil {
				b.Fatal(err)
			}
			if _, err := store.SRem("s", members); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("SRem miss batch", func(b *testing.B) {
		store := NewStore()
		if _, _, err := store.SAdd("s", [][]byte{[]byte("present")}); err != nil {
			b.Fatal(err)
		}
		members := make([][]byte, 16)
		for i := range members {
			members[i] = fmt.Appendf(nil, "absent%d", i)
		}
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if _, err := store.SRem("s", members); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("SMembers 64", func(b *testing.B) {
		store := NewStore()
		members := make([][]byte, 64)
		for i := range members {
			members[i] = fmt.Appendf(nil, "m%d", i)
		}
		if _, _, err := store.SAdd("s", members); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if _, err := store.SMembers("s"); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkList(b *testing.B) {
	payload := []byte("value")

	// Push and pop at the same end of a list that already holds n values, so the
	// list stays at length n and each operation is measured at that size.
	for _, n := range []int{100, 10_000, 100_000} {
		filled := func(b *testing.B) *Store {
			store := NewStore()
			values := make([][]byte, n)
			for i := range values {
				values[i] = payload
			}
			if _, _, err := store.RightPush("l", values); err != nil {
				b.Fatal(err)
			}
			return store
		}

		b.Run(fmt.Sprintf("LPush+LPop at length %d", n), func(b *testing.B) {
			store := filled(b)
			one := [][]byte{payload}
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if _, _, err := store.LeftPush("l", one); err != nil {
					b.Fatal(err)
				}
				if _, _, err := store.LeftPop("l"); err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run(fmt.Sprintf("RPush+RPop at length %d", n), func(b *testing.B) {
			store := filled(b)
			one := [][]byte{payload}
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if _, _, err := store.RightPush("l", one); err != nil {
					b.Fatal(err)
				}
				if _, _, err := store.RightPop("l"); err != nil {
					b.Fatal(err)
				}
			}
		})
	}

	// Build a list of 5,000 values by pushing them one at a time.
	for _, build := range []struct {
		name string
		push func(*Store, [][]byte) error
	}{
		{"LPush", func(s *Store, v [][]byte) error { _, _, err := s.LeftPush("l", v); return err }},
		{"RPush", func(s *Store, v [][]byte) error { _, _, err := s.RightPush("l", v); return err }},
	} {
		b.Run("Build 5000 with "+build.name, func(b *testing.B) {
			one := [][]byte{payload}
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				store := NewStore()
				for j := 0; j < 5000; j++ {
					if err := build.push(store, one); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

// BenchmarkAccountedCollections measures collection writes at three sizes, with
// maxmemory off and on. With it on, a write that grows its value takes every
// shard lock and is sized against the limit, and a write that shrinks it takes
// only its own shard's lock. Each growing write is paired with the shrinking
// write that undoes it, so the collection stays at its size; RPOP, with and
// without a count, runs alone. Every call's result is checked, so a write that
// silently did nothing fails the benchmark instead of speeding it up.
func BenchmarkAccountedCollections(b *testing.B) {
	payload := []byte("value")
	extra := []byte("extra")

	modes := []struct {
		name  string
		limit int64
	}{
		{"maxmemory off", 0},
		{"maxmemory on", 1 << 40}, // far above what the benchmark stores, so nothing is evicted
	}

	// filled returns a store holding one collection of n values, made by fill
	// before maxmemory is configured: with it on, every write while filling would
	// take every shard lock.
	filled := func(b *testing.B, limit int64, fill func(*Store) error) *Store {
		b.Helper()
		store := NewStore()
		if err := fill(store); err != nil {
			b.Fatal(err)
		}
		if limit > 0 {
			store.ConfigureMaxMemory(limit, 16)
		}
		return store
	}

	for _, n := range []int{16, 1_000, 10_000} {
		pairs := make([]HashFieldValue, n)
		members := make([][]byte, n)
		values := make([][]byte, n)
		for i := 0; i < n; i++ {
			pairs[i] = HashFieldValue{Field: fmt.Sprintf("f%d", i), Value: payload}
			members[i] = fmt.Appendf(nil, "m%d", i)
			values[i] = payload
		}

		b.Run(fmt.Sprintf("HSet+HDel at %d", n), func(b *testing.B) {
			field := []HashFieldValue{{Field: "extra", Value: payload}}
			fieldName := []string{"extra"}
			for _, mode := range modes {
				b.Run(mode.name, func(b *testing.B) {
					store := filled(b, mode.limit, func(s *Store) error { _, _, err := s.HSet("h", pairs); return err })
					b.ReportAllocs()
					b.ResetTimer()

					for i := 0; i < b.N; i++ {
						if added, _, err := store.HSet("h", field); err != nil || added != 1 {
							b.Fatalf("HSet() = %d, %v, want 1 field added", added, err)
						}
						if removed, err := store.HDel("h", fieldName); err != nil || removed != 1 {
							b.Fatalf("HDel() = %d, %v, want 1 field removed", removed, err)
						}
					}
				})
			}
		})

		b.Run(fmt.Sprintf("SAdd+SRem at %d", n), func(b *testing.B) {
			one := [][]byte{extra}
			for _, mode := range modes {
				b.Run(mode.name, func(b *testing.B) {
					store := filled(b, mode.limit, func(s *Store) error { _, _, err := s.SAdd("s", members); return err })
					b.ReportAllocs()
					b.ResetTimer()

					for i := 0; i < b.N; i++ {
						if added, _, err := store.SAdd("s", one); err != nil || added != 1 {
							b.Fatalf("SAdd() = %d, %v, want 1 member added", added, err)
						}
						if removed, err := store.SRem("s", one); err != nil || removed != 1 {
							b.Fatalf("SRem() = %d, %v, want 1 member removed", removed, err)
						}
					}
				})
			}
		})

		b.Run(fmt.Sprintf("RPush+RPop at %d", n), func(b *testing.B) {
			one := [][]byte{payload}
			for _, mode := range modes {
				b.Run(mode.name, func(b *testing.B) {
					store := filled(b, mode.limit, func(s *Store) error { _, _, err := s.RightPush("l", values); return err })
					b.ReportAllocs()
					b.ResetTimer()

					for i := 0; i < b.N; i++ {
						if length, _, err := store.RightPush("l", one); err != nil || length != int64(n+1) {
							b.Fatalf("RightPush() = %d, %v, want length %d", length, err, n+1)
						}
						if _, ok, err := store.RightPop("l"); err != nil || !ok {
							b.Fatalf("RightPop() ok = %v, err = %v, want a value", ok, err)
						}
					}
				})
			}
		})

		// A pop on its own cannot keep its list at n values, so the pop rows pop
		// lists of n values until they are empty, then refill them with the timer
		// stopped. Stopping the timer reads the memory statistics, which stops the
		// world, so enough lists are filled at once to pay for that only about
		// once every 16,384 values popped. The lists hold n values down to the
		// last pop, which deletes the key. Besides the pop, the timed loop only
		// counts down the values left in the current list and the lists left.
		popLists := func(b *testing.B, limit int64) (*Store, []string, func()) {
			b.Helper()
			keys := make([]string, max(1, 16_384/n))
			for i := range keys {
				keys[i] = fmt.Sprintf("l%d", i)
			}
			store := NewStore()
			if limit > 0 {
				store.ConfigureMaxMemory(limit, 16)
			}
			refill := func() {
				for _, key := range keys {
					if _, _, err := store.RightPush(key, values); err != nil {
						b.Fatal(err)
					}
				}
			}
			return store, keys, refill
		}

		b.Run(fmt.Sprintf("RPop from %d", n), func(b *testing.B) {
			for _, mode := range modes {
				b.Run(mode.name, func(b *testing.B) {
					store, keys, refill := popLists(b, mode.limit)
					list, left := 0, 0 // the list being popped, and the values it has left
					b.ReportAllocs()
					b.ResetTimer()

					for i := 0; i < b.N; i++ {
						if left == 0 {
							if list == 0 {
								b.StopTimer()
								refill()
								list = len(keys)
								b.StartTimer()
							}
							list--
							left = n
						}
						left--
						if _, ok, err := store.RightPop(keys[list]); err != nil || !ok {
							b.Fatalf("RightPop() ok = %v, err = %v, want a value", ok, err)
						}
					}
				})
			}
		})

		// RPOP with a count, which pops through RightPopN. Each n is a multiple of
		// the count, so every call pops a full count.
		const popCount = 4
		b.Run(fmt.Sprintf("RPop count %d from %d", popCount, n), func(b *testing.B) {
			for _, mode := range modes {
				b.Run(mode.name, func(b *testing.B) {
					store, keys, refill := popLists(b, mode.limit)
					list, left := 0, 0 // the list being popped, and the values it has left
					b.ReportAllocs()
					b.ResetTimer()

					for i := 0; i < b.N; i++ {
						if left == 0 {
							if list == 0 {
								b.StopTimer()
								refill()
								list = len(keys)
								b.StartTimer()
							}
							list--
							left = n
						}
						left -= popCount
						if popped, ok, err := store.RightPopN(keys[list], popCount); err != nil || !ok || len(popped) != popCount {
							b.Fatalf("RightPopN() = %d values, ok = %v, err = %v, want %d values", len(popped), ok, err, popCount)
						}
					}
				})
			}
		})
	}
}
