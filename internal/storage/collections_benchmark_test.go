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
