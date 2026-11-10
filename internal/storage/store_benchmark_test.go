package storage

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func BenchmarkStore(b *testing.B) {
	payload := []byte("value")
	deleteKeys := make([]string, 0, 128)
	for i := 0; i < 128; i++ {
		deleteKeys = append(deleteKeys, fmt.Sprintf("delete-%d", i))
	}

	snapshotKeys := make([]string, 0, 1024)
	for i := 0; i < 1024; i++ {
		snapshotKeys = append(snapshotKeys, fmt.Sprintf("snapshot-%d", i))
	}

	b.Run("Set same key", func(b *testing.B) {
		store := NewStore()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			_ = store.Set(nil, "hot", payload, 0)
		}
	})

	b.Run("SetBit same key", func(b *testing.B) {
		store := NewStore()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			if _, err := store.SetBit(nil, "bitmap", int64(i%64), 1); err != nil {
				b.Fatalf("SetBit() error = %v", err)
			}
		}
	})

	b.Run("Get hit", func(b *testing.B) {
		store := NewStore()
		_ = store.Set(nil, "hot", payload, 0)
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			if _, ok, err := store.Get("hot"); err != nil {
				b.Fatalf("Get() error = %v", err)
			} else if !ok {
				b.Fatal("Get() ok = false, want true")
			}
		}
	})

	b.Run("Get miss", func(b *testing.B) {
		store := NewStore()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			if _, ok, err := store.Get("missing"); err != nil {
				b.Fatalf("Get() error = %v", err)
			} else if ok {
				b.Fatal("Get() ok = true, want false")
			}
		}
	})

	b.Run("Parallel get hit", func(b *testing.B) {
		store := NewStore()
		_ = store.Set(nil, "hot", payload, 0)
		b.ReportAllocs()

		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if _, ok, err := store.Get("hot"); err != nil {
					b.Fatalf("Get() error = %v", err)
				} else if !ok {
					b.Fatal("Get() ok = false, want true")
				}
			}
		})
	})

	b.Run("Parallel set same key", func(b *testing.B) {
		store := NewStore()
		b.ReportAllocs()

		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_ = store.Set(nil, "hot", payload, 0)
			}
		})
	})

	b.Run("Parallel mixed hot key", func(b *testing.B) {
		store := NewStore()
		_ = store.Set(nil, "hot", payload, 0)
		b.ReportAllocs()

		var op atomic.Uint64
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				switch op.Add(1) % 3 {
				case 0:
					_ = store.Set(nil, "hot", payload, 0)
				case 1:
					_, _, _ = store.Get("hot")
				default:
					_ = store.Delete(nil, "hot")
				}
			}
		})
	})

	b.Run("Parallel disjoint keys", func(b *testing.B) {
		store := NewStore()
		b.ReportAllocs()

		var counter atomic.Uint64
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				key := fmt.Sprintf("key-%d", counter.Add(1)%128)
				_ = store.Set(nil, key, payload, 0)
				_, _, _ = store.Get(key)
			}
		})
	})

	b.Run("Parallel passive eviction", func(b *testing.B) {
		store := NewStore()
		_ = store.Set(nil, "expired", payload, time.Now().Add(-time.Millisecond).UnixMilli())
		b.ReportAllocs()

		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_, _, _ = store.Get("expired")
			}
		})
	})

	b.Run("Parallel active eviction with writes", func(b *testing.B) {
		store := NewStore()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		store.StartEviction(ctx, time.Millisecond, 32)
		b.ReportAllocs()

		var counter atomic.Uint64
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				index := counter.Add(1)
				key := fmt.Sprintf("ttl-%d", index%64)
				expiresAt := time.Now().Add(2 * time.Millisecond).UnixMilli()
				_ = store.Set(nil, key, payload, expiresAt)
				_, _, _ = store.Get(key)
			}
		})
	})

	b.Run("DeleteMany 128 keys", func(b *testing.B) {
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			b.StopTimer()
			store := NewStore()
			for _, key := range deleteKeys {
				_ = store.Set(nil, key, payload, 0)
			}
			b.StartTimer()

			removed := store.DeleteMany(nil, deleteKeys)
			if len(removed) != len(deleteKeys) {
				b.Fatalf("len(DeleteMany()) = %d, want %d", len(removed), len(deleteKeys))
			}
		}
	})

	b.Run("SnapshotAll 1024 strings", func(b *testing.B) {
		store := NewStore()
		for _, key := range snapshotKeys {
			_ = store.Set(nil, key, payload, 0)
		}

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if stats := store.Image(nil).Stats(); stats.TotalKeys != len(snapshotKeys) || stats.ExportedKeys != len(snapshotKeys) {
				b.Fatalf("Image().Stats() = %+v, want total/exported %d", stats, len(snapshotKeys))
			}
		}
	})
}

func BenchmarkStoreMaxMemory(b *testing.B) {
	value := []byte("value-of-about-thirty-two-bytes!")

	// New keys into a store that already holds n keys, with plenty of headroom.
	for _, n := range []int{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("Insert new key with %d keys", n), func(b *testing.B) {
			store := NewStore()
			for i := 0; i < n; i++ {
				if err := store.Set(nil, fmt.Sprintf("seed-%d", i), value, 0); err != nil {
					b.Fatal(err)
				}
			}
			store.ConfigureMaxMemory(nil, 1<<40, 16) // after seeding: with accounting on, each seed write is O(keys)
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if err := store.Set(nil, fmt.Sprintf("new-%d", i), value, 0); err != nil {
					b.Fatal(err)
				}
			}
		})
	}

	// Overwrite one key with a value of the same size.
	b.Run("Overwrite with 10000 keys", func(b *testing.B) {
		store := NewStore()
		for i := 0; i < 10_000; i++ {
			if err := store.Set(nil, fmt.Sprintf("seed-%d", i), value, 0); err != nil {
				b.Fatal(err)
			}
		}
		store.ConfigureMaxMemory(nil, 1<<40, 16)
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if err := store.Set(nil, "seed-1", value, 0); err != nil {
				b.Fatal(err)
			}
		}
	})

	// At the limit, every new key evicts an old one.
	for _, n := range []int{1_000, 10_000} {
		b.Run(fmt.Sprintf("Insert at the limit with %d keys", n), func(b *testing.B) {
			store := NewStore()
			for i := 0; i < n; i++ {
				if err := store.Set(nil, fmt.Sprintf("seed-%d", i), value, 0); err != nil {
					b.Fatal(err)
				}
			}
			store.ConfigureMaxMemory(nil, 1<<40, 16)
			store.ConfigureMaxMemory(nil, store.UsedMemory(), 16)
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if err := store.Set(nil, fmt.Sprintf("new-%d", i), value, 0); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// imageBenchmarkStores are the keyspaces BenchmarkStoreSnapshotAll images and
// BenchmarkRecreate recreates: 100,000 strings, large enough for the value arena
// to grow many times (the 1,024-key case is BenchmarkStore's), and 10,000 keys of
// each collection kind with 16 items each.
func imageBenchmarkStores(b *testing.B) []struct {
	name  string
	keys  int
	store *Store
} {
	b.Helper()

	value := []byte("value-of-about-thirty-two-bytes!")
	items := make([][]byte, 16)
	for i := range items {
		items[i] = []byte(fmt.Sprintf("item-%02d", i))
	}
	kinds := []struct {
		name  string
		keys  int
		write func(s *Store, key string) error
	}{
		{name: "100000 strings", keys: 100_000, write: func(s *Store, key string) error {
			err := s.Set(nil, key, value, 0)
			return err
		}},
		{name: "10000 lists of 16", keys: 10_000, write: func(s *Store, key string) error {
			_, err := s.RightPush(nil, key, items)
			return err
		}},
		{name: "10000 hashes of 16", keys: 10_000, write: func(s *Store, key string) error {
			pairs := make([]HashFieldValue, len(items))
			for i, item := range items {
				pairs[i] = HashFieldValue{Field: string(item), Value: value}
			}
			_, err := s.HSet(nil, key, pairs)
			return err
		}},
		{name: "10000 sets of 16", keys: 10_000, write: func(s *Store, key string) error {
			_, err := s.SAdd(nil, key, items)
			return err
		}},
		{name: "10000 sorted sets of 16", keys: 10_000, write: func(s *Store, key string) error {
			entries := make([]ZSetEntry, len(items))
			for i, item := range items {
				entries[i] = ZSetEntry{Member: item, Score: float64(i)}
			}
			_, err := s.ZAdd(nil, key, entries)
			return err
		}},
		{name: "10000 streams of 16", keys: 10_000, write: func(s *Store, key string) error {
			for i := range items {
				if _, err := s.XAdd(nil, key, fmt.Sprintf("1-%d", i+1), [][]byte{[]byte("field"), value}); err != nil {
					return err
				}
			}
			return nil
		}},
	}

	stores := make([]struct {
		name  string
		keys  int
		store *Store
	}, len(kinds))
	for i, kind := range kinds {
		store := NewStore()
		for j := 0; j < kind.keys; j++ {
			if err := kind.write(store, fmt.Sprintf("key-%d", j)); err != nil {
				b.Fatal(err)
			}
		}
		stores[i].name, stores[i].keys, stores[i].store = kind.name, kind.keys, store
	}
	return stores
}

// BenchmarkStoreSnapshotAll takes a keyspace image, which is the part of an AOF
// rewrite that holds every shard lock.
func BenchmarkStoreSnapshotAll(b *testing.B) {
	for _, tt := range imageBenchmarkStores(b) {
		tt := tt
		b.Run(tt.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if stats := tt.store.Image(nil).Stats(); stats.ExportedKeys != tt.keys {
					b.Fatalf("the image has %d keys, want %d", stats.ExportedKeys, tt.keys)
				}
			}
		})
	}
}

// BenchmarkRecreate emits the recreation commands of a keyspace image that is
// already taken and sorted, which is the part of an AOF rewrite that holds no
// lock.
func BenchmarkRecreate(b *testing.B) {
	for _, tt := range imageBenchmarkStores(b) {
		tt := tt
		b.Run(tt.name, func(b *testing.B) {
			img := tt.store.Image(nil)
			discard := func([][]byte) error { return nil }
			if _, err := img.Recreate(discard); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				keys, err := img.Recreate(discard)
				if err != nil || keys != tt.keys {
					b.Fatalf("Recreate() = (%d, %v), want %d keys", keys, err, tt.keys)
				}
			}
		})
	}
}
