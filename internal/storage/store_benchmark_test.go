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
			_, _ = store.Set("hot", payload, 0)
		}
	})

	b.Run("SetBit same key", func(b *testing.B) {
		store := NewStore()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			if _, _, err := store.SetBit("bitmap", int64(i%64), 1); err != nil {
				b.Fatalf("SetBit() error = %v", err)
			}
		}
	})

	b.Run("Get hit", func(b *testing.B) {
		store := NewStore()
		_, _ = store.Set("hot", payload, 0)
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
		_, _ = store.Set("hot", payload, 0)
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
				_, _ = store.Set("hot", payload, 0)
			}
		})
	})

	b.Run("Parallel mixed hot key", func(b *testing.B) {
		store := NewStore()
		_, _ = store.Set("hot", payload, 0)
		b.ReportAllocs()

		var op atomic.Uint64
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				switch op.Add(1) % 3 {
				case 0:
					_, _ = store.Set("hot", payload, 0)
				case 1:
					_, _, _ = store.Get("hot")
				default:
					_ = store.Delete("hot")
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
				_, _ = store.Set(key, payload, 0)
				_, _, _ = store.Get(key)
			}
		})
	})

	b.Run("Parallel passive eviction", func(b *testing.B) {
		store := NewStore()
		_, _ = store.Set("expired", payload, time.Now().Add(-time.Millisecond).UnixMilli())
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
				_, _ = store.Set(key, payload, expiresAt)
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
				_, _ = store.Set(key, payload, 0)
			}
			b.StartTimer()

			removed := store.DeleteMany(deleteKeys)
			if len(removed) != len(deleteKeys) {
				b.Fatalf("len(DeleteMany()) = %d, want %d", len(removed), len(deleteKeys))
			}
		}
	})

	b.Run("SnapshotAll 1024 strings", func(b *testing.B) {
		store := NewStore()
		for _, key := range snapshotKeys {
			_, _ = store.Set(key, payload, 0)
		}

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			entries, stats := store.SnapshotAll()
			if len(entries) != len(snapshotKeys) {
				b.Fatalf("len(SnapshotAll()) = %d, want %d", len(entries), len(snapshotKeys))
			}
			if stats.TotalKeys != len(snapshotKeys) || stats.ExportedKeys != len(snapshotKeys) {
				b.Fatalf("SnapshotAll() stats = %+v, want total/exported %d", stats, len(snapshotKeys))
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
				if _, err := store.Set(fmt.Sprintf("seed-%d", i), value, 0); err != nil {
					b.Fatal(err)
				}
			}
			store.ConfigureMaxMemory(1<<40, 16) // after seeding: with accounting on, each seed write is O(keys)
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if _, err := store.Set(fmt.Sprintf("new-%d", i), value, 0); err != nil {
					b.Fatal(err)
				}
			}
		})
	}

	// Overwrite one key with a value of the same size.
	b.Run("Overwrite with 10000 keys", func(b *testing.B) {
		store := NewStore()
		for i := 0; i < 10_000; i++ {
			if _, err := store.Set(fmt.Sprintf("seed-%d", i), value, 0); err != nil {
				b.Fatal(err)
			}
		}
		store.ConfigureMaxMemory(1<<40, 16)
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if _, err := store.Set("seed-1", value, 0); err != nil {
				b.Fatal(err)
			}
		}
	})

	// At the limit, every new key evicts an old one.
	for _, n := range []int{1_000, 10_000} {
		b.Run(fmt.Sprintf("Insert at the limit with %d keys", n), func(b *testing.B) {
			store := NewStore()
			for i := 0; i < n; i++ {
				if _, err := store.Set(fmt.Sprintf("seed-%d", i), value, 0); err != nil {
					b.Fatal(err)
				}
			}
			store.ConfigureMaxMemory(1<<40, 16)
			store.ConfigureMaxMemory(store.UsedMemory(), 16)
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if _, err := store.Set(fmt.Sprintf("new-%d", i), value, 0); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkStoreSnapshotAll(b *testing.B) {
	value := []byte("value-of-about-thirty-two-bytes!")

	// The 1,024-key case is BenchmarkStore's; this one is large enough for the
	// value arena to grow many times.
	for _, n := range []int{100_000} {
		b.Run(fmt.Sprintf("%d strings", n), func(b *testing.B) {
			store := NewStore()
			for i := 0; i < n; i++ {
				if _, err := store.Set(fmt.Sprintf("key-%d", i), value, 0); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				entries, _ := store.SnapshotAll()
				if len(entries) != n {
					b.Fatalf("snapshot has %d entries, want %d", len(entries), n)
				}
			}
		})
	}
}
