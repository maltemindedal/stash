package storage

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

// recountUsedMemory measures the keyspace from scratch, the way the store's own
// full recount does, without the sweep of expired keys that recount also runs.
func recountUsedMemory(s *Store) int64 {
	total := int64(0)
	for i := range s.shards {
		shard := &s.shards[i]
		shard.mu.RLock()
		for key, value := range shard.data {
			total += s.approximateValueObjectSize(key, value)
		}
		shard.mu.RUnlock()
	}
	return total
}

// TestUsedMemoryCounterStaysExact drives every mutating operation with random
// sequences and checks after each one that the incrementally maintained counter
// equals a fresh measurement of the keyspace. Skipping the full recount on every
// growing write is only sound while that holds.
func TestUsedMemoryCounterStaysExact(t *testing.T) {
	keys := []string{"a", "b", "c", "d", "e", "f"}

	for seed := int64(0); seed < 20; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			store := NewStore()
			store.ConfigureMaxMemory(1<<40, 16)
			member := func() []byte { return []byte(fmt.Sprintf("m%d", rng.Intn(12))) }
			members := func() [][]byte {
				out := make([][]byte, 1+rng.Intn(4))
				for i := range out {
					out[i] = member()
				}
				return out
			}

			for step := 0; step < 400; step++ {
				key := keys[rng.Intn(len(keys))]
				var desc string
				switch rng.Intn(21) {
				case 0:
					desc = "Set"
					_, _ = store.Set(key, []byte(fmt.Sprintf("value-%d", rng.Intn(1000))), 0)
				case 1:
					desc = "Set with TTL"
					_, _ = store.Set(key, []byte("ttl"), time.Now().Add(time.Hour).UnixMilli())
				case 2:
					desc = "Delete"
					store.Delete(key)
				case 3:
					desc = "DeleteMany"
					store.DeleteMany([]string{key, keys[rng.Intn(len(keys))]})
				case 4:
					desc = "Increment"
					_, _, _ = store.Increment(key)
				case 5:
					desc = "LeftPush"
					_, _, _ = store.LeftPush(key, members())
				case 6:
					desc = "RightPush"
					_, _, _ = store.RightPush(key, members())
				case 7:
					desc = "LeftPop"
					_, _, _ = store.LeftPop(key)
				case 8:
					desc = "RightPop"
					_, _, _ = store.RightPop(key)
				case 9:
					desc = "LeftPopN"
					_, _, _ = store.LeftPopN(key, int64(1+rng.Intn(3)))
				case 10:
					desc = "RightPopN"
					_, _, _ = store.RightPopN(key, int64(1+rng.Intn(3)))
				case 11:
					desc = "ZAdd"
					_, _, _ = store.ZAdd(key, []ZSetEntry{{Member: member(), Score: float64(rng.Intn(50))}})
				case 12:
					desc = "XAdd"
					_, _, _ = store.XAdd(key, "*", [][]byte{[]byte("f"), []byte("v")})
				case 13:
					desc = "SetBit"
					_, _, _ = store.SetBit(key, int64(rng.Intn(4096)), int64(rng.Intn(2)))
				case 14:
					desc = "PFAdd"
					_, _, _ = store.PFAdd(key, members())
				case 15:
					desc = "HSet"
					_, _, _ = store.HSet(key, []HashFieldValue{{Field: string(member()), Value: []byte("hv")}})
				case 16:
					desc = "HDel"
					_, _ = store.HDel(key, []string{string(member())})
				case 17:
					desc = "SAdd"
					_, _, _ = store.SAdd(key, members())
				case 18:
					desc = "SRem"
					_, _ = store.SRem(key, members())
				case 19:
					desc = "Get"
					_, _, _ = store.Get(key)
				default:
					desc = "Set (expired)"
					_, _ = store.Set(key, []byte("gone"), time.Now().Add(-time.Second).UnixMilli())
				}

				if used, want := store.UsedMemory(), recountUsedMemory(store); used != want {
					t.Fatalf("step %d after %s(%q): UsedMemory() = %d, recount = %d (drift %d)", step, desc, key, used, want, used-want)
				}
			}
		})
	}
}
