package storage

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

// TestSetBitConcurrentWithMaxMemoryToggle stresses SetBit while memory
// accounting is toggled on and off. Before the fix, setBit read
// maxMemoryEnabled() twice, once to pick the lock scope and once under the
// narrower lock. A toggle in that window let a single-shard SetBit enter the
// cross-shard recalculation path and read/delete other shards' maps without
// holding their locks. This must run clean under the race detector.
func TestSetBitConcurrentWithMaxMemoryToggle(t *testing.T) {
	store := NewStore()
	stop := make(chan struct{})

	var toggler sync.WaitGroup
	toggler.Add(1)
	go func() {
		defer toggler.Done()
		enabled := false
		for {
			select {
			case <-stop:
				return
			default:
			}
			if enabled {
				store.ConfigureMaxMemory(0, 0)
			} else {
				store.ConfigureMaxMemory(64*1024*1024, 8)
			}
			enabled = !enabled
		}
	}()

	var workers sync.WaitGroup
	for i := 0; i < 8; i++ {
		workers.Add(1)
		go func(id int) {
			defer workers.Done()
			key := fmt.Sprintf("bitmap-%d", id)
			for j := 0; j < 3000; j++ {
				if _, _, err := store.SetBit(key, int64(j%1024), int64(j%2)); err != nil && !errors.Is(err, ErrMemoryLimitExceeded) {
					t.Errorf("SetBit error = %v", err)
					return
				}
			}
		}(i)
	}

	workers.Wait()
	close(stop)
	toggler.Wait()
}
