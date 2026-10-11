package storage

import (
	"sync/atomic"
	"time"
)

// manualClock is a Store clock that stands still until a test advances it, so
// a test can order clock readings, and carry a key past its TTL deadline,
// without sleeping. It also counts the readings the store takes.
type manualClock struct {
	millis atomic.Int64
	taken  atomic.Int64
}

// newStoreWithManualClock returns an empty Store whose clock reads a fixed
// Unix-millis time until the test advances it.
func newStoreWithManualClock() (*Store, *manualClock) {
	clock := &manualClock{}
	clock.millis.Store(1_700_000_000_000)
	store := NewStore()
	store.clock = clock.read
	return store, clock
}

// now returns the clock's reading in Unix milliseconds without counting it.
func (c *manualClock) now() int64 {
	return c.millis.Load()
}

// read is the store's view of the clock: now, counted as one reading taken.
func (c *manualClock) read() int64 {
	c.taken.Add(1)
	return c.now()
}

// readings returns how many readings the store has taken.
func (c *manualClock) readings() int64 {
	return c.taken.Load()
}

// advance moves the clock forward by d and returns its new reading.
func (c *manualClock) advance(d time.Duration) int64 {
	return c.millis.Add(d.Milliseconds())
}

func (s *Store) setValueObjectForTest(key string, value *ValueObject) {
	shard := s.shardForKey(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	s.setKeyLocked(shard, key, value)
}

func (s *Store) expireKeyForTest(key string, expiresAt int64) bool {
	shard := s.shardForKey(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	value, ok := shard.data[key]
	if !ok {
		return false
	}

	value.ExpiresAt = expiresAt
	return true
}

func (s *Store) lastAccessedAtForTest(key string) int64 {
	shard := s.shardForKey(key)
	shard.mu.RLock()
	defer shard.mu.RUnlock()

	value, ok := shard.data[key]
	if !ok {
		return 0
	}

	return value.lastAccessed()
}

func (s *Store) valueObjectForTest(key string) *ValueObject {
	shard := s.shardForKey(key)
	shard.mu.RLock()
	defer shard.mu.RUnlock()

	return shard.data[key]
}

func scoresOfEntries(entries []ZSetEntry) []float64 {
	scores := make([]float64, 0, len(entries))
	for _, entry := range entries {
		scores = append(scores, entry.Score)
	}
	return scores
}
