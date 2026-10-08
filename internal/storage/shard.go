package storage

import (
	"hash/maphash"
	"sync"
	"time"
)

const defaultShardCount = 256

// Shard owns a subset of the in-memory keyspace behind its own lock.
type Shard struct {
	mu   sync.RWMutex
	data map[string]*ValueObject
}

func newShards(count int) []Shard {
	if count <= 0 {
		count = defaultShardCount
	}

	shards := make([]Shard, count)
	for i := range shards {
		shards[i].data = make(map[string]*ValueObject)
	}

	return shards
}

// readKey applies fn to the live value stored at key while the key's shard read
// lock is held, and reports whether a live value was found.
//
// It owns everything a read of a single key must get right: taking one clock
// reading, locking the owning shard, treating a missing key and a key past its
// TTL alike, reclaiming the expired key on the way out, refreshing the access
// stamp, and releasing the lock on every path. fn therefore only ever sees a
// live value of unknown kind.
//
// fn runs under the read lock so it can copy whatever it needs before another
// goroutine can mutate the value; it must not retain the value, or memory
// borrowed from it, after it returns. A key that exists but fails fn reports
// found with the error, and does not have its access stamp refreshed: a read
// that could not be served is not an access.
func readKey[T any](s *Store, key string, fn func(value *ValueObject) (T, error)) (T, bool, error) {
	var zero T

	now := time.Now().UnixMilli()
	shard := s.shardForKey(key)

	shard.mu.RLock()
	value, ok := shard.data[key]
	if !ok {
		shard.mu.RUnlock()
		return zero, false, nil
	}
	if isExpired(value, now) {
		shard.mu.RUnlock()

		s.dropIfStillExpired(shard, key)
		return zero, false, nil
	}
	defer shard.mu.RUnlock()

	result, err := fn(value)
	if err != nil {
		return zero, true, err
	}

	value.touch(now)
	return result, true, nil
}

// keyWrite is the state one single-key write runs against: the shard that owns
// the key, the live value stored under it (nil when the key is absent), the
// clock reading the write is anchored to, and whether the store is accounting
// for memory. It is produced by writeKey and is only valid while that call
// holds its locks.
type keyWrite struct {
	store      *Store
	shard      *Shard
	key        string
	current    *ValueObject
	now        int64
	accounting bool
}

// commit stores newValue under the key and reports the keys evicted to make
// room for it.
//
// While the store is accounting for memory the write is sized against the
// pre-write value, so newValue must be a distinct object from current: a write
// that turns out to breach maxmemory fails with ErrMemoryLimitExceeded and
// stores nothing, and the value already under the key must survive that
// unchanged. Without accounting there is nothing to size and newValue may be
// current itself.
func (w keyWrite) commit(newValue *ValueObject) ([]string, error) {
	if w.accounting {
		return w.store.commitValueWithEvictionLocked(w.shard, w.key, w.current, newValue, w.now)
	}

	w.store.setKeyLocked(w.shard, w.key, newValue)
	return nil, nil
}

// commitString stores a string payload of length bytes under the key, written
// by fill, and reports the keys evicted to make room for it.
//
// commit takes a value that already exists, which is the wrong shape when the
// payload is the expensive part of the write: SETBIT can address an offset half
// a gigabyte out, and that buffer must not be allocated and zeroed under every
// shard write lock only for the write to be rejected. A string's size follows
// from its length alone, so this decides the write before it calls fill.
func (w keyWrite) commitString(length int, expiresAt int64, fill func(payload []byte)) ([]string, error) {
	if w.accounting {
		return w.store.commitStringWithEvictionLocked(w.shard, w.key, w.current, length, expiresAt, w.now, fill)
	}

	payload := make([]byte, length)
	fill(payload)
	newValue := newOwnedStringValue(payload, expiresAt)
	newValue.touch(w.now)
	return w.commit(newValue)
}

// writeKey applies fn to the value stored at key while holding the write locks
// that a write of that key needs, and reports the keys evicted to make room for
// it.
//
// It is the write-path counterpart to readKey. Before the operation runs, it
// takes one clock reading, reads the maxmemory setting once, and locks the owning
// shard for writing. It may lock every shard because eviction can cross shard
// boundaries. It also reclaims the key when its TTL has passed and releases all
// locks on every path. fn therefore sees either a live value of unknown kind or
// nil when the key is absent.
//
// fn runs under the write locks and stores its result through the keyWrite it is
// handed; it must not retain that keyWrite, or memory borrowed from the value,
// after it returns.
func writeKey[T any](s *Store, key string, fn func(w keyWrite) (T, []string, error)) (T, []string, error) {
	now := time.Now().UnixMilli()
	// Decide the locking mode and the accounting mode from a single read of the
	// maxmemory setting. Reading it again under the narrower lock would be a
	// TOCTOU: if accounting turned on in between, this call would hold one shard
	// lock yet enter the cross-shard eviction path and touch other shards' maps
	// without holding their locks.
	accounting := s.maxMemoryEnabled()
	if accounting {
		// Registered before the unlock below so it runs after it: an accounted
		// write recalculates the keyspace and sweeps expired keys out of it, and
		// the listener that hears about them must not be entered under a lock.
		defer s.publishExpiredKeys()
		s.writeLockAllShards()
		defer s.writeUnlockAllShards()
	} else {
		shard := s.shardForKey(key)
		shard.mu.Lock()
		defer shard.mu.Unlock()
	}

	shard, current := s.prepareExistingValueLocked(key, now)
	return fn(keyWrite{store: s, shard: shard, key: key, current: current, now: now, accounting: accounting})
}

func (s *Store) shardIndex(key string) int {
	return int(maphash.String(s.seed, key) % uint64(len(s.shards)))
}

func (s *Store) shardForKey(key string) *Shard {
	return &s.shards[s.shardIndex(key)]
}

func (s *Store) readLockAllShards() {
	for i := range s.shards {
		s.shards[i].mu.RLock()
	}
}

func (s *Store) readUnlockAllShards() {
	for i := len(s.shards) - 1; i >= 0; i-- {
		s.shards[i].mu.RUnlock()
	}
}

func (s *Store) writeLockAllShards() {
	for i := range s.shards {
		s.shards[i].mu.Lock()
	}
}

func (s *Store) writeUnlockAllShards() {
	for i := len(s.shards) - 1; i >= 0; i-- {
		s.shards[i].mu.Unlock()
	}
}
