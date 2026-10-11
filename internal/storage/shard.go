package storage

import (
	"errors"
	"hash/maphash"
	"sync"
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

	now := s.now()
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

// commit stores newValue under the key, stamped with the write's clock reading
// as its access, and reports the keys evicted to make room for it.
//
// While the store is accounting for memory the write is sized against the
// pre-write value, so newValue must be a distinct object from current: a write
// that turns out to breach maxmemory fails with ErrMemoryLimitExceeded and
// stores nothing, and the value already under the key must survive that
// unchanged. Without accounting there is nothing to size and newValue may be
// current itself.
func (w keyWrite) commit(newValue *ValueObject) ([]string, error) {
	newValue.touch(w.now)
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
	return w.commit(newOwnedStringValue(payload, expiresAt))
}

// writeKey applies fn to the value stored at key while holding the write locks
// that a write of that key needs, and reports the keys evicted to make room for
// it.
//
// It serves the growing writes of a string: SET, INCR, SETBIT and PFADD. Every
// other write goes through mutateKey. Before the operation runs, it takes one
// clock reading, reads the maxmemory setting once, and locks the owning shard
// for writing. It may lock every shard because eviction can cross shard
// boundaries. It also reclaims the key when its TTL has passed and releases all
// locks on every path. fn therefore sees either a live value of unknown kind or
// nil when the key is absent. The one clock reading decides every expiry the
// write sees: its own key's and, under maxmemory, those in the sweep that makes
// room for it, so the sweep cannot remove a key fn was handed as live.
//
// fn runs under the write locks and stores its result through the keyWrite it is
// handed; it must not retain that keyWrite, or memory borrowed from the value,
// after it returns.
func writeKey[T any](s *Store, key string, fn func(w keyWrite) (T, []string, error)) (T, []string, error) {
	now := s.now()
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

// anyKind is the want of a mutation that accepts a value of every kind, as DEL
// does.
const anyKind ValueKind = ""

// growth is whether a write may add bytes to its key's value. It is a property
// of the operation, not of the store's mode, and decides what a mutation locks
// and whether it may evict while the store accounts for memory.
type growth uint8

const (
	// shrinks is a shrinking write: a pop, HDEL, SREM or DEL. It can only remove
	// bytes, so even under maxmemory it locks only its key's shard, and it
	// neither evicts nor sweeps.
	shrinks growth = iota
	// mayGrow is every other write. Under maxmemory it locks every shard, is
	// sized against the limit, and evicts other keys to make room.
	mayGrow
)

// mutateKey applies fn to the value stored at key, and returns fn's result with
// the keys evicted to make room for it.
//
// mutateKey takes one clock reading, locks for g, reclaims the key when its TTL
// has passed, and answers ErrWrongType, without calling fn, for a live value of
// a kind other than want (anyKind accepts every kind). fn gets the live value,
// or nil when the key is absent, and the clock reading. It returns what the key
// holds after it: nil deletes the key, cur keeps the edit fn made to it in place,
// and for a growing write any other value replaces it. A list, hash, set or
// sorted set that is left empty is deleted, while an empty string or stream
// stays. A value that stays is stamped with the clock reading, and the memory
// counter takes the change in its size. When fn fails it must have left cur
// unchanged: its error is returned, and the key is neither stamped nor deleted.
//
// A shrinking write never needs room, so even under maxmemory it locks only its
// key's shard, and it neither evicts nor sweeps; it returns nil or cur, never
// another value. A growing write under maxmemory locks every shard, because
// making room for it can evict a key from any of them, and it is handed a
// private copy as cur (see containerCopy), so that a write refused with
// ErrMemoryLimitExceeded leaves the stored value as it was.
//
// fn runs under the locks and must not retain cur, or memory borrowed from it,
// after it returns. mutateKey releases the locks on every path and calls nothing
// outside the store while it holds them, so a caller that wakes a list waiter
// does so after it returns.
func mutateKey[T any](s *Store, key string, want ValueKind, g growth, fn func(cur *ValueObject, now int64) (*ValueObject, T, error)) (T, []string, error) {
	if g == mayGrow {
		return mutateGrowing(s, key, want, fn)
	}

	// The rest is the shrinking path, written out here rather than in a function
	// of its own so that a pop, the commonest shrinking write, pays for no second
	// call. It reads the maxmemory setting under the key's shard lock, where it
	// cannot change: ConfigureMaxMemory changes it under every shard lock.
	var zero T

	now := s.now()
	shard := s.shardForKey(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	cur := shard.data[key]
	if cur != nil && isExpired(cur, now) {
		s.deleteKeyLocked(shard, key)
		cur = nil
	}
	if cur != nil && want != anyKind && cur.Kind != want {
		return zero, nil, ErrWrongType
	}
	if s.maxMemoryEnabled() {
		result, err := mutateShrinkingAccounted(s, shard, key, cur, now, fn)
		return result, nil, err
	}

	// Without a memory limit there is nothing to size, and this path does no
	// more than the hand-written ones it replaced.
	next, result, err := fn(cur, now)
	if err != nil {
		return zero, nil, err
	}
	if next != nil && next != cur {
		panic(errNotAShrinkingWrite)
	}
	if cur == nil {
		return result, nil, nil
	}
	if next == nil || cur.emptied() {
		s.removeKeyLocked(shard, key)
		return result, nil, nil
	}
	cur.touch(now)
	return result, nil, nil
}

// errNotAShrinkingWrite is the panic of a mutation that returned a value other
// than nil or the one it was given, which a shrinking write never does.
var errNotAShrinkingWrite = errors.New("storage: a shrinking write returned a value other than the one it was given")

// mutateShrinkingAccounted is mutateKey's path for a shrinking write while the
// store accounts for memory: it sizes the value before and after fn and moves
// the counter by the difference. The caller holds the key's shard lock and has
// already reclaimed an expired key and checked the kind.
func mutateShrinkingAccounted[T any](s *Store, shard *Shard, key string, cur *ValueObject, now int64, fn func(cur *ValueObject, now int64) (*ValueObject, T, error)) (T, error) {
	var zero T
	var oldSize int64
	if cur != nil {
		oldSize = s.approximateValueObjectSize(key, cur)
	}

	next, result, err := fn(cur, now)
	if err != nil {
		return zero, err
	}
	if next != nil && next != cur {
		panic(errNotAShrinkingWrite)
	}
	if cur == nil {
		return result, nil
	}
	if next == nil || cur.emptied() {
		s.deleteKeyWithSizeLocked(shard, key, oldSize)
		return result, nil
	}
	cur.touch(now)
	s.usedMemory.Add(s.approximateValueObjectSize(key, cur) - oldSize)
	return result, nil
}

// mutateGrowing is mutateKey for a growing write.
func mutateGrowing[T any](s *Store, key string, want ValueKind, fn func(cur *ValueObject, now int64) (*ValueObject, T, error)) (T, []string, error) {
	var zero T

	now := s.now()
	// Decide the locking mode and the accounting mode from a single read of the
	// maxmemory setting. Reading it again under the narrower lock would be a
	// TOCTOU: if accounting turned on in between, this call would hold one shard
	// lock yet enter the cross-shard eviction path and touch other shards' maps
	// without holding their locks.
	if s.maxMemoryEnabled() {
		return mutateGrowingAccounted(s, key, want, now, fn)
	}

	shard := s.shardForKey(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	cur := shard.data[key]
	if cur != nil && isExpired(cur, now) {
		s.deleteKeyLocked(shard, key)
		cur = nil
	}
	if cur != nil && want != anyKind && cur.Kind != want {
		return zero, nil, ErrWrongType
	}

	next, result, err := fn(cur, now)
	if err != nil {
		return zero, nil, err
	}
	switch {
	case next == nil || next.emptied():
		if cur != nil {
			s.removeKeyLocked(shard, key)
		}
	case next == cur:
		cur.touch(now)
	default:
		next.touch(now)
		s.setKeyLocked(shard, key, next)
	}
	return result, nil, nil
}

// mutateGrowingAccounted is mutateGrowing while the store accounts for memory.
// It locks every shard, hands fn a container copy of the stored value, sizes
// what fn returns against the stored value, and makes room for the difference
// before it stores it. The one clock reading decides every expiry the write
// sees: its own key's and those in the sweep that makes room for it, so the
// sweep cannot remove a key fn was handed as live.
func mutateGrowingAccounted[T any](s *Store, key string, want ValueKind, now int64, fn func(cur *ValueObject, now int64) (*ValueObject, T, error)) (T, []string, error) {
	var zero T

	// Registered before the unlock below so it runs after it: an accounted write
	// recalculates the keyspace and sweeps expired keys out of it, and the
	// listener that hears about them must not be entered under a lock.
	defer s.publishExpiredKeys()
	s.writeLockAllShards()
	defer s.writeUnlockAllShards()

	shard, stored := s.prepareExistingValueLocked(key, now)
	if stored != nil && want != anyKind && stored.Kind != want {
		return zero, nil, ErrWrongType
	}
	var (
		cur     *ValueObject
		oldSize int64
	)
	if stored != nil {
		cur = stored.containerCopy()
		oldSize = s.approximateValueObjectSize(key, stored)
	}

	next, result, err := fn(cur, now)
	if err != nil {
		return zero, nil, err
	}
	if next == nil || next.emptied() {
		if stored != nil {
			s.deleteKeyWithSizeLocked(shard, key, oldSize)
		}
		return result, nil, nil
	}

	newSize := s.approximateValueObjectSize(key, next)
	evicted, err := s.ensureMemoryAvailableLocked(newSize-oldSize, protectedKeys(key), now)
	if err != nil {
		return zero, nil, err
	}
	next.touch(now)
	s.setKeyLocked(shard, key, next)
	s.usedMemory.Add(newSize - oldSize)
	return result, evicted, nil
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
