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

// replaceKey stores value under key in place of whatever the key held, and
// reports the keys evicted to make room for it.
//
// It serves SET, which writes a whole value: what the key held does not shape
// the new value, so replaceKey never copies it, and only sizes it while the
// store accounts for memory. It takes one clock reading, which stamps value and,
// under maxmemory, decides whether the key's TTL has passed and which keys the
// sweep that makes room removes. Under maxmemory it locks every shard, because
// making room can evict a key from any of them, and a write refused with
// ErrMemoryLimitExceeded stores nothing.
func replaceKey(s *Store, key string, value *ValueObject) ([]string, error) {
	now := s.now()
	value.touch(now)
	// One read of the maxmemory setting decides the locks, as in mutateGrowing.
	if s.maxMemoryEnabled() {
		// Registered before the unlock below so it runs after it, as in
		// mutateGrowingAccounted.
		defer s.publishExpiredKeys()
		s.writeLockAllShards()
		defer s.writeUnlockAllShards()

		shard, stored := s.prepareExistingValueLocked(key, now)
		oldSize := s.approximateValueObjectSize(key, stored)
		newSize := s.approximateValueObjectSize(key, value)
		evicted, err := s.ensureMemoryAvailableLocked(newSize-oldSize, protectedKeys(key), now)
		if err != nil {
			return nil, err
		}
		s.setKeyLocked(shard, key, value)
		s.usedMemory.Add(newSize - oldSize)
		return evicted, nil
	}

	// Without a memory limit an expired value needs no reclaiming first: it is
	// replaced like a live one.
	shard := s.shardForKey(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	s.setKeyLocked(shard, key, value)
	return nil, nil
}

// mutateString applies fn, a write of a string's bytes, to the string stored at
// key, and returns fn's result with the keys evicted to make room for it.
//
// It serves INCR, SETBIT and PFADD. Like mutateKey for a growing write, it takes
// one clock reading, locks the key's shard, or every shard under maxmemory,
// reclaims the key when its TTL has passed, and answers ErrWrongType, without
// calling fn, for a live value that is not a string. fn reads the string through
// the stringWrite it is handed and writes it through Payload, which it calls at
// most once, and only once nothing else can make it fail. A write that fails
// before it calls Payload leaves the key as it was.
//
// fn runs under the locks and must not retain the stringWrite, or memory
// borrowed from the string, after it returns.
func mutateString[T any](s *Store, key string, fn func(w stringWrite) (T, []string, error)) (T, []string, error) {
	var zero T

	now := s.now()
	// One read of the maxmemory setting decides the locks, as in mutateGrowing.
	accounting := s.maxMemoryEnabled()
	if accounting {
		// Registered before the unlock below so it runs after it, as in
		// mutateGrowingAccounted.
		defer s.publishExpiredKeys()
		s.writeLockAllShards()
		defer s.writeUnlockAllShards()
	} else {
		shard := s.shardForKey(key)
		shard.mu.Lock()
		defer shard.mu.Unlock()
	}

	shard, cur := s.prepareExistingValueLocked(key, now)
	if cur != nil && cur.Kind != ValueKindString {
		return zero, nil, ErrWrongType
	}
	return fn(stringWrite{store: s, shard: shard, key: key, cur: cur, now: now, accounting: accounting})
}

// stringWrite is what a write of a string's bytes runs against: the live string
// stored under the key, if any, and what Payload needs to replace it. mutateString
// hands it to fn by value, which keeps it off the heap, and its methods take a
// pointer to fn's copy, so that a call does not copy it again: with value
// receivers SETBIT within a string was 7% to 13% slower. It is valid only while
// mutateString holds its locks.
type stringWrite struct {
	store      *Store
	shard      *Shard
	key        string
	cur        *ValueObject // nil when the key is absent
	now        int64
	accounting bool
}

// Current returns the string stored under the key and whether the key holds
// one. It is for reading: a write changes the string's bytes only through
// Payload.
func (w *stringWrite) Current() ([]byte, bool) {
	if w.cur == nil {
		return nil, false
	}
	return w.cur.String, true
}

// Payload resizes the string stored under the key to length bytes, creating it
// when the key is absent, and returns its bytes for the write to edit, with the
// keys evicted to make room for it. It stamps the key with the write's clock
// reading.
//
// When the string already has length bytes, Payload returns the stored buffer
// itself, and the write edits it in place: the key's size does not change, so
// there is nothing to size and the write cannot be refused. Otherwise it stores a
// new buffer holding as many of the stored bytes as fit, zero after them, and
// leaves the old buffer as it was (#46 decision 2). While the store accounts for
// memory, the new buffer is sized, and room made for it, before it is allocated:
// SETBIT can address an offset half a gigabyte out, and that buffer must not be
// allocated and zeroed under every shard lock only for the write to be refused.
// A refused write gets ErrMemoryLimitExceeded and leaves the key as it was. A
// stored string keeps its TTL.
func (w *stringWrite) Payload(length int) ([]byte, []string, error) {
	if w.cur != nil && len(w.cur.String) == length {
		w.cur.touch(w.now)
		return w.cur.String, nil, nil
	}
	return w.resize(length)
}

// resize is Payload for a length the stored string does not have, or for an
// absent key: it admits the new length and copies the string into a new buffer.
func (w *stringWrite) resize(length int) ([]byte, []string, error) {
	var evicted []string
	if w.accounting {
		var expiresAt int64
		if w.cur != nil {
			expiresAt = w.cur.ExpiresAt
		}
		oldSize := w.store.approximateValueObjectSize(w.key, w.cur)
		newSize := w.store.approximateStringValueObjectSize(w.key, length, expiresAt)
		var err error
		evicted, err = w.store.ensureMemoryAvailableLocked(newSize-oldSize, protectedKeys(w.key), w.now)
		if err != nil {
			return nil, nil, err
		}
		w.store.usedMemory.Add(newSize - oldSize)
	}

	payload := make([]byte, length)
	if w.cur == nil {
		value := newOwnedStringValue(payload, 0)
		value.touch(w.now)
		w.store.setKeyLocked(w.shard, w.key, value)
		return payload, evicted, nil
	}
	copy(payload, w.cur.String)
	w.cur.String = payload
	w.cur.touch(w.now)
	return payload, evicted, nil
}

// deleteKeys removes several keys, as DEL does, and returns the ones that held
// a live value, in no particular order.
//
// It is mutateKey's counterpart for a DEL of several keys, which DeleteMany
// hands it; one key goes through Delete. Deleting is a shrinking write, so even
// under maxmemory deleteKeys locks only the keys' own shards, in ascending
// order, and it neither evicts nor sweeps. It takes one clock reading under
// those locks, by which a key whose TTL has passed is absent: it is reclaimed
// and not reported.
func deleteKeys(s *Store, keys []string) []string {
	groups := s.groupKeysByShard(keys)
	groups.lock(s)
	defer groups.unlock(s)

	now := s.now()
	removed := make([]string, 0, len(keys))
	for shardID, count := range groups.counts {
		if count == 0 {
			continue
		}
		shard := &s.shards[shardID]
		for _, key := range groups.keysForShard(shardID) {
			value, ok := shard.data[key]
			if !ok {
				continue
			}
			s.deleteKeyLocked(shard, key)
			if !isExpired(value, now) {
				removed = append(removed, key)
			}
		}
	}

	return removed
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
