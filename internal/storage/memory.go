package storage

import (
	"time"
)

const (
	approxValueObjectOverhead int64 = 48
	approxCollectionOverhead  int64 = 32
	approxListEntryOverhead   int64 = 16
	approxHashEntryOverhead   int64 = 24
	approxSetEntryOverhead    int64 = 16
	approxZSetEntryOverhead   int64 = 24
	approxStreamEntryOverhead int64 = 32
	maxInt64Value                   = int64(^uint64(0) >> 1)
)

// ConfigureMaxMemory enables approximate keyspace accounting and probabilistic
// LRU eviction for subsequent writes. A limit of 0 disables the feature.
func (s *Store) ConfigureMaxMemory(limit int64, sampleSize int) {
	if s == nil {
		return
	}
	if limit < 0 {
		limit = 0
	}
	if sampleSize <= 0 {
		sampleSize = defaultSampleSize
	}

	s.writeLockAllShards()
	s.maxMemory.Store(limit)
	s.memoryEvictionSampleSize = sampleSize
	if limit > 0 {
		s.recalculateUsedMemoryLocked(time.Now().UnixMilli())
	} else {
		s.usedMemory.Store(0)
	}
	s.writeUnlockAllShards()
	s.publishExpiredKeys()
}

// MaxMemory returns the configured approximate keyspace memory limit in bytes.
func (s *Store) MaxMemory() int64 {
	if s == nil {
		return 0
	}

	return s.maxMemory.Load()
}

// UsedMemory returns the current approximate keyspace memory usage in bytes.
func (s *Store) UsedMemory() int64 {
	if s == nil {
		return 0
	}

	return s.usedMemory.Load()
}

// EnforceMaxMemory evicts least-recently-used candidates until the keyspace is
// at or below the configured maxmemory limit.
func (s *Store) EnforceMaxMemory() ([]string, error) {
	if !s.maxMemoryEnabled() {
		return nil, nil
	}

	defer s.publishExpiredKeys()
	s.writeLockAllShards()
	defer s.writeUnlockAllShards()

	limit := s.maxMemory.Load()
	s.recalculateUsedMemoryLocked(time.Now().UnixMilli())
	if s.usedMemory.Load() <= limit {
		return nil, nil
	}

	return s.evictUntilMemoryAtOrBelowLocked(limit, nil)
}

func (s *Store) approximateValueObjectSize(key string, value *ValueObject) int64 {
	if value == nil {
		return 0
	}

	switch value.Kind {
	case ValueKindString:
		return s.approximateStringValueObjectSize(key, len(value.String), value.ExpiresAt)
	case ValueKindList:
		size := approximateBaseValueObjectSize(key, value.ExpiresAt)
		list := value.liveList()
		size += approxCollectionOverhead + int64(len(list))*approxListEntryOverhead
		for _, item := range list {
			size += int64(len(item))
		}
		return size
	case ValueKindHash:
		size := approximateBaseValueObjectSize(key, value.ExpiresAt)
		size += approxCollectionOverhead
		if value.HashEncoding == ValueEncodingCompact {
			if value.CompactHash != nil {
				size += int64(len(value.CompactHash.entries))*approxHashEntryOverhead + int64(len(value.CompactHash.arena))
			}
			return size
		}
		size += int64(len(value.Hash)) * approxHashEntryOverhead
		for field, raw := range value.Hash {
			size += int64(len(field) + len(raw))
		}
		return size
	case ValueKindSet:
		size := approximateBaseValueObjectSize(key, value.ExpiresAt)
		size += approxCollectionOverhead
		if value.SetEncoding == ValueEncodingCompact {
			if value.IntSet != nil {
				size += int64(value.IntSet.len()) * 8
			}
			return size
		}
		size += int64(len(value.Set)) * approxSetEntryOverhead
		for member := range value.Set {
			size += int64(len(member))
		}
		return size
	case ValueKindZSet:
		size := approximateBaseValueObjectSize(key, value.ExpiresAt)
		size += approxCollectionOverhead
		if value.ZSetEncoding == ValueEncodingCompact {
			if value.CompactZSet != nil {
				size += int64(len(value.CompactZSet.entries))*approxZSetEntryOverhead + int64(len(value.CompactZSet.arena))
			}
			return size
		}
		if value.ZSet != nil {
			size += int64(len(value.ZSet.index)) * approxZSetEntryOverhead
			for member := range value.ZSet.index {
				size += int64(len(member))
			}
		}
		return size
	case ValueKindStream:
		size := approximateBaseValueObjectSize(key, value.ExpiresAt)
		size += approxCollectionOverhead
		if value.Stream != nil {
			size += int64(len(value.Stream.entries)) * approxStreamEntryOverhead
			for _, record := range value.Stream.entries {
				size += int64(len(record.idText))
				for _, item := range record.values {
					size += int64(len(item))
				}
			}
		}
		return size
	}

	return approximateBaseValueObjectSize(key, value.ExpiresAt)
}

func (s *Store) approximateStringValueObjectSize(key string, length int, expiresAt int64) int64 {
	return approximateBaseValueObjectSize(key, expiresAt) + int64(length)
}

func approximateBaseValueObjectSize(key string, expiresAt int64) int64 {
	size := approxValueObjectOverhead + int64(len(key))
	if expiresAt > 0 {
		size += 8
	}
	return size
}

func (s *Store) commitValueWithEvictionLocked(shard *Shard, key string, oldValue *ValueObject, newValue *ValueObject) ([]string, error) {
	oldSize := s.approximateValueObjectSize(key, oldValue)
	newSize := s.approximateValueObjectSize(key, newValue)

	evicted, err := s.ensureMemoryAvailableLocked(newSize-oldSize, protectedKeys(key))
	if err != nil {
		return nil, err
	}

	s.setKeyLocked(shard, key, newValue)
	s.usedMemory.Add(newSize - oldSize)
	return evicted, nil
}

// commitStringWithEvictionLocked stores a string payload of length bytes under
// key, written by fill, and reports the keys evicted to make room for it.
//
// It is commitValueWithEvictionLocked for a value that does not exist yet. A
// string's size follows from its length alone, so the write is sized, and
// evicted for, before the payload is allocated: SETBIT can address an offset
// half a gigabyte out, and that buffer must not be allocated and zeroed under
// every shard write lock only for the write to be rejected.
func (s *Store) commitStringWithEvictionLocked(shard *Shard, key string, oldValue *ValueObject, length int, expiresAt int64, now int64, fill func(payload []byte)) ([]string, error) {
	oldSize := s.approximateValueObjectSize(key, oldValue)
	newSize := s.approximateStringValueObjectSize(key, length, expiresAt)

	evicted, err := s.ensureMemoryAvailableLocked(newSize-oldSize, protectedKeys(key))
	if err != nil {
		return nil, err
	}

	payload := make([]byte, length)
	fill(payload)
	newValue := newOwnedStringValue(payload, expiresAt)
	newValue.touch(now)

	s.setKeyLocked(shard, key, newValue)
	s.usedMemory.Add(newSize - oldSize)
	return evicted, nil
}

// recalculateUsedMemoryLocked re-measures the whole keyspace, dropping the
// expired keys it passes on the way. Those drops are a sweep rather than the
// caller's own mutation, so they are queued for the expiration listener instead
// of being folded into whatever the caller is returning.
func (s *Store) recalculateUsedMemoryLocked(now int64) int64 {
	used := int64(0)
	var expired []string
	for i := range s.shards {
		shard := &s.shards[i]
		for key, value := range shard.data {
			if isExpired(value, now) {
				s.removeKeyLocked(shard, key)
				expired = append(expired, key)
				continue
			}
			used += s.approximateValueObjectSize(key, value)
		}
	}
	s.usedMemory.Store(used)
	s.noteExpiredKeysLocked(expired)
	return used
}

func (s *Store) ensureMemoryAvailableLocked(delta int64, protected map[string]struct{}) ([]string, error) {
	if !s.maxMemoryEnabled() || delta <= 0 {
		return nil, nil
	}

	current := s.recalculateUsedMemoryLocked(time.Now().UnixMilli())
	limit := s.maxMemory.Load()
	targetUsed := limit - delta
	if targetUsed < 0 {
		return nil, ErrMemoryLimitExceeded
	}
	if current <= targetUsed {
		return nil, nil
	}
	if current-s.totalEvictableMemoryLocked(protected) > targetUsed {
		return nil, ErrMemoryLimitExceeded
	}

	return s.evictUntilMemoryAtOrBelowLocked(targetUsed, protected)
}

func (s *Store) evictUntilMemoryAtOrBelowLocked(targetUsed int64, protected map[string]struct{}) ([]string, error) {
	evicted := make([]string, 0)
	for s.usedMemory.Load() > targetUsed {
		candidate := s.findStalestCandidateLocked(protected)
		if candidate == "" {
			break
		}

		if s.deleteKeyLocked(s.shardForKey(candidate), candidate) {
			evicted = append(evicted, candidate)
		}
	}

	if s.usedMemory.Load() > targetUsed {
		return nil, ErrMemoryLimitExceeded
	}

	return evicted, nil
}

func (s *Store) findStalestCandidateLocked(protected map[string]struct{}) string {
	sampled := s.sampleKeysLocked(s.memoryEvictionSampleSize)
	stalestKey := ""
	stalestAt := maxInt64Value
	for _, key := range sampled {
		if _, blocked := protected[key]; blocked {
			continue
		}
		value, ok := s.shardForKey(key).data[key]
		if !ok {
			continue
		}
		accessedAt := value.lastAccessed()
		if accessedAt < stalestAt {
			stalestAt = accessedAt
			stalestKey = key
		}
	}
	if stalestKey != "" {
		return stalestKey
	}

	for i := range s.shards {
		for key, value := range s.shards[i].data {
			if _, blocked := protected[key]; blocked {
				continue
			}
			accessedAt := value.lastAccessed()
			if accessedAt < stalestAt {
				stalestAt = accessedAt
				stalestKey = key
			}
		}
	}

	return stalestKey
}

func (s *Store) totalEvictableMemoryLocked(protected map[string]struct{}) int64 {
	total := int64(0)
	for i := range s.shards {
		for key, value := range s.shards[i].data {
			if _, blocked := protected[key]; blocked {
				continue
			}
			total += s.approximateValueObjectSize(key, value)
		}
	}

	return total
}

func (s *Store) sampleKeysLocked(limit int) []string {
	if limit <= 0 {
		return nil
	}

	keys := make([]string, 0, limit)
	start := int(time.Now().UnixNano() % int64(len(s.shards)))
	for offset := 0; offset < len(s.shards) && len(keys) < limit; offset++ {
		index := (start + offset) % len(s.shards)
		for key := range s.shards[index].data {
			keys = append(keys, key)
			if len(keys) == limit {
				break
			}
		}
	}

	return keys
}

func (s *Store) prepareExistingValueLocked(key string, now int64) (*Shard, *ValueObject) {
	shard := s.shardForKey(key)
	value, ok := shard.data[key]
	if ok && isExpired(value, now) {
		s.deleteKeyLocked(shard, key)
		return shard, nil
	}

	return shard, value
}

func (s *Store) deleteKeyWithSizeLocked(shard *Shard, key string, size int64) bool {
	if shard == nil {
		return false
	}

	if _, ok := shard.data[key]; !ok {
		return false
	}
	if s.maxMemoryEnabled() {
		s.usedMemory.Add(-size)
	}
	s.removeKeyLocked(shard, key)
	return true
}

// dropIfStillExpired reclaims a key a reader found expired. The caller must
// have released its read lock first: the write lock is a separate acquisition,
// so another goroutine may have deleted, rewritten, or renewed the key in
// between. The expiry is therefore rechecked under the write lock rather than
// carried over from the read, and against a fresh clock reading.
func (s *Store) dropIfStillExpired(shard *Shard, key string) {
	if shard == nil {
		return
	}

	shard.mu.Lock()
	defer shard.mu.Unlock()

	if value, ok := shard.data[key]; ok && isExpired(value, time.Now().UnixMilli()) {
		s.deleteKeyLocked(shard, key)
	}
}

func (s *Store) deleteKeyLocked(shard *Shard, key string) bool {
	if shard == nil {
		return false
	}

	value, ok := shard.data[key]
	if !ok {
		return false
	}
	size := int64(0)
	if s.maxMemoryEnabled() {
		size = s.approximateValueObjectSize(key, value)
	}

	return s.deleteKeyWithSizeLocked(shard, key, size)
}

func (s *Store) maxMemoryEnabled() bool {
	return s != nil && s.maxMemory.Load() > 0
}

func protectedKeys(keys ...string) map[string]struct{} {
	if len(keys) == 0 {
		return nil
	}

	protected := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		protected[key] = struct{}{}
	}

	return protected
}
