package storage

import (
	"errors"
	"hash/maphash"
	"log/slog"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrSyntax reports malformed storage-related command syntax.
	ErrSyntax = errors.New("syntax error")
	// ErrInvalidExpireTime reports an invalid EX/PX duration.
	ErrInvalidExpireTime = errors.New("invalid expire time in 'SET' command")
	// ErrInvalidStreamID reports that a stream ID could not be parsed.
	ErrInvalidStreamID = errors.New("invalid stream ID specified as stream command argument")
	// ErrStreamIDTooSmall reports that XADD was given a non-monotonic explicit ID.
	ErrStreamIDTooSmall = errors.New("stream ID is equal or smaller than the target stream top item")
	// ErrValueNotInteger reports that a stored string cannot be parsed as a 64-bit integer.
	ErrValueNotInteger = errors.New("value is not an integer or out of range")
	// ErrWrongType reports that a command targeted the wrong logical value type.
	ErrWrongType = errors.New("operation against a key holding the wrong kind of value")
	// ErrNotHyperLogLog reports that a string key does not hold a valid HyperLogLog value.
	ErrNotHyperLogLog = errors.New("key is not a valid HyperLogLog string value")
	// ErrMemoryLimitExceeded reports that a write would exceed the configured maxmemory limit.
	ErrMemoryLimitExceeded = errors.New("command not allowed when used memory > 'maxmemory'")
)

// Store is a thread-safe in-memory key/value store.
type Store struct {
	shards                   []Shard
	seed                     maphash.Seed
	waiters                  *listWaiters
	loggerMu                 sync.RWMutex
	logger                   *slog.Logger
	expirationMu             sync.Mutex
	expirationListener       func(keys []string)
	pendingExpired           []string
	usedMemory               atomic.Int64
	keyKindCounts            [keyStatsKindCount]atomic.Int64
	maxMemory                atomic.Int64
	memoryEvictionSampleSize int
	// nextExpiry is a lower bound on the earliest TTL deadline in the store, kept
	// only while maxmemory is enabled. No key can have expired before it, so an
	// accounted write can skip the sweep-and-recount until it passes. Zero, the
	// initial value, means unknown and forces one recount, which sets it exactly.
	nextExpiry atomic.Int64
}

// NewStore constructs an empty Store.
func NewStore() *Store {
	return &Store{shards: newShards(defaultShardCount), seed: maphash.MakeSeed(), waiters: newListWaiters()}
}

// SetLogger configures optional structured logging for background store operations.
func (s *Store) SetLogger(logger *slog.Logger) {
	if s == nil {
		return
	}

	s.loggerMu.Lock()
	defer s.loggerMu.Unlock()
	s.logger = logger
}

// SetExpirationListener registers the sink for keys the store removed because
// their TTL had passed.
//
// Expiry is the one keyspace mutation the store performs on its own initiative
// rather than on a caller's instruction, which makes it the one a caller cannot
// otherwise see. A caller that replicates and persists mutations has to see
// it. Two paths sweep for it: the background eviction loop, and the keyspace
// recalculation every accounted write runs. Both report here.
//
// A key dropped because the very operation reading it found it expired is not
// reported: passive expiry is a conclusion any server holding the key reaches
// from the TTL it already has. The listener runs with no shard lock held, on
// whichever goroutine noticed the expiry.
func (s *Store) SetExpirationListener(listener func(keys []string)) {
	if s == nil {
		return
	}

	s.expirationMu.Lock()
	defer s.expirationMu.Unlock()
	s.expirationListener = listener
}

// noteExpiredKeysLocked queues keys removed for expiry while shard locks are
// held, for publishExpiredKeys to hand on once they are released.
func (s *Store) noteExpiredKeysLocked(keys []string) {
	if s == nil || len(keys) == 0 {
		return
	}

	s.expirationMu.Lock()
	defer s.expirationMu.Unlock()
	s.pendingExpired = append(s.pendingExpired, keys...)
}

// publishExpiredKeys hands every queued expiry to the listener. Callers must
// hold no shard lock: the listener reaches replication and durability sinks,
// which must never be entered under a store lock. It always drains the queue,
// so an unconfigured listener cannot let it grow.
func (s *Store) publishExpiredKeys() {
	if s == nil {
		return
	}

	s.expirationMu.Lock()
	pending := s.pendingExpired
	s.pendingExpired = nil
	listener := s.expirationListener
	s.expirationMu.Unlock()

	if len(pending) == 0 || listener == nil {
		return
	}

	listener(pending)
}

// Set stores a byte slice under the provided key. When maxmemory is configured
// it first frees space and reports the keys evicted to make room; otherwise it
// evicts nothing.
func (s *Store) Set(key string, value []byte, expiresAt int64) ([]string, error) {
	_, evicted, err := writeKey(s, key, func(w keyWrite) (struct{}, []string, error) {
		evicted, err := w.commit(newStringValue(value, expiresAt))
		return struct{}{}, evicted, err
	})
	return evicted, err
}

// Get fetches a value from the store, passively evicting it if it has expired.
func (s *Store) Get(key string) ([]byte, bool, error) {
	var copied []byte
	ok, err := s.withStringValue(key, func(data []byte) error {
		copied = cloneBytes(data)
		return nil
	})
	if !ok || err != nil {
		return nil, ok, err
	}

	return copied, true, nil
}

// withStringValue runs fn on the stored string payload for key while holding
// the shard read lock, handling passive expiry and access tracking. It
// reports whether a live value was found; fn must not retain data beyond the
// call.
func (s *Store) withStringValue(key string, fn func(data []byte) error) (bool, error) {
	_, found, err := readKey(s, key, func(value *ValueObject) (struct{}, error) {
		data, err := value.StringValue()
		if err != nil {
			return struct{}{}, err
		}
		return struct{}{}, fn(data)
	})
	return found, err
}

// Delete removes a key from the store.
func (s *Store) Delete(key string) bool {
	shard := s.shardForKey(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	value, ok := shard.data[key]
	if ok && isExpired(value, time.Now().UnixMilli()) {
		s.deleteKeyLocked(shard, key)
		return false
	}

	s.deleteKeyLocked(shard, key)
	return ok
}

// DeleteMany removes the supplied keys and returns the keys actually removed.
// Expired keys are treated as absent. The returned key order is unspecified.
func (s *Store) DeleteMany(keys []string) []string {
	if len(keys) == 0 {
		return nil
	}
	if len(keys) == 1 {
		if s.Delete(keys[0]) {
			return []string{keys[0]}
		}
		return nil
	}

	groups := s.groupKeysByShard(keys)
	groups.lock(s)
	defer groups.unlock(s)

	now := time.Now().UnixMilli()
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
			if isExpired(value, now) {
				s.deleteKeyLocked(shard, key)
				continue
			}

			s.deleteKeyLocked(shard, key)
			removed = append(removed, key)
		}
	}

	return removed
}

// Increment atomically increments the string value stored at key by one. When
// maxmemory is configured it first frees space and reports the keys evicted to
// make room; otherwise it evicts nothing.
func (s *Store) Increment(key string) (int64, []string, error) {
	return writeKey(s, key, func(w keyWrite) (int64, []string, error) {
		if w.current == nil {
			evicted, err := w.commit(newOwnedStringValue([]byte("1"), 0))
			if err != nil {
				return 0, nil, err
			}
			return 1, evicted, nil
		}

		currentValue, err := w.current.StringValue()
		if err != nil {
			return 0, nil, err
		}
		parsed, err := strconv.ParseInt(string(currentValue), 10, 64)
		if err != nil || parsed == math.MaxInt64 {
			return 0, nil, ErrValueNotInteger
		}
		parsed++
		next := []byte(strconv.FormatInt(parsed, 10))

		if !w.accounting {
			w.current.String = next
			w.current.touch(w.now)
			return parsed, nil, nil
		}

		// Size the write against the pre-write value, and leave the counter
		// untouched if it turns out to breach maxmemory, by committing a
		// replacement rather than editing the string still stored under the key.
		newValue := newOwnedStringValue(next, w.current.ExpiresAt)
		newValue.touch(w.now)
		evicted, err := w.commit(newValue)
		if err != nil {
			return 0, nil, err
		}
		return parsed, evicted, nil
	})
}

// LeftPush prepends one or more values to the list stored at key and returns
// the new length. When maxmemory is configured it first frees space and reports
// the keys evicted to make room; otherwise it evicts nothing.
func (s *Store) LeftPush(key string, values [][]byte) (int64, []string, error) {
	return s.pushList(key, values, true)
}

// RightPush appends one or more values to the list stored at key and returns
// the new length. When maxmemory is configured it first frees space and reports
// the keys evicted to make room; otherwise it evicts nothing.
func (s *Store) RightPush(key string, values [][]byte) (int64, []string, error) {
	return s.pushList(key, values, false)
}

// LeftPop removes and returns the left-most value from the list stored at key.
func (s *Store) LeftPop(key string) ([]byte, bool, error) {
	shard := s.shardForKey(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	value, ok := shard.data[key]
	if ok && isExpired(value, time.Now().UnixMilli()) {
		s.deleteKeyLocked(shard, key)
		ok = false
	}
	if !ok {
		return nil, false, nil
	}
	list, err := value.ListValue()
	if err != nil {
		return nil, false, err
	}
	if len(list) == 0 {
		s.deleteKeyLocked(shard, key)
		return nil, false, nil
	}
	accounting := s.maxMemoryEnabled()
	var oldSize int64
	if accounting {
		oldSize = s.approximateValueObjectSize(key, value)
	}

	item := list[0]
	list[0] = nil
	list = list[1:]
	value.dropListFront(1)
	value.touch(time.Now().UnixMilli())
	if len(list) == 0 {
		s.deleteKeyWithSizeLocked(shard, key, oldSize)
		return item, true, nil
	}
	if accounting {
		newSize := s.approximateValueObjectSize(key, value)
		s.usedMemory.Add(newSize - oldSize)
	}

	return item, true, nil
}

// RightPop removes and returns the right-most value from the list stored at key.
func (s *Store) RightPop(key string) ([]byte, bool, error) {
	shard := s.shardForKey(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	value, ok := shard.data[key]
	if ok && isExpired(value, time.Now().UnixMilli()) {
		s.deleteKeyLocked(shard, key)
		ok = false
	}
	if !ok {
		return nil, false, nil
	}
	list, err := value.ListValue()
	if err != nil {
		return nil, false, err
	}
	if len(list) == 0 {
		s.deleteKeyLocked(shard, key)
		return nil, false, nil
	}
	accounting := s.maxMemoryEnabled()
	var oldSize int64
	if accounting {
		oldSize = s.approximateValueObjectSize(key, value)
	}

	last := len(list) - 1
	item := list[last]
	list[last] = nil
	list = list[:last]
	value.dropListEnd(1)
	value.touch(time.Now().UnixMilli())
	if len(list) == 0 {
		s.deleteKeyWithSizeLocked(shard, key, oldSize)
		return item, true, nil
	}
	if accounting {
		newSize := s.approximateValueObjectSize(key, value)
		s.usedMemory.Add(newSize - oldSize)
	}

	return item, true, nil
}

// LeftPopN removes and returns up to count left-most values from the list stored at key.
// Returns (nil, false, nil) when the key is missing or expired.
func (s *Store) LeftPopN(key string, count int64) ([][]byte, bool, error) {
	return s.popN(key, count, true)
}

// RightPopN removes and returns up to count right-most values from the list stored at key.
// Returns (nil, false, nil) when the key is missing or expired.
func (s *Store) RightPopN(key string, count int64) ([][]byte, bool, error) {
	return s.popN(key, count, false)
}

func (s *Store) popN(key string, count int64, left bool) ([][]byte, bool, error) {
	if count < 0 {
		return nil, false, ErrSyntax
	}

	shard := s.shardForKey(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	value, ok := shard.data[key]
	if ok && isExpired(value, time.Now().UnixMilli()) {
		s.deleteKeyLocked(shard, key)
		ok = false
	}
	if !ok {
		return nil, false, nil
	}
	list, err := value.ListValue()
	if err != nil {
		return nil, false, err
	}
	if len(list) == 0 {
		s.deleteKeyLocked(shard, key)
		return nil, false, nil
	}
	accounting := s.maxMemoryEnabled()
	var oldSize int64
	if accounting {
		oldSize = s.approximateValueObjectSize(key, value)
	}

	n := int(min(int64(len(list)), count))
	popped := make([][]byte, n)
	if left {
		for i := 0; i < n; i++ {
			popped[i] = list[i]
			list[i] = nil
		}
		list = list[n:]
		value.dropListFront(n)
	} else {
		for i := 0; i < n; i++ {
			src := len(list) - 1 - i
			popped[i] = list[src]
			list[src] = nil
		}
		list = list[:len(list)-n]
		value.dropListEnd(n)
	}

	value.touch(time.Now().UnixMilli())
	if len(list) == 0 {
		s.deleteKeyWithSizeLocked(shard, key, oldSize)
		return popped, true, nil
	}
	if accounting {
		newSize := s.approximateValueObjectSize(key, value)
		s.usedMemory.Add(newSize - oldSize)
	}

	return popped, true, nil
}

// ListRange returns an inclusive range of values from the list stored at key.
func (s *Store) ListRange(key string, start, stop int64) ([][]byte, error) {
	values, found, err := readKey(s, key, func(value *ValueObject) ([][]byte, error) {
		list, err := value.ListValue()
		if err != nil {
			return nil, err
		}

		from, to, ok := normalizeListRange(len(list), start, stop)
		if !ok {
			return [][]byte{}, nil
		}
		return cloneList(list[from : to+1]), nil
	})
	if err != nil {
		return nil, err
	}
	if !found {
		return [][]byte{}, nil
	}
	return values, nil
}

// ZAdd inserts or updates one or more sorted-set members and returns the number
// of newly added members. When maxmemory is configured it first frees space and
// reports the keys evicted to make room; otherwise it evicts nothing.
func (s *Store) ZAdd(key string, entries []ZSetEntry) (int64, []string, error) {
	if len(entries) == 0 {
		return 0, nil, ErrSyntax
	}

	return writeKey(s, key, func(w keyWrite) (int64, []string, error) {
		var (
			newValue *ValueObject
			added    int64
			err      error
		)
		if w.current != nil {
			newValue = w.current
			if w.accounting {
				// Size the write against the pre-write value, and leave the sorted
				// set untouched if it turns out to breach maxmemory, by mutating a
				// copy rather than the value still stored under the key.
				newValue, err = w.current.cloneZSetValue(w.current.ExpiresAt)
				if err != nil {
					return 0, nil, err
				}
			}
			added, err = newValue.zsetAdd(entries)
			if err != nil {
				return 0, nil, err
			}
		} else {
			newValue = newZSetValueForEntries(entries, 0)
			newLen, lenErr := newValue.zsetLen()
			if lenErr != nil {
				return 0, nil, lenErr
			}
			added = int64(newLen)
		}
		newValue.touch(w.now)

		evicted, err := w.commit(newValue)
		if err != nil {
			return 0, nil, err
		}
		return added, evicted, nil
	})
}

// ZScores returns the scores of the requested sorted-set members under a
// single lock acquisition; found[i] reports whether members[i] exists.
func (s *Store) ZScores(key string, members [][]byte) ([]float64, []bool, error) {
	scores := make([]float64, len(members))
	found := make([]bool, len(members))

	_, _, err := readKey(s, key, func(value *ValueObject) (struct{}, error) {
		for i, member := range members {
			score, exists, err := value.zsetScore(member)
			if err != nil {
				return struct{}{}, err
			}
			scores[i] = score
			found[i] = exists
		}
		return struct{}{}, nil
	})
	if err != nil {
		return nil, nil, err
	}

	return scores, found, nil
}

// ZRange returns an inclusive rank range from the sorted set stored at key.
func (s *Store) ZRange(key string, start, stop int64) ([]ZSetRangeEntry, error) {
	entries, found, err := readKey(s, key, func(value *ValueObject) ([]ZSetRangeEntry, error) {
		setLen, err := value.zsetLen()
		if err != nil {
			return nil, err
		}

		from, to, ok := normalizeListRange(setLen, start, stop)
		if !ok {
			return []ZSetRangeEntry{}, nil
		}
		return value.zsetRangeByRank(from, to)
	})
	if err != nil {
		return nil, err
	}
	if !found {
		return []ZSetRangeEntry{}, nil
	}
	return entries, nil
}

// ZRangeByScores returns the members of the sorted set stored at key whose
// scores fall within the given score ranges, concatenated in range order.
// The ranges must be disjoint and ascending so the result stays ordered by
// score then member. All ranges are scanned under a single lock acquisition,
// so the result reflects one consistent snapshot of the set.
func (s *Store) ZRangeByScores(key string, scoreRanges ...ScoreRange) ([]ZSetRangeEntry, error) {
	entries, found, err := readKey(s, key, func(value *ValueObject) ([]ZSetRangeEntry, error) {
		var entries []ZSetRangeEntry
		for _, scoreRange := range scoreRanges {
			ranged, err := value.zsetRangeByScore(scoreRange)
			if err != nil {
				return nil, err
			}
			entries = append(entries, ranged...)
		}
		return entries, nil
	})
	if err != nil {
		return nil, err
	}
	if !found {
		return []ZSetRangeEntry{}, nil
	}
	return entries, nil
}

// XAdd appends a new stream entry to the stream stored at key and returns its
// ID. When maxmemory is configured it first frees space and reports the keys
// evicted to make room; otherwise it evicts nothing.
func (s *Store) XAdd(key, rawID string, values [][]byte) (string, []string, error) {
	if len(values) == 0 || len(values)%2 != 0 {
		return "", nil, ErrSyntax
	}

	return writeKey(s, key, func(w keyWrite) (string, []string, error) {
		var (
			stream    *StreamValue
			expiresAt int64
		)
		if w.current != nil {
			existing, err := w.current.StreamValue()
			if err != nil {
				return "", nil, err
			}
			expiresAt = w.current.ExpiresAt
			stream = existing
			if w.accounting {
				// Size the write against the pre-write stream, and leave it
				// untouched if it turns out to breach maxmemory, by appending to a
				// copy rather than the stream still stored under the key.
				stream = cloneStreamValue(existing)
			}
		} else {
			stream = newStream()
		}

		id, err := stream.add(rawID, values, w.now)
		if err != nil {
			return "", nil, err
		}

		newValue := newStreamValue(stream, expiresAt)
		newValue.touch(w.now)

		evicted, err := w.commit(newValue)
		if err != nil {
			return "", nil, err
		}
		return id, evicted, nil
	})
}

// XRead returns stream entries whose IDs are greater than the supplied ID.
func (s *Store) XRead(key, rawID string) ([]StreamEntry, error) {
	entries, found, err := readKey(s, key, func(value *ValueObject) ([]StreamEntry, error) {
		stream, err := value.StreamValue()
		if err != nil {
			return nil, err
		}
		return stream.entriesAfter(rawID)
	})
	if err != nil {
		return nil, err
	}
	if !found {
		return []StreamEntry{}, nil
	}
	return entries, nil
}

// SubscribeListPush registers a waiter that is notified when a push occurs for key.
func (s *Store) SubscribeListPush(key string) chan struct{} {
	return s.waiters.subscribe(key)
}

// UnsubscribeListPush removes a previously registered list push waiter.
func (s *Store) UnsubscribeListPush(key string, ch chan struct{}) {
	s.waiters.unsubscribe(key, ch)
}

// Len returns the current number of stored keys.
func (s *Store) Len() int {
	total := 0
	s.readLockAllShards()
	defer s.readUnlockAllShards()

	for i := range s.shards {
		total += len(s.shards[i].data)
	}

	return total
}

// SnapshotStrings returns defensive copies of the currently supported
// shutdown-persistence scope: non-expired DB 0 string keys only.
func (s *Store) SnapshotStrings() ([]StringSnapshotEntry, StringSnapshotStats) {
	now := time.Now().UnixMilli()

	s.readLockAllShards()
	defer s.readUnlockAllShards()

	stats := StringSnapshotStats{}
	entries := make([]StringSnapshotEntry, 0, s.totalKeyCountLocked())
	valueArena := make([]byte, 0)
	for i := range s.shards {
		shard := &s.shards[i]
		stats.TotalKeys += len(shard.data)
		for key, value := range shard.data {
			if isExpired(value, now) {
				stats.SkippedExpiredKeys++
				continue
			}
			if value.Kind != ValueKindString {
				stats.SkippedUnsupportedKeys++
				continue
			}

			var clonedValue []byte
			valueArena, clonedValue = appendClonedBytesArena(valueArena, value.String)

			entries = append(entries, StringSnapshotEntry{
				Key:       key,
				Value:     clonedValue,
				ExpiresAt: value.ExpiresAt,
			})
			stats.ExportedKeys++
		}
	}

	return entries, stats
}

// SnapshotAll returns defensive copies of every currently supported non-expired value.
func (s *Store) SnapshotAll() ([]SnapshotEntry, SnapshotStats) {
	s.readLockAllShards()
	defer s.readUnlockAllShards()

	return s.snapshotAllLocked(time.Now().UnixMilli())
}

// SnapshotAllWithWriteBarrier snapshots the full keyspace while holding the store's
// write locks, invoking barrier immediately before write traffic resumes.
func (s *Store) SnapshotAllWithWriteBarrier(barrier func()) ([]SnapshotEntry, SnapshotStats) {
	s.writeLockAllShards()
	defer s.writeUnlockAllShards()

	entries, stats := s.snapshotAllLocked(time.Now().UnixMilli())
	if barrier != nil {
		barrier()
	}

	return entries, stats
}

// ReplaceWith swaps the store contents with a snapshot from another store.
//
// The replacement is applied only after the source snapshot has already been
// materialized, which makes it suitable for FULLRESYNC-style state replacement.
func (s *Store) ReplaceWith(other *Store) {
	if s == nil || other == nil {
		return
	}

	other.readLockAllShards()
	replacement := newShards(len(s.shards))
	for i := range other.shards {
		for key, value := range other.shards[i].data {
			replacement[s.shardIndex(key)].data[key] = value
		}
	}
	other.readUnlockAllShards()

	s.writeLockAllShards()
	for i := range s.shards {
		s.shards[i].data = replacement[i].data
	}
	s.recalculateKeyStatsLocked()
	if s.maxMemoryEnabled() {
		s.recalculateUsedMemoryLocked(time.Now().UnixMilli())
	}
	s.writeUnlockAllShards()
	s.publishExpiredKeys()
}

func (s *Store) snapshotAllLocked(now int64) ([]SnapshotEntry, SnapshotStats) {
	stats := SnapshotStats{}
	entries := make([]SnapshotEntry, 0, s.totalKeyCountLocked())
	valueArena := make([]byte, 0)
	for i := range s.shards {
		shard := &s.shards[i]
		stats.TotalKeys += len(shard.data)
		for key, value := range shard.data {
			if isExpired(value, now) {
				stats.SkippedExpiredKeys++
				continue
			}

			entry := SnapshotEntry{Key: key, Kind: value.Kind, ExpiresAt: value.ExpiresAt}
			switch value.Kind {
			case ValueKindString:
				valueArena, entry.String = appendClonedBytesArena(valueArena, value.String)
			case ValueKindList:
				list := value.liveList()
				entry.List = make([][]byte, len(list))
				for j, item := range list {
					valueArena, entry.List[j] = appendClonedBytesArena(valueArena, item)
				}
			case ValueKindZSet:
				setLen, err := value.zsetLen()
				if err != nil {
					continue
				}
				if setLen > 0 {
					entry.ZSet, err = value.zsetRangeByRank(0, setLen-1)
					if err != nil {
						continue
					}
				}
			case ValueKindStream:
				if value.Stream != nil {
					entry.Stream = make([]StreamEntry, 0, len(value.Stream.entries))
					for _, record := range value.Stream.entries {
						clonedValues := make([][]byte, len(record.values))
						for j, item := range record.values {
							valueArena, clonedValues[j] = appendClonedBytesArena(valueArena, item)
						}
						entry.Stream = append(entry.Stream, StreamEntry{ID: record.idText, Values: clonedValues})
					}
				}
			case ValueKindHash:
				hashEntries, err := value.hashEntries()
				if err != nil {
					continue
				}
				entry.Hash = make([]HashFieldValue, 0, len(hashEntries))
				for _, hashEntry := range hashEntries {
					var clonedValue []byte
					valueArena, clonedValue = appendClonedBytesArena(valueArena, hashEntry.Value)
					entry.Hash = append(entry.Hash, HashFieldValue{Field: hashEntry.Field, Value: clonedValue})
				}
			case ValueKindSet:
				if value.SetEncoding == ValueEncodingCompact {
					setMembers, err := value.setMembers()
					if err != nil {
						continue
					}
					entry.Set = make([][]byte, len(setMembers))
					for j, member := range setMembers {
						valueArena, entry.Set[j] = appendClonedBytesArena(valueArena, member)
					}
					break
				}
				if value.Set == nil {
					continue
				}
				entry.Set = make([][]byte, 0, len(value.Set))
				for member := range value.Set {
					var clonedMember []byte
					valueArena, clonedMember = appendClonedStringBytesArena(valueArena, member)
					entry.Set = append(entry.Set, clonedMember)
				}
			}

			entries = append(entries, entry)
			stats.ExportedKeys++
		}
	}

	return entries, stats
}

func (s *Store) totalKeyCountLocked() int {
	total := 0
	for i := range s.shards {
		total += len(s.shards[i].data)
	}
	return total
}

func (s *Store) logDebug(msg string, args ...any) {
	if s == nil {
		return
	}

	s.loggerMu.RLock()
	logger := s.logger
	s.loggerMu.RUnlock()
	if logger == nil {
		return
	}

	logger.Debug(msg, args...)
}

func (s *Store) logError(msg string, args ...any) {
	if s == nil {
		return
	}

	s.loggerMu.RLock()
	logger := s.logger
	s.loggerMu.RUnlock()
	if logger == nil {
		return
	}

	logger.Error(msg, args...)
}

func (s *Store) snapshotKeys(limit int) []string {
	if limit <= 0 {
		return nil
	}

	keys := make([]string, 0, limit)
	start := int(time.Now().UnixNano() % int64(len(s.shards)))
	for offset := 0; offset < len(s.shards) && len(keys) < limit; offset++ {
		index := (start + offset) % len(s.shards)
		shard := &s.shards[index]
		shard.mu.RLock()
		for key := range shard.data {
			keys = append(keys, key)
			if len(keys) == limit {
				break
			}
		}
		shard.mu.RUnlock()
	}

	return keys
}

type shardKeyGroups struct {
	counts  []int
	offsets []int
	keys    []string
}

func (s *Store) groupKeysByShard(keys []string) shardKeyGroups {
	shardCount := len(s.shards)
	counts := make([]int, shardCount)
	for _, key := range keys {
		counts[s.shardIndex(key)]++
	}

	offsets := make([]int, shardCount)
	total := 0
	for shardID, count := range counts {
		offsets[shardID] = total
		total += count
	}

	groupedKeys := make([]string, len(keys))
	next := make([]int, shardCount)
	copy(next, offsets)
	for _, key := range keys {
		shardID := s.shardIndex(key)
		groupedKeys[next[shardID]] = key
		next[shardID]++
	}

	return shardKeyGroups{counts: counts, offsets: offsets, keys: groupedKeys}
}

func (g shardKeyGroups) lock(s *Store) {
	for shardID, count := range g.counts {
		if count == 0 {
			continue
		}
		s.shards[shardID].mu.Lock()
	}
}

func (g shardKeyGroups) unlock(s *Store) {
	for shardID := len(g.counts) - 1; shardID >= 0; shardID-- {
		if g.counts[shardID] == 0 {
			continue
		}
		s.shards[shardID].mu.Unlock()
	}
}

func (g shardKeyGroups) keysForShard(shardID int) []string {
	count := g.counts[shardID]
	start := g.offsets[shardID]
	return g.keys[start : start+count]
}

func cloneBytes(src []byte) []byte {
	if src == nil {
		return nil
	}

	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

func appendClonedBytesArena(arena []byte, src []byte) ([]byte, []byte) {
	if src == nil {
		return arena, nil
	}

	start := len(arena)
	arena = append(arena, src...)
	cloned := arena[start:len(arena):len(arena)]
	return arena, cloned
}

func appendClonedStringBytesArena(arena []byte, src string) ([]byte, []byte) {
	start := len(arena)
	arena = append(arena, src...)
	cloned := arena[start:len(arena):len(arena)]
	return arena, cloned
}

func cloneList(items [][]byte) [][]byte {
	if items == nil {
		return nil
	}

	cloned := make([][]byte, len(items))
	for i, item := range items {
		cloned[i] = cloneBytes(item)
	}

	return cloned
}

func (s *Store) pushList(key string, values [][]byte, left bool) (int64, []string, error) {
	if len(values) == 0 {
		return 0, nil, ErrSyntax
	}

	length, evicted, err := writeKey(s, key, func(w keyWrite) (int64, []string, error) {
		var (
			list      [][]byte // the values of the list
			full      [][]byte // the array they live in, ListHead dead slots ahead of them
			head      int
			expiresAt int64
		)
		if w.current != nil {
			currentList, err := w.current.ListValue()
			if err != nil {
				return 0, nil, err
			}
			expiresAt = w.current.ExpiresAt
			list = currentList
			if w.accounting {
				// Size the write against the pre-write list, and leave it untouched
				// if it turns out to breach maxmemory, by appending to a copy rather
				// than to the slice still stored under the key. The copy shares no
				// array with the stored list, so there is nothing to reuse.
				list = append([][]byte(nil), currentList...)
			} else {
				full, head = w.current.List, int(w.current.ListHead)
			}
		}

		additions := cloneList(values)
		if left {
			full, head = prependList(full, head, list, additions)
		} else {
			grown := append(list, additions...)
			if full != nil && len(list) > 0 && &grown[0] == &list[0] {
				// It fitted: the list still lives in the same array, and so do the
				// dead slots ahead of it.
				full = full[:head+len(grown)]
			} else {
				full, head = grown, 0
			}
		}

		value := newListValue(full, expiresAt)
		value.ListHead = int32(head)
		evicted, err := w.commit(value)
		if err != nil {
			return 0, nil, err
		}
		return int64(len(full) - head), evicted, nil
	})
	if err != nil {
		return 0, nil, err
	}

	// Notify only once every shard lock is released. A shard lock must never be
	// held while taking waiters.mu, so that a future path taking those two locks
	// in the other order cannot deadlock against this one.
	s.waiters.notifyOne(key)
	return length, evicted, nil
}

// prependList puts additions in front of list, the last addition first, and
// returns the array the result lives in and how many dead slots precede it. full
// and head describe where list currently lives (see ValueObject.liveList); full
// is nil when there is nothing to reuse. When the dead slots ahead of list are
// enough it fills them and copies nothing. Otherwise it moves the list into a new
// array with as much headroom as the list has values, so pushing to the front
// copies the list once per doubling instead of once per push.
func prependList(full [][]byte, head int, list, additions [][]byte) ([][]byte, int) {
	k := len(additions)
	if head >= k {
		start := head - k
		for i, item := range additions {
			full[start+k-1-i] = item
		}
		return full, start
	}

	headroom := min(len(list), math.MaxInt32-k)
	grown := make([][]byte, headroom+k+len(list))
	for i, item := range additions {
		grown[headroom+k-1-i] = item
	}
	copy(grown[headroom+k:], list)
	return grown, headroom
}

func normalizeListRange(length int, start, stop int64) (int, int, bool) {
	if length == 0 {
		return 0, 0, false
	}

	from := normalizeListIndex(length, start)
	to := normalizeListIndex(length, stop)
	if from < 0 {
		from = 0
	}
	if to < 0 {
		return 0, 0, false
	}
	if from >= length {
		return 0, 0, false
	}
	if to >= length {
		to = length - 1
	}
	if from > to {
		return 0, 0, false
	}

	return from, to, true
}

// normalizeListIndex resolves a possibly-negative Redis list index against length.
// Out-of-range inputs are saturated to -1 (below the list) or length (past its end)
// so the arithmetic stays in int64 and never truncates on narrow-int platforms.
func normalizeListIndex(length int, index int64) int {
	if index < 0 {
		index += int64(length)
	}
	if index < 0 {
		return -1
	}
	if index >= int64(length) {
		return length
	}

	return int(index)
}

func isExpired(value *ValueObject, now int64) bool {
	return value != nil && value.ExpiresAt > 0 && now > value.ExpiresAt
}

// deadlineAfter returns the Unix-millis deadline value units of millisPerUnit
// after nowMillis, or ErrInvalidExpireTime when that does not fit in an int64.
// It works in integer milliseconds because time.Duration, at nanosecond
// resolution, wraps past about 292 years and turned a huge expiry into a
// deadline in the past (or a negative one, which reads as "no expiry").
func deadlineAfter(nowMillis, value, millisPerUnit int64) (int64, error) {
	if value > (math.MaxInt64-nowMillis)/millisPerUnit {
		return 0, ErrInvalidExpireTime
	}
	return nowMillis + value*millisPerUnit, nil
}

// ParseExpiryMillis parses Redis-style EX/PX/PXAT arguments into a Unix-millis deadline.
func ParseExpiryMillis(args [][]byte) (int64, error) {
	if len(args) == 0 {
		return 0, nil
	}
	if len(args) != 2 {
		return 0, ErrSyntax
	}

	value, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil || value <= 0 {
		return 0, ErrInvalidExpireTime
	}

	nowMillis := time.Now().UnixMilli()
	switch strings.ToUpper(string(args[0])) {
	case "EX":
		return deadlineAfter(nowMillis, value, 1000)
	case "PX":
		return deadlineAfter(nowMillis, value, 1)
	case "PXAT":
		// Absolute expiry in Unix milliseconds. Used both by clients and by the
		// frame the executor propagates/persists for SET, so replicas and AOF
		// replay anchor the TTL to the master's clock instead of restarting it.
		return value, nil
	default:
		return 0, ErrSyntax
	}
}
