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
	evictionGuard            func() (release func())
	usedMemory               atomic.Int64
	keyKindCounts            [keyStatsKindCount]atomic.Int64
	maxMemory                atomic.Int64
	memoryEvictionSampleSize int
	// nextExpiry is a lower bound on the earliest TTL deadline in the store, kept
	// only while maxmemory is enabled. No key can have expired before it, so an
	// accounted write can skip the sweep-and-recount until it passes. Zero, the
	// initial value, means unknown and forces one recount, which sets it exactly.
	nextExpiry atomic.Int64
	// clock returns the time, in Unix milliseconds, that now reads. Nil, as
	// NewStore leaves it, means the system clock. Nothing guards the field, so a
	// test that sets a manual clock sets it before the store is shared.
	clock func() int64
}

// now takes a clock reading: the time in Unix milliseconds that decides whether
// a TTL deadline has passed and that stamps a key's access. Every store
// operation takes one and judges everything it touches by it, with one
// exception: a read that finds its key expired takes a second reading under the
// write lock before dropping the key, because the key may have been renewed
// since the first (see dropIfStillExpired).
//
// Within the store, two things read the system clock directly because they pace
// work rather than decide what a key holds: the time budget of a background
// eviction pass, and the shard a key sample starts from. The value constructors
// read no clock: they leave a new value unstamped, and the write that stores it
// stamps it with its own reading. Code outside the store takes its own
// reading of the system clock to judge TTLs: SET for its relative EX or PX
// (ParseExpiryMillis is handed it), RDB loading and snapshot building, and the
// AOF rewrite.
func (s *Store) now() int64 {
	if s.clock != nil {
		return s.clock()
	}
	return time.Now().UnixMilli()
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

// SetEvictionGuard registers a function the background eviction loop calls before
// each sample and whose result it calls once the sample's removals have been
// handed to the expiration listener. It lets the owner of the listener order those
// removals against the writes it is also logging: a key expiring and being set
// again must reach the log in the order they happened.
func (s *Store) SetEvictionGuard(guard func() (release func())) {
	if s == nil {
		return
	}

	s.expirationMu.Lock()
	defer s.expirationMu.Unlock()
	s.evictionGuard = guard
}

func (s *Store) acquireEvictionGuard() func() {
	s.expirationMu.Lock()
	guard := s.evictionGuard
	s.expirationMu.Unlock()
	if guard == nil {
		return func() {}
	}
	return guard()
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
	return replaceKey(s, key, newStringValue(value, expiresAt))
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

// Delete removes a key from the store and reports whether it held a live value.
func (s *Store) Delete(key string) bool {
	// A mutation that accepts every kind and never fails cannot return an error.
	deleted, _, _ := mutateKey(s, key, anyKind, shrinks, func(cur *ValueObject, _ int64) (*ValueObject, bool, error) {
		return nil, cur != nil, nil
	})
	return deleted
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
	return mutateString(s, key, func(w stringWrite) (int64, []string, error) {
		parsed := int64(0)
		if current, ok := w.Current(); ok {
			n, err := strconv.ParseInt(string(current), 10, 64)
			if err != nil || n == math.MaxInt64 {
				return 0, nil, ErrValueNotInteger
			}
			parsed = n
		}
		parsed++

		var digits [20]byte // the longest int64, with its sign
		next := strconv.AppendInt(digits[:0], parsed, 10)
		payload, evicted, err := w.Payload(len(next))
		if err != nil {
			return 0, nil, err
		}
		copy(payload, next)
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
	return s.popOne(key, true)
}

// RightPop removes and returns the right-most value from the list stored at key.
func (s *Store) RightPop(key string) ([]byte, bool, error) {
	return s.popOne(key, false)
}

// popOne removes one value from the left or right end of the list stored at key.
// A pop that leaves values behind wakes the next waiting client, once the
// shard lock is released.
func (s *Store) popOne(key string, left bool) ([]byte, bool, error) {
	// The mutation reports only whether it found a list. The popped value and
	// the count left come back through variables it sets: a struct result
	// carrying them was copied on every return and made a pop over 10% slower.
	var item []byte
	remaining := 0
	ok, _, err := mutateKey(s, key, ValueKindList, shrinks, func(cur *ValueObject, _ int64) (*ValueObject, bool, error) {
		if cur == nil || len(cur.liveList()) == 0 {
			return nil, false, nil
		}
		if left {
			item, remaining = cur.popListFront()
		} else {
			item, remaining = cur.popListBack()
		}
		return cur, true, nil
	})
	if remaining > 0 {
		s.wakeNextListWaiter(key)
	}
	return item, ok, err
}

// LeftPopN removes and returns up to count left-most values from the list stored at key.
// Returns (nil, false, nil) when the key is missing or expired.
func (s *Store) LeftPopN(key string, count int) ([][]byte, bool, error) {
	return s.popN(key, count, true)
}

// RightPopN removes and returns up to count right-most values from the list stored at key.
// Returns (nil, false, nil) when the key is missing or expired.
func (s *Store) RightPopN(key string, count int) ([][]byte, bool, error) {
	return s.popN(key, count, false)
}

// popN removes up to count values from the left or right end of the list stored
// at key. A pop that removes values and leaves values behind wakes the next
// waiting client, once the shard lock is released.
func (s *Store) popN(key string, count int, left bool) ([][]byte, bool, error) {
	if count < 0 {
		return nil, false, ErrSyntax
	}

	// As in popOne, the values and the count left come back through variables.
	var values [][]byte
	remaining := 0
	ok, _, err := mutateKey(s, key, ValueKindList, shrinks, func(cur *ValueObject, _ int64) (*ValueObject, bool, error) {
		if cur == nil || len(cur.liveList()) == 0 {
			return nil, false, nil
		}
		values, remaining = cur.popListValues(count, left)
		return cur, true, nil
	})
	if len(values) > 0 && remaining > 0 {
		s.wakeNextListWaiter(key)
	}
	return values, ok, err
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

	return mutateKey(s, key, ValueKindZSet, mayGrow, func(cur *ValueObject, _ int64) (*ValueObject, int64, error) {
		if cur == nil {
			created := newZSetValueForEntries(entries, 0)
			n, err := created.zsetLen()
			return created, int64(n), err
		}
		added, err := cur.zsetAdd(entries)
		return cur, added, err
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

	return mutateKey(s, key, ValueKindStream, mayGrow, func(cur *ValueObject, now int64) (*ValueObject, string, error) {
		if cur == nil {
			cur = newStreamValue(newStream(), 0)
		}
		stream, err := cur.StreamValue()
		if err != nil {
			return nil, "", err
		}
		id, err := stream.add(rawID, values, now)
		return cur, id, err
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

// SubscribeListPush registers a waiter for an element of the list at key. It is
// signaled when its turn comes: by a push, or by a pop that leaves elements
// behind (wakeNextListWaiter).
func (s *Store) SubscribeListPush(key string) chan struct{} {
	return s.waiters.subscribe(key)
}

// UnsubscribeListPush removes a waiter registered with SubscribeListPush. A
// waiter that was signaled but whose wake-up nobody received passes its turn to
// the next waiter, so a client that stops waiting after its turn came (it has
// popped already, or has gone) leaves no other client blocked while the list
// has an element. Call it only once every shard lock is released, as
// wakeNextListWaiter.
func (s *Store) UnsubscribeListPush(key string, ch chan struct{}) {
	s.waiters.unsubscribe(key, ch)
}

// PassListPushWake passes a turn that a waiter received but will not take, as
// when its client has gone, to the next waiter for key. Call it only once every
// shard lock is released, as wakeNextListWaiter.
func (s *Store) PassListPushWake(key string) {
	s.wakeNextListWaiter(key)
}

// wakeNextListWaiter wakes the client that has waited longest for an element of
// the list at key, if any client is waiting. A push calls it, and so does a pop
// that leaves elements behind. The woken client's own pop then wakes the next,
// so a push of n elements serves up to n waiting clients, longest-waiting first,
// one after the other rather than all at once: woken together, they would race,
// and a later waiter could take the first element.
//
// Call it only once every shard lock is released. A shard lock must never be
// held while taking waiters.mu, so that a future path taking those two locks in
// the other order cannot deadlock against this one.
func (s *Store) wakeNextListWaiter(key string) {
	s.waiters.notifyOne(key)
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
	now := s.now()

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

	return s.snapshotAllLocked(s.now())
}

// SnapshotAllWithWriteBarrier snapshots the full keyspace while holding the store's
// write locks, invoking barrier immediately before write traffic resumes.
func (s *Store) SnapshotAllWithWriteBarrier(barrier func()) ([]SnapshotEntry, SnapshotStats) {
	s.writeLockAllShards()
	defer s.writeUnlockAllShards()

	entries, stats := s.snapshotAllLocked(s.now())
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
		s.recalculateUsedMemoryLocked(s.now())
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

const (
	// snapshotArenaMinChunk and snapshotArenaMaxChunk bound the chunks a
	// snapshot's values are copied into. Chunks start small, so a small snapshot
	// does not pay for a large allocation, and double up to the maximum. A value
	// larger than the next chunk gets a chunk of its own.
	snapshotArenaMinChunk = 4 << 10
	snapshotArenaMaxChunk = 1 << 20
)

// growArena returns an arena with room for n more bytes. When the current one is
// full it starts a new chunk instead of growing it: appending would copy every
// byte already handed out into a larger array, while the slices already returned
// keep pointing into, and so keeping alive, the old one.
func growArena(arena []byte, n int) []byte {
	if n <= cap(arena)-len(arena) {
		return arena
	}
	chunk := min(max(2*cap(arena), snapshotArenaMinChunk), snapshotArenaMaxChunk)
	return make([]byte, 0, max(chunk, n))
}

func appendClonedBytesArena(arena []byte, src []byte) ([]byte, []byte) {
	if src == nil {
		return arena, nil
	}

	arena = growArena(arena, len(src))
	start := len(arena)
	arena = append(arena, src...)
	cloned := arena[start:len(arena):len(arena)]
	return arena, cloned
}

func appendClonedStringBytesArena(arena []byte, src string) ([]byte, []byte) {
	arena = growArena(arena, len(src))
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

	length, evicted, err := mutateKey(s, key, ValueKindList, mayGrow, func(cur *ValueObject, _ int64) (*ValueObject, int64, error) {
		if cur == nil {
			cur = newListValue(nil, 0)
		}
		full, head := cur.List, int(cur.ListHead) // the array the list lives in, and the dead slots ahead of it
		list := full[head:]
		additions := cloneList(values)
		if left {
			full, head = prependList(full, head, list, additions)
		} else {
			grown := append(list, additions...)
			if len(list) > 0 && &grown[0] == &list[0] {
				// It fitted: the list still lives in the same array, and so do the
				// dead slots ahead of it.
				full = full[:head+len(grown)]
			} else {
				full, head = grown, 0
			}
		}
		cur.List, cur.ListHead = full, int32(head)
		return cur, int64(len(full) - head), nil
	})
	if err != nil {
		return 0, nil, err
	}

	s.wakeNextListWaiter(key)
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

// ParseExpiryMillis parses Redis-style EX/PX/PXAT arguments into a Unix-millis
// deadline. A relative EX or PX counts from nowMillis, the caller's clock
// reading in Unix milliseconds.
func ParseExpiryMillis(args [][]byte, nowMillis int64) (int64, error) {
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
