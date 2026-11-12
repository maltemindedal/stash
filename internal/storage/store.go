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
	shards           []Shard
	seed             maphash.Seed
	waiters          *listWaiters
	loggerMu         sync.RWMutex
	logger           *slog.Logger
	backgroundMu     sync.Mutex
	backgroundRunner func(work func(changes Changes))
	// removed is the keys the holder of every shard write lock has removed on
	// its own initiative during that hold. Those locks guard it, and the holder
	// empties it as it releases them (writeUnlockAllShardsAndReport), so one
	// call never hands on what another removed.
	removed                  removals
	usedMemory               atomic.Int64
	keyKindCounts            [kindCount]atomic.Int64
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
// (ParseExpiryMillis is handed it), and RDB loading and snapshot building. An
// AOF rewrite reads none: the keyspace image it is generated from was judged by
// the store's reading, and its deadlines are written as they are.
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

// SetBackgroundRunner registers run, which the store calls with each piece of
// work it does on its own initiative: one sample of the active expiry loop.
//
// A TTL eviction by that loop is a keyspace change no caller asked for, so no
// request's Changes hears of it, yet whoever replicates and persists writes has
// to. run calls work with the Changes that hears it, and orders work against
// the writes it is also recording: a key expiring and being set again must
// reach the log in the order they happened. Without a runner the store calls
// work with a nil Changes.
func (s *Store) SetBackgroundRunner(run func(work func(changes Changes))) {
	if s == nil {
		return
	}

	s.backgroundMu.Lock()
	defer s.backgroundMu.Unlock()
	s.backgroundRunner = run
}

// runInBackground runs work through the background runner.
func (s *Store) runInBackground(work func(changes Changes)) {
	s.backgroundMu.Lock()
	run := s.backgroundRunner
	s.backgroundMu.Unlock()
	if run == nil {
		work(nil)
		return
	}
	run(work)
}

// removals is the keys one call removed that its caller did not ask it to
// remove.
type removals struct {
	// expired are active TTL evictions: keys a sweep found past their deadline.
	expired []string
	// evicted are memory-pressure evictions: keys removed to make room.
	evicted []string
}

// noteExpiredLocked adds keys a sweep removed to what the caller's hold of
// every shard write lock has removed.
func (s *Store) noteExpiredLocked(keys []string) {
	s.removed.expired = append(s.removed.expired, keys...)
}

// writeUnlockAllShardsAndReport releases every shard write lock and then tells
// changes which keys the hold swept and which it evicted to make room.
func (s *Store) writeUnlockAllShardsAndReport(changes Changes) {
	removed := s.removed
	s.removed = removals{}
	s.writeUnlockAllShards()
	if changes == nil {
		return
	}
	for _, key := range removed.expired {
		changes.KeyChanged(key, Expired)
	}
	for _, key := range removed.evicted {
		changes.KeyChanged(key, MemoryEvicted)
	}
}

// Set stores a byte slice under the provided key. When maxmemory is configured
// it first evicts keys to make room.
func (s *Store) Set(changes Changes, key string, value []byte, expiresAt int64) error {
	return replaceKey(s, changes, key, newStringValue(value, expiresAt))
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
func (s *Store) Delete(changes Changes, key string) bool {
	// A mutation that accepts every kind and never fails cannot return an error.
	deleted, _ := mutateKey(s, changes, key, anyKind, shrinks, func(cur *ValueObject, _ int64) (*ValueObject, bool, error) {
		if cur == nil {
			return nil, false, errUnchanged
		}
		return nil, true, nil
	})
	return deleted
}

// DeleteMany removes the supplied keys and returns the keys actually removed.
// Expired keys are treated as absent. The returned key order is unspecified.
func (s *Store) DeleteMany(changes Changes, keys []string) []string {
	if len(keys) == 0 {
		return nil
	}
	if len(keys) == 1 {
		if s.Delete(changes, keys[0]) {
			return []string{keys[0]}
		}
		return nil
	}

	return deleteKeys(s, changes, keys)
}

// Increment atomically increments the string value stored at key by one. When
// maxmemory is configured it first evicts keys to make room.
func (s *Store) Increment(changes Changes, key string) (int64, error) {
	return mutateString(s, changes, key, func(w stringWrite) (int64, error) {
		parsed := int64(0)
		if current, ok := w.Current(); ok {
			n, err := strconv.ParseInt(string(current), 10, 64)
			if err != nil || n == math.MaxInt64 {
				return 0, ErrValueNotInteger
			}
			parsed = n
		}
		parsed++

		var digits [20]byte // the longest int64, with its sign
		next := strconv.AppendInt(digits[:0], parsed, 10)
		payload, err := w.Payload(len(next))
		if err != nil {
			return 0, err
		}
		copy(payload, next)
		return parsed, nil
	})
}

// LeftPush prepends one or more values to the list stored at key and returns
// the new length. When maxmemory is configured it first evicts keys to make
// room.
func (s *Store) LeftPush(changes Changes, key string, values [][]byte) (int64, error) {
	return s.pushList(changes, key, values, true)
}

// RightPush appends one or more values to the list stored at key and returns
// the new length. When maxmemory is configured it first evicts keys to make
// room.
func (s *Store) RightPush(changes Changes, key string, values [][]byte) (int64, error) {
	return s.pushList(changes, key, values, false)
}

// LeftPop removes and returns the left-most value from the list stored at key.
func (s *Store) LeftPop(changes Changes, key string) ([]byte, bool, error) {
	return s.popOne(changes, key, true)
}

// RightPop removes and returns the right-most value from the list stored at key.
func (s *Store) RightPop(changes Changes, key string) ([]byte, bool, error) {
	return s.popOne(changes, key, false)
}

// popOne removes one value from the left or right end of the list stored at key.
// A pop that leaves values behind wakes the next waiting client, once the
// shard lock is released.
func (s *Store) popOne(changes Changes, key string, left bool) ([]byte, bool, error) {
	// The mutation reports only whether it found a list. The popped value and
	// the count left come back through variables it sets: a struct result
	// carrying them was copied on every return and made a pop over 10% slower.
	var item []byte
	remaining := 0
	ok, err := mutateKey(s, changes, key, kindList, shrinks, func(cur *ValueObject, _ int64) (*ValueObject, bool, error) {
		if cur == nil {
			return nil, false, errUnchanged
		}
		list, err := listOf(cur)
		if err != nil {
			return nil, false, err
		}
		if list.empty() {
			return nil, false, errUnchanged
		}
		if left {
			item, remaining = list.popFront()
		} else {
			item, remaining = list.popBack()
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
func (s *Store) LeftPopN(changes Changes, key string, count int) ([][]byte, bool, error) {
	return s.popN(changes, key, count, true)
}

// RightPopN removes and returns up to count right-most values from the list stored at key.
// Returns (nil, false, nil) when the key is missing or expired.
func (s *Store) RightPopN(changes Changes, key string, count int) ([][]byte, bool, error) {
	return s.popN(changes, key, count, false)
}

// popN removes up to count values from the left or right end of the list stored
// at key. A pop that removes values and leaves values behind wakes the next
// waiting client, once the shard lock is released.
func (s *Store) popN(changes Changes, key string, count int, left bool) ([][]byte, bool, error) {
	if count < 0 {
		return nil, false, ErrSyntax
	}

	// As in popOne, the values and the count left come back through variables.
	var values [][]byte
	remaining := 0
	ok, err := mutateKey(s, changes, key, kindList, shrinks, func(cur *ValueObject, _ int64) (*ValueObject, bool, error) {
		if cur == nil {
			return nil, false, errUnchanged
		}
		list, err := listOf(cur)
		if err != nil {
			return nil, false, err
		}
		if list.empty() {
			return nil, false, errUnchanged
		}
		values, remaining = list.popValues(count, left)
		if len(values) == 0 {
			return cur, true, errUnchanged
		}
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
		list, err := listOf(value)
		if err != nil {
			return nil, err
		}

		live := list.live()
		from, to, ok := normalizeListRange(len(live), start, stop)
		if !ok {
			return [][]byte{}, nil
		}
		return cloneList(live[from : to+1]), nil
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
// of newly added members. When maxmemory is configured it first evicts keys to
// make room.
func (s *Store) ZAdd(changes Changes, key string, entries []ZSetEntry) (int64, error) {
	if len(entries) == 0 {
		return 0, ErrSyntax
	}

	return mutateKey(s, changes, key, kindZSet, mayGrow, func(cur *ValueObject, _ int64) (*ValueObject, int64, error) {
		if cur == nil {
			created := newZSetValueForEntries(entries)
			return newKindValue(ValueObject{}, created), int64(created.len()), nil
		}
		zset, err := zsetOf(cur)
		if err != nil {
			return nil, 0, err
		}
		return cur, zset.add(entries), nil
	})
}

// ZScores returns the scores of the requested sorted-set members under a
// single lock acquisition; found[i] reports whether members[i] exists.
func (s *Store) ZScores(key string, members [][]byte) ([]float64, []bool, error) {
	scores := make([]float64, len(members))
	found := make([]bool, len(members))

	_, _, err := readKey(s, key, func(value *ValueObject) (struct{}, error) {
		zset, err := zsetOf(value)
		if err != nil {
			return struct{}{}, err
		}
		for i, member := range members {
			scores[i], found[i] = zset.score(member)
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
		zset, err := zsetOf(value)
		if err != nil {
			return nil, err
		}

		from, to, ok := normalizeListRange(zset.len(), start, stop)
		if !ok {
			return []ZSetRangeEntry{}, nil
		}
		return zset.rangeByRank(from, to), nil
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
		zset, err := zsetOf(value)
		if err != nil {
			return nil, err
		}
		var entries []ZSetRangeEntry
		for _, scoreRange := range scoreRanges {
			entries = append(entries, zset.rangeByScore(scoreRange)...)
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
// ID. When maxmemory is configured it first evicts keys to make room.
func (s *Store) XAdd(changes Changes, key, rawID string, values [][]byte) (string, error) {
	if len(values) == 0 || len(values)%2 != 0 {
		return "", ErrSyntax
	}

	return mutateKey(s, changes, key, kindStream, mayGrow, func(cur *ValueObject, now int64) (*ValueObject, string, error) {
		if cur == nil {
			cur = newKindValue(ValueObject{}, StreamValue{})
		}
		stream, err := streamOf(cur)
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
		stream, err := streamOf(value)
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

	return s.copyStringsLocked(now)
}

// SnapshotStringsIf is SnapshotStrings for a caller that takes the snapshot
// only when admit allows it, and decides from the entries' lengths. It first
// calls measure with the key and value lengths of every entry the snapshot
// would hold, and whether the entry expires, copying nothing; then it calls
// admit, and copies the entries only when admit returns true. Otherwise it
// returns no entries and false, with the same stats.
//
// Both passes run under one hold of the shard read locks and at one time, so
// the entries returned are the ones measured. measure and admit run under
// those locks and must not call the Store.
func (s *Store) SnapshotStringsIf(measure func(keyBytes, valueBytes int, expires bool), admit func() bool) ([]StringSnapshotEntry, StringSnapshotStats, bool) {
	now := s.now()

	s.readLockAllShards()
	defer s.readUnlockAllShards()

	stats := s.eachSnapshotStringLocked(now, func(key string, value *ValueObject) {
		measure(len(key), len(value.String), value.ExpiresAt > 0)
	})
	if !admit() {
		return nil, stats, false
	}
	entries, stats := s.copyStringsLocked(now)
	return entries, stats, true
}

func (s *Store) copyStringsLocked(now int64) ([]StringSnapshotEntry, StringSnapshotStats) {
	entries := make([]StringSnapshotEntry, 0, s.totalKeyCountLocked())
	valueArena := make([]byte, 0)
	stats := s.eachSnapshotStringLocked(now, func(key string, value *ValueObject) {
		var clonedValue []byte
		valueArena, clonedValue = appendClonedBytesArena(valueArena, value.String)

		entries = append(entries, StringSnapshotEntry{
			Key:       key,
			Value:     clonedValue,
			ExpiresAt: value.ExpiresAt,
		})
	})

	return entries, stats
}

// eachSnapshotStringLocked calls visit for every key a string snapshot taken
// at now holds, and counts the keys it holds and the ones it skips.
func (s *Store) eachSnapshotStringLocked(now int64, visit func(key string, value *ValueObject)) StringSnapshotStats {
	stats := StringSnapshotStats{}
	for i := range s.shards {
		shard := &s.shards[i]
		stats.TotalKeys += len(shard.data)
		for key, value := range shard.data {
			if isExpired(value, now) {
				stats.SkippedExpiredKeys++
				continue
			}
			if value.data != nil {
				stats.SkippedUnsupportedKeys++
				continue
			}

			visit(key, value)
			stats.ExportedKeys++
		}
	}

	return stats
}

// ReplaceWith swaps the store contents with a snapshot from another store.
//
// The replacement is applied only after the source snapshot has already been
// materialized, which makes it suitable for FULLRESYNC-style state replacement.
func (s *Store) ReplaceWith(changes Changes, other *Store) {
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
	s.writeUnlockAllShardsAndReport(changes)
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

func (s *Store) pushList(changes Changes, key string, values [][]byte, left bool) (int64, error) {
	if len(values) == 0 {
		return 0, ErrSyntax
	}

	length, err := mutateKey(s, changes, key, kindList, mayGrow, func(cur *ValueObject, _ int64) (*ValueObject, int64, error) {
		if cur == nil {
			cur = newKindValue(ValueObject{}, listValue{})
		}
		list, err := listOf(cur)
		if err != nil {
			return nil, 0, err
		}
		return cur, int64(list.push(cloneList(values), left)), nil
	})
	if err != nil {
		return 0, err
	}

	s.wakeNextListWaiter(key)
	return length, nil
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
