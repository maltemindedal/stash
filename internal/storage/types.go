package storage

import (
	"errors"
	"math"
	"sync/atomic"
	"time"
)

var errInvalidValueObjectState = errors.New("storage: invalid value object state")

// ValueKind identifies the logical data shape of a stored value.
type ValueKind string

// ValueEncoding identifies the internal physical representation of a stored value.
type ValueEncoding uint8

const (
	// ValueEncodingGeneral stores a value in its general-purpose representation.
	ValueEncodingGeneral ValueEncoding = iota
	// ValueEncodingCompact stores a small collection in a contiguous compact representation.
	ValueEncodingCompact
)

const (
	// ValueKindString is the Phase 1 string storage type used by SET/GET.
	ValueKindString ValueKind = "string"
	// ValueKindList is the Phase 2 list storage type used by LPUSH/RPUSH/LRANGE.
	ValueKindList ValueKind = "list"
	// ValueKindZSet is the Phase 2 sorted-set storage type used by ZADD/ZRANGE.
	ValueKindZSet ValueKind = "zset"
	// ValueKindStream is the Phase 2 stream storage type used by XADD/XREAD.
	ValueKindStream ValueKind = "stream"
	// ValueKindHash is the Phase 6 hash storage type used by HSET/HGET/HDEL/HGETALL.
	ValueKindHash ValueKind = "hash"
	// ValueKindSet is the Phase 6 set storage type used by SADD/SISMEMBER/SREM/SMEMBERS.
	ValueKindSet ValueKind = "set"
)

// HashFieldValue represents a field/value pair for hash commands.
type HashFieldValue struct {
	Field string
	Value []byte
}

// ZSetEntry represents a member/score pair in a sorted set.
type ZSetEntry struct {
	Member []byte
	Score  float64
}

// ZSetRangeEntry represents a member/score pair returned from ZRANGE.
type ZSetRangeEntry struct {
	Member string
	Score  float64
}

// ScoreRange bounds a sorted-set score interval for ZRangeByScore scans.
// Min and Max are inclusive unless the matching exclusive flag is set, and
// either bound may be infinite.
type ScoreRange struct {
	Min          float64
	Max          float64
	MinExclusive bool
	MaxExclusive bool
}

func (r ScoreRange) belowMin(score float64) bool {
	return score < r.Min || (r.MinExclusive && score == r.Min)
}

func (r ScoreRange) aboveMax(score float64) bool {
	return score > r.Max || (r.MaxExclusive && score == r.Max)
}

// StreamEntry represents a stream entry returned by XREAD.
type StreamEntry struct {
	ID     string
	Values [][]byte
}

// StringSnapshotEntry is the shutdown-persistence representation for the
// currently supported RDB writer scope: DB 0 string keys only.
type StringSnapshotEntry struct {
	Key       string
	Value     []byte
	ExpiresAt int64
}

// StringSnapshotStats summarizes the export of supported string keys from the
// in-memory store.
type StringSnapshotStats struct {
	TotalKeys              int
	ExportedKeys           int
	SkippedExpiredKeys     int
	SkippedUnsupportedKeys int
}

// SnapshotEntry is a defensive copy of a stored value used for AOF rewrite generation.
type SnapshotEntry struct {
	Key       string
	Kind      ValueKind
	ExpiresAt int64
	String    []byte
	List      [][]byte
	ZSet      []ZSetRangeEntry
	Stream    []StreamEntry
	Hash      []HashFieldValue
	Set       [][]byte
}

// SnapshotStats summarizes a full-value snapshot export.
type SnapshotStats struct {
	TotalKeys          int
	ExportedKeys       int
	SkippedExpiredKeys int
}

// ValueObject is the internal representation of an item stored in Stash.
//
// The type is a tagged union: Kind selects which payload field is valid.
// ExpiresAt is stored as a Unix timestamp in milliseconds; 0 means no TTL.
// LastAccessedAt stores the last observed successful access in Unix
// milliseconds. Reads refresh it with atomic stores so shard read paths can
// keep their RLock fast path while still providing data for approximate LRU
// eviction.
//
// A list keeps its values in List[ListHead:]; see liveList.
type ValueObject struct {
	String         []byte
	List           [][]byte
	ZSet           *SortedSet
	CompactZSet    *CompactZSet
	ZSetEncoding   ValueEncoding
	ListHead       int32
	Stream         *StreamValue
	Hash           map[string][]byte
	CompactHash    *CompactHash
	HashEncoding   ValueEncoding
	Set            map[string]struct{}
	IntSet         *IntSet
	SetEncoding    ValueEncoding
	ExpiresAt      int64
	LastAccessedAt int64
	Kind           ValueKind
}

func (v *ValueObject) touch(now int64) {
	if v == nil {
		return
	}

	atomic.StoreInt64(&v.LastAccessedAt, now)
}

func (v *ValueObject) lastAccessed() int64 {
	if v == nil {
		return 0
	}

	return atomic.LoadInt64(&v.LastAccessedAt)
}

// StringValue returns the string payload for a string value.
func (v *ValueObject) StringValue() ([]byte, error) {
	if v == nil {
		return nil, errInvalidValueObjectState
	}
	if v.Kind != ValueKindString {
		return nil, ErrWrongType
	}

	return v.String, nil
}

// ListValue returns the list payload for a list value.
func (v *ValueObject) ListValue() ([][]byte, error) {
	if v == nil {
		return nil, errInvalidValueObjectState
	}
	if v.Kind != ValueKindList {
		return nil, ErrWrongType
	}

	return v.liveList(), nil
}

// liveList returns the values of a list value. List holds the whole array the
// list lives in and ListHead counts the dead (nil) slots at its front, left by
// pops from the left. LPUSH refills those slots, so a list used from its left end
// is neither copied on push nor left holding memory it no longer needs.
func (v *ValueObject) liveList() [][]byte {
	return v.List[v.ListHead:]
}

// dropListFront removes the n left-most values of a list without copying: their
// slots become dead, and a later LPUSH can refill them.
func (v *ValueObject) dropListFront(n int) {
	if head := int(v.ListHead) + n; head <= math.MaxInt32 {
		v.ListHead = int32(head)
		return
	}

	// Two billion pops from the left with no append moving the list into a new
	// array: copy what is left instead of letting the head overflow.
	v.List = append([][]byte(nil), v.List[int(v.ListHead)+n:]...)
	v.ListHead = 0
}

// dropListEnd removes the n right-most values of a list.
func (v *ValueObject) dropListEnd(n int) {
	v.List = v.List[:len(v.List)-n]
}

// ZSetValue returns the general sorted-set payload for a sorted-set value.
// Compact sorted sets are materialized as detached general sorted sets.
func (v *ValueObject) ZSetValue() (*SortedSet, error) {
	if v == nil {
		return nil, errInvalidValueObjectState
	}
	if v.Kind != ValueKindZSet {
		return nil, ErrWrongType
	}
	if v.ZSetEncoding == ValueEncodingCompact {
		if v.CompactZSet == nil {
			return nil, errInvalidValueObjectState
		}
		set := newSortedSet()
		for _, entry := range v.CompactZSet.rangeByRank(0, v.CompactZSet.len()-1) {
			set.add(entry.Member, entry.Score)
		}
		return set, nil
	}
	if v.ZSet == nil {
		return nil, errInvalidValueObjectState
	}

	return v.ZSet, nil
}

// StreamValue returns the stream payload for a stream value.
func (v *ValueObject) StreamValue() (*StreamValue, error) {
	if v == nil {
		return nil, errInvalidValueObjectState
	}
	if v.Kind != ValueKindStream {
		return nil, ErrWrongType
	}
	if v.Stream == nil {
		return nil, errInvalidValueObjectState
	}

	return v.Stream, nil
}

func newStringValue(data []byte, expiresAt int64) *ValueObject {
	return newOwnedStringValue(cloneBytes(data), expiresAt)
}

// newOwnedStringValue stores an already-owned string representation without cloning.
// Callers must only pass byte slices that are not aliased with caller-controlled memory.
func newOwnedStringValue(data []byte, expiresAt int64) *ValueObject {
	return &ValueObject{
		String:         data,
		ExpiresAt:      expiresAt,
		LastAccessedAt: time.Now().UnixMilli(),
		Kind:           ValueKindString,
	}
}

// newListValue stores an already-owned list representation without cloning.
// Callers must only pass slices whose element bytes are not aliased with caller-
// controlled memory.
func newListValue(items [][]byte, expiresAt int64) *ValueObject {
	return &ValueObject{
		List:           items,
		ExpiresAt:      expiresAt,
		LastAccessedAt: time.Now().UnixMilli(),
		Kind:           ValueKindList,
	}
}

func newZSetValue(set *SortedSet, expiresAt int64) *ValueObject {
	return &ValueObject{
		ZSet:           set,
		ExpiresAt:      expiresAt,
		LastAccessedAt: time.Now().UnixMilli(),
		Kind:           ValueKindZSet,
	}
}

func newCompactZSetValue(set *CompactZSet, expiresAt int64) *ValueObject {
	return &ValueObject{
		CompactZSet:    set,
		ZSetEncoding:   ValueEncodingCompact,
		ExpiresAt:      expiresAt,
		LastAccessedAt: time.Now().UnixMilli(),
		Kind:           ValueKindZSet,
	}
}

func newStreamValue(stream *StreamValue, expiresAt int64) *ValueObject {
	return &ValueObject{
		Stream:         stream,
		ExpiresAt:      expiresAt,
		LastAccessedAt: time.Now().UnixMilli(),
		Kind:           ValueKindStream,
	}
}

// HashValue returns the general hash payload for a hash value. Compact hashes
// are materialized as detached general hash maps.
func (v *ValueObject) HashValue() (map[string][]byte, error) {
	if v == nil {
		return nil, errInvalidValueObjectState
	}
	if v.Kind != ValueKindHash {
		return nil, ErrWrongType
	}
	if v.HashEncoding == ValueEncodingCompact {
		if v.CompactHash == nil {
			return nil, errInvalidValueObjectState
		}
		fields := make(map[string][]byte, v.CompactHash.len())
		for _, entry := range v.CompactHash.all() {
			fields[entry.Field] = cloneBytes(entry.Value)
		}
		return fields, nil
	}
	if v.Hash == nil {
		return nil, errInvalidValueObjectState
	}

	return v.Hash, nil
}

// SetValue returns the general set payload for a set value. Integer sets are
// materialized as detached general set maps.
func (v *ValueObject) SetValue() (map[string]struct{}, error) {
	if v == nil {
		return nil, errInvalidValueObjectState
	}
	if v.Kind != ValueKindSet {
		return nil, ErrWrongType
	}
	if v.SetEncoding == ValueEncodingCompact {
		if v.IntSet == nil {
			return nil, errInvalidValueObjectState
		}
		return v.IntSet.generalSet(), nil
	}
	if v.Set == nil {
		return nil, errInvalidValueObjectState
	}

	return v.Set, nil
}

func newHashValue(fields map[string][]byte, expiresAt int64) *ValueObject {
	return &ValueObject{
		Hash:           fields,
		ExpiresAt:      expiresAt,
		LastAccessedAt: time.Now().UnixMilli(),
		Kind:           ValueKindHash,
	}
}

func newCompactHashValue(hash *CompactHash, expiresAt int64) *ValueObject {
	return &ValueObject{
		CompactHash:    hash,
		HashEncoding:   ValueEncodingCompact,
		ExpiresAt:      expiresAt,
		LastAccessedAt: time.Now().UnixMilli(),
		Kind:           ValueKindHash,
	}
}

func newSetValue(members map[string]struct{}, expiresAt int64) *ValueObject {
	return &ValueObject{
		Set:            members,
		ExpiresAt:      expiresAt,
		LastAccessedAt: time.Now().UnixMilli(),
		Kind:           ValueKindSet,
	}
}

func newIntSetValue(members *IntSet, expiresAt int64) *ValueObject {
	return &ValueObject{
		IntSet:         members,
		SetEncoding:    ValueEncodingCompact,
		ExpiresAt:      expiresAt,
		LastAccessedAt: time.Now().UnixMilli(),
		Kind:           ValueKindSet,
	}
}
