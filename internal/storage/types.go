package storage

import (
	"errors"
	"maps"
	"math"
	"sync/atomic"
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
	ListHead       int32
	Hash           map[string][]byte
	CompactHash    *CompactHash
	HashEncoding   ValueEncoding
	ExpiresAt      int64
	LastAccessedAt int64
	Kind           ValueKind
	// data is the payload of a kind that has moved out of the union above.
	data kindValue
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

// popListFront removes the left-most value of a list that is not empty, and
// returns it with the number of values left. It and popListBack are two methods
// rather than one taking a side so that each inlines: the call cost a pop about
// 3% of its time.
func (v *ValueObject) popListFront() ([]byte, int) {
	list := v.liveList()
	item := list[0]
	list[0] = nil
	v.dropListFront(1)
	return item, len(list) - 1
}

// popListBack removes the right-most value of a list that is not empty, and
// returns it with the number of values left.
func (v *ValueObject) popListBack() ([]byte, int) {
	list := v.liveList()
	last := len(list) - 1
	item := list[last]
	list[last] = nil
	v.dropListEnd(1)
	return item, last
}

// popListValues removes up to count values, which must not be negative, from
// the left or right end of a list that is not empty. It returns them in the
// order they were popped, with the number of values left. count is an int, which
// the command parses as one, so no int64 is narrowed on the way (6db82b0).
func (v *ValueObject) popListValues(count int, left bool) ([][]byte, int) {
	list := v.liveList()
	n := min(count, len(list))
	popped := make([][]byte, n)
	if left {
		for i := 0; i < n; i++ {
			popped[i] = list[i]
			list[i] = nil
		}
		v.dropListFront(n)
	} else {
		for i := 0; i < n; i++ {
			src := len(list) - 1 - i
			popped[i] = list[src]
			list[src] = nil
		}
		v.dropListEnd(n)
	}
	return popped, len(list) - n
}

// emptied reports whether v is a list, hash, set or sorted set with nothing
// left in it, which a write deletes rather than keeps. A string or a stream is
// kept however short it is. The list case is answered here, where it inlines,
// because the pops are the commonest shrinking writes.
func (v *ValueObject) emptied() bool {
	if v.Kind == ValueKindList {
		return len(v.List) == int(v.ListHead)
	}
	return v.emptiedCollection()
}

// emptiedCollection is emptied for a value that is not a list.
func (v *ValueObject) emptiedCollection() bool {
	if v.data != nil {
		return v.data.empty()
	}
	switch v.Kind {
	case ValueKindHash:
		n, err := v.hashLen()
		return err == nil && n == 0
	}
	return false
}

// containerCopy returns a copy of v that a growing write can edit while v stays
// stored under its key, so that a write refused for want of memory leaves v as
// it was. It copies the container and not the elements: a list's array, a
// hash's map or compact entries and arena, a set's map or integers, a sorted
// set's index and skip list or compact entries and arena, and a stream's
// records. The element byte strings are shared, because nothing edits one in
// place once it is stored. A string's bytes are shared too, so a write that
// edits a string's bytes in place must not be handed this copy.
func (v *ValueObject) containerCopy() *ValueObject {
	c := *v
	if v.data != nil {
		c.data = v.data.clone()
		return &c
	}
	switch v.Kind {
	case ValueKindList:
		c.List = append([][]byte(nil), v.liveList()...)
		c.ListHead = 0
	case ValueKindHash:
		c.Hash = maps.Clone(v.Hash)
		c.CompactHash = cloneCompactHash(v.CompactHash)
	}
	return &c
}

func newStringValue(data []byte, expiresAt int64) *ValueObject {
	return newOwnedStringValue(cloneBytes(data), expiresAt)
}

// newOwnedStringValue stores an already-owned string representation without cloning.
// Callers must only pass byte slices that are not aliased with caller-controlled memory.
func newOwnedStringValue(data []byte, expiresAt int64) *ValueObject {
	return &ValueObject{
		String:    data,
		ExpiresAt: expiresAt,
		Kind:      ValueKindString,
	}
}

// newListValue stores an already-owned list representation without cloning.
// Callers must only pass slices whose element bytes are not aliased with caller-
// controlled memory.
func newListValue(items [][]byte, expiresAt int64) *ValueObject {
	return &ValueObject{
		List:      items,
		ExpiresAt: expiresAt,
		Kind:      ValueKindList,
	}
}

// newKindValue wraps the payload of a collection kind in a value without a
// TTL. The write that stores it stamps it.
func newKindValue(data kindValue) *ValueObject {
	return &ValueObject{data: data, Kind: kindNames[data.kind()]}
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

func newHashValue(fields map[string][]byte, expiresAt int64) *ValueObject {
	return &ValueObject{
		Hash:      fields,
		ExpiresAt: expiresAt,
		Kind:      ValueKindHash,
	}
}

func newCompactHashValue(hash *CompactHash, expiresAt int64) *ValueObject {
	return &ValueObject{
		CompactHash:  hash,
		HashEncoding: ValueEncodingCompact,
		ExpiresAt:    expiresAt,
		Kind:         ValueKindHash,
	}
}
