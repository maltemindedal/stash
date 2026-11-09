package storage

import (
	"errors"
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
type ValueObject struct {
	String         []byte
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

// emptied reports whether v is a list, hash, set or sorted set with nothing
// left in it, which a write deletes rather than keeps. A string or a stream is
// kept however short it is.
func (v *ValueObject) emptied() bool {
	return v.data != nil && v.data.empty()
}

// containerCopy returns a copy of v that a growing write can edit while v stays
// stored under its key, so that a write refused for want of memory leaves v as
// it was. It copies the container and not the elements (see kindValue.clone). A
// string's bytes are shared too, so a write that edits a string's bytes in place
// must not be handed this copy.
func (v *ValueObject) containerCopy() *ValueObject {
	c := *v
	if v.data != nil {
		c.data = v.data.clone()
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

// newKindValue wraps the payload of a collection kind in a value without a
// TTL. The write that stores it stamps it.
func newKindValue(data kindValue) *ValueObject {
	return &ValueObject{data: data, Kind: kindNames[data.kind()]}
}
