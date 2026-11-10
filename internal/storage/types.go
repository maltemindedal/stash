package storage

import (
	"sync/atomic"
)

// ValueKind identifies the logical data shape of a stored value.
type ValueKind string

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

// ValueObject is the internal representation of an item stored in Stash: its
// TTL, its access stamp and its value.
//
// The value is a string when data is nil, and then String holds its bytes.
// Otherwise data is the payload of a collection kind and String is unused. A
// string therefore costs one allocation beside its bytes, and GET and SET make
// no interface call.
//
// ExpiresAt is a Unix timestamp in milliseconds; 0 means no TTL.
// LastAccessedAt is the last successful access in Unix milliseconds. Reads
// refresh it with atomic stores, so shard read paths keep their RLock fast path
// while still providing data for approximate LRU eviction. The two int64 fields
// come first, which keeps them 8-byte aligned for those atomic stores on every
// platform.
type ValueObject struct {
	ExpiresAt      int64
	LastAccessedAt int64
	String         []byte
	data           kindValue
}

// kind returns the kind of the value v holds.
func (v *ValueObject) kind() kindID {
	if v.data == nil {
		return kindString
	}
	return v.data.kind()
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
	if v.data != nil {
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
	if v.data != nil {
		return v.data.clone(*v)
	}
	c := *v
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
	}
}

// newKindValue returns a value that holds data, the payload of a collection
// kind, under a copy of header: a ValueObject{} for a new key, which the write
// that stores it stamps, or the stored value's own header for a clone. The
// value and its payload are one allocation, so a collection costs no more
// allocations than a string does.
func newKindValue[T any, P interface {
	*T
	kindValue
}](header ValueObject, data T) *ValueObject {
	box := &struct {
		value ValueObject
		data  T
	}{value: header, data: data}
	box.value.data = P(&box.data)
	return &box.value
}
