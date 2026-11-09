package storage

import "errors"

// Cause is why a key changed.
type Cause uint8

const (
	// Written: the write the caller asked for changed the key.
	Written Cause = iota + 1
	// Expired: an active TTL eviction removed the key. A sweep found it past
	// its TTL deadline.
	Expired
	// MemoryEvicted: a memory-pressure eviction removed the key, to make room
	// for a write under maxmemory.
	MemoryEvicted
)

// Changes hears each keyspace change of one request or Background unit: a key
// the Store wrote or removed during one call, and why. The Store calls it after
// it has released every shard lock, on the goroutine that made the change,
// before the write returns.
//
// A write that succeeds without changing its key (HDEL of a missing field, LPOP
// with a count of 0) reports nothing. Neither does a passive TTL eviction, where
// the operation that touches a key finds it expired: every server that holds
// the key reaches that conclusion from the TTL it already has.
//
// A nil Changes hears nothing.
type Changes interface {
	KeyChanged(key string, cause Cause)
}

// errUnchanged is what a mutation returns, with its result, when it succeeded
// without changing its key. The write primitives treat it as success and report
// no change; it never reaches a caller of the Store.
var errUnchanged = errors.New("storage: the write did not change its key")

// changedBy splits what a mutation returned into whether it changed its key and
// the error it failed with.
func changedBy(err error) (changed bool, failure error) {
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, errUnchanged):
		return false, nil
	default:
		return false, err
	}
}

// reportWritten tells changes that the caller's write changed key, if it did. A
// write defers it before it takes its locks, so that it runs after they are
// released.
func reportWritten(changes Changes, key string, written *bool) {
	if *written && changes != nil {
		changes.KeyChanged(key, Written)
	}
}
