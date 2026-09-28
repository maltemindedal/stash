package storage

import (
	"context"
	"fmt"
	"runtime/debug"
	"time"
)

const defaultSampleSize = 20

// StartEviction launches the background TTL eviction loop, reporting the keys
// each pass removed through the store's expiration listener.
//
// It returns a channel closed once the loop has exited, so a caller whose
// listener writes to resources it later tears down can order that teardown after
// the loop. Cancelling ctx stops the loop.
func (s *Store) StartEviction(ctx context.Context, interval time.Duration, sampleSize int) <-chan struct{} {
	done := make(chan struct{})
	if interval <= 0 {
		close(done)
		return done
	}
	if sampleSize <= 0 {
		sampleSize = defaultSampleSize
	}

	go func() {
		defer close(done)

		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		s.logDebug("background eviction loop started", "interval", interval, "sample_size", sampleSize)

		for {
			select {
			case <-ctx.Done():
				s.logDebug("background eviction loop stopped", "reason", ctx.Err())
				return
			case <-ticker.C:
				s.evictionPass(sampleSize)
			}
		}
	}()

	return done
}

// evictionPass runs one background eviction pass. A panic in it, most likely
// from the expiration listener, which reaches replication and durability sinks,
// is logged and ends only this pass: recovering around the whole loop instead
// would let one bug stop active expiry for the life of the process.
func (s *Store) evictionPass(sampleSize int) {
	defer func() {
		if recovered := recover(); recovered != nil {
			s.logError("background eviction pass panicked", "panic", fmt.Sprint(recovered), "stack", string(debug.Stack()))
		}
	}()

	expired := s.evictExpiredSample(time.Now().UnixMilli(), sampleSize)
	if len(expired) == 0 {
		return
	}

	s.logDebug("background eviction removed expired keys", "removed", len(expired), "sample_size", sampleSize)
	// Published once evictExpiredSample has released every shard lock
	// it took: the listener reaches sinks outside the store, which
	// must never be entered while holding one.
	s.publishExpiredKeys()
}

// evictExpiredSample removes the expired keys in one sample of the keyspace and
// returns them.
func (s *Store) evictExpiredSample(now int64, sampleSize int) []string {
	keys := s.snapshotKeys(sampleSize)
	if len(keys) == 0 {
		return nil
	}
	if len(keys) == 1 {
		shard := s.shardForKey(keys[0])
		shard.mu.Lock()
		defer shard.mu.Unlock()

		value, ok := shard.data[keys[0]]
		if ok && isExpired(value, now) {
			s.deleteKeyLocked(shard, keys[0])
			s.noteExpiredKeysLocked(keys)
			return keys
		}

		return nil
	}

	groups := s.groupKeysByShard(keys)
	groups.lock(s)
	defer groups.unlock(s)

	expired := make([]string, 0, len(keys))
	for shardID, count := range groups.counts {
		if count == 0 {
			continue
		}

		shard := &s.shards[shardID]
		for _, key := range groups.keysForShard(shardID) {
			value, ok := shard.data[key]
			if ok && isExpired(value, now) {
				s.deleteKeyLocked(shard, key)
				expired = append(expired, key)
			}
		}
	}

	s.noteExpiredKeysLocked(expired)
	return expired
}
