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

	// One pass may keep sampling for a quarter of the interval, the same share of
	// time Redis gives its active expiry cycle.
	budget := interval / 4

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
				s.evictionPass(sampleSize, budget)
			}
		}
	}()

	return done
}

// expiredSampleRepeatDenominator sets the share of a sample that must have
// expired for a pass to sample again: more than one in this many, a quarter, as
// in Redis.
const expiredSampleRepeatDenominator = 4

// evictionPass runs one background eviction pass and returns how many samples it
// took. A pass samples the keyspace and removes the expired keys it finds; if
// more than a quarter of the sample had expired, the keyspace is probably full of
// expired keys, so it samples again, until the sample is mostly live or the
// budget is spent. Without the repetition a fixed sample per interval clears only
// sampleSize keys per interval however many have expired.
//
// A panic in it, most likely from the expiration listener, which reaches
// replication and durability sinks, is logged and ends only this pass:
// recovering around the whole loop instead would let one bug stop active expiry
// for the life of the process.
func (s *Store) evictionPass(sampleSize int, budget time.Duration) (samples int) {
	defer func() {
		if recovered := recover(); recovered != nil {
			s.logError("background eviction pass panicked", "panic", fmt.Sprint(recovered), "stack", string(debug.Stack()))
		}
	}()

	deadline := time.Now().Add(budget)
	for {
		sampled, removed := s.sweepSample(sampleSize)
		samples++

		if removed*expiredSampleRepeatDenominator <= sampled || !time.Now().Before(deadline) {
			return samples
		}
	}
}

// sweepSample takes one sample, removes its expired keys, and publishes the
// removals, all inside the eviction guard so that they reach the listener in
// order with the writes the guard's owner is logging. It returns how many keys
// it sampled and how many it removed.
func (s *Store) sweepSample(sampleSize int) (sampled, removed int) {
	release := s.acquireEvictionGuard()
	defer release()

	sampled, expired := s.evictExpiredSample(time.Now().UnixMilli(), sampleSize)
	if len(expired) > 0 {
		s.logDebug("background eviction removed expired keys", "removed", len(expired), "sample_size", sampleSize)
		// Published once evictExpiredSample has released every shard lock
		// it took: the listener reaches sinks outside the store, which
		// must never be entered while holding one.
		s.publishExpiredKeys()
	}

	return sampled, len(expired)
}

// evictExpiredSample removes the expired keys in one sample of the keyspace and
// returns how many keys it sampled and the ones it removed.
func (s *Store) evictExpiredSample(now int64, sampleSize int) (sampled int, expired []string) {
	keys := s.snapshotKeys(sampleSize)
	if len(keys) == 0 {
		return 0, nil
	}
	if len(keys) == 1 {
		shard := s.shardForKey(keys[0])
		shard.mu.Lock()
		defer shard.mu.Unlock()

		value, ok := shard.data[keys[0]]
		if ok && isExpired(value, now) {
			s.deleteKeyLocked(shard, keys[0])
			s.noteExpiredKeysLocked(keys)
			return 1, keys
		}

		return 1, nil
	}

	groups := s.groupKeysByShard(keys)
	groups.lock(s)
	defer groups.unlock(s)

	expired = make([]string, 0, len(keys))
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
	return len(keys), expired
}
