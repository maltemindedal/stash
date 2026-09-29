package storage

const keyStatsKindCount = 6

var keyStatsKinds = [...]ValueKind{
	ValueKindString,
	ValueKindList,
	ValueKindHash,
	ValueKindSet,
	ValueKindZSet,
	ValueKindStream,
}

// KeyStats is an approximate snapshot of key counts by value kind.
type KeyStats struct {
	TotalKeys int
	ByKind    map[ValueKind]int
}

// KeyStats returns an approximate snapshot of resident key counts by value kind.
// Expired keys are reflected once passive or active expiry removes them.
func (s *Store) KeyStats() KeyStats {
	stats := KeyStats{ByKind: make(map[ValueKind]int)}
	if s == nil {
		return stats
	}

	for i, kind := range keyStatsKinds {
		count := int(s.keyKindCounts[i].Load())
		stats.TotalKeys += count
		stats.ByKind[kind] = count
	}

	return stats
}

func (s *Store) setKeyLocked(shard *Shard, key string, value *ValueObject) {
	if shard == nil || value == nil {
		return
	}

	previous := shard.data[key]
	if previous == nil || previous.Kind != value.Kind {
		s.adjustKeyKindCount(previous, -1)
		s.adjustKeyKindCount(value, 1)
	}
	shard.data[key] = value
	s.noteExpiry(value.ExpiresAt)
}

func (s *Store) removeKeyLocked(shard *Shard, key string) {
	if shard == nil {
		return
	}

	previous := shard.data[key]
	if previous != nil {
		s.adjustKeyKindCount(previous, -1)
	}
	delete(shard.data, key)
}

func (s *Store) adjustKeyKindCount(value *ValueObject, delta int64) {
	if s == nil || value == nil || delta == 0 {
		return
	}
	if index, ok := keyStatsKindIndex(value.Kind); ok {
		s.keyKindCounts[index].Add(delta)
	}
}

func (s *Store) recalculateKeyStatsLocked() {
	var counts [keyStatsKindCount]int64
	for i := range s.shards {
		for _, value := range s.shards[i].data {
			if index, ok := keyStatsKindIndex(value.Kind); ok {
				counts[index]++
			}
		}
	}
	for i := range counts {
		s.keyKindCounts[i].Store(counts[i])
	}
}

func keyStatsKindIndex(kind ValueKind) (int, bool) {
	for i, candidate := range keyStatsKinds {
		if kind == candidate {
			return i, true
		}
	}
	return 0, false
}
