package storage

import (
	"bytes"
	"slices"
)

const (
	compactZSetMaxEntries   = 8
	compactZSetMaxMemberLen = 64
)

type compactZSetEntry struct {
	memberStart int
	memberLen   int
	score       float64
}

// CompactZSet stores small sorted sets as contiguous member bytes plus compact
// score metadata kept sorted by score and member.
type CompactZSet struct {
	entries []compactZSetEntry
	arena   []byte
}

func cloneCompactZSet(src *CompactZSet) *CompactZSet {
	if src == nil {
		return nil
	}

	return &CompactZSet{
		entries: append([]compactZSetEntry(nil), src.entries...),
		arena:   cloneBytes(src.arena),
	}
}

func (s *CompactZSet) len() int {
	if s == nil {
		return 0
	}
	return len(s.entries)
}

func (s *CompactZSet) add(member []byte, score float64) bool {
	for i, entry := range s.entries {
		if s.memberEqual(entry, member) {
			if entry.score == score {
				return false
			}
			s.entries[i].score = score
			s.sort()
			return false
		}
	}

	start := len(s.arena)
	s.arena = append(s.arena, member...)
	s.entries = append(s.entries, compactZSetEntry{memberStart: start, memberLen: len(member), score: score})
	s.sort()
	return true
}

func (s *CompactZSet) score(member []byte) (float64, bool) {
	for _, entry := range s.entries {
		if s.memberEqual(entry, member) {
			return entry.score, true
		}
	}
	return 0, false
}

func (s *CompactZSet) rangeByRank(start, stop int) []ZSetRangeEntry {
	if s == nil || start < 0 || stop < start || start >= len(s.entries) {
		return nil
	}
	if stop >= len(s.entries) {
		stop = len(s.entries) - 1
	}

	items := make([]ZSetRangeEntry, 0, stop-start+1)
	for _, entry := range s.entries[start : stop+1] {
		items = append(items, ZSetRangeEntry{Member: s.member(entry), Score: entry.score})
	}
	return items
}

func (s *CompactZSet) rangeByScore(scoreRange ScoreRange) []ZSetRangeEntry {
	if s == nil {
		return nil
	}

	var items []ZSetRangeEntry
	for _, entry := range s.entries {
		if scoreRange.aboveMax(entry.score) {
			break
		}
		if !scoreRange.belowMin(entry.score) {
			items = append(items, ZSetRangeEntry{Member: s.member(entry), Score: entry.score})
		}
	}
	return items
}

func (s *CompactZSet) sort() {
	slices.SortFunc(s.entries, func(left, right compactZSetEntry) int {
		if left.score < right.score {
			return -1
		}
		if left.score > right.score {
			return 1
		}
		return bytes.Compare(s.memberBytes(left), s.memberBytes(right))
	})
}

func (s *CompactZSet) member(entry compactZSetEntry) string {
	return string(s.memberBytes(entry))
}

func (s *CompactZSet) memberBytes(entry compactZSetEntry) []byte {
	return s.arena[entry.memberStart : entry.memberStart+entry.memberLen]
}

func (s *CompactZSet) memberEqual(entry compactZSetEntry, member []byte) bool {
	return bytes.Equal(s.memberBytes(entry), member)
}
