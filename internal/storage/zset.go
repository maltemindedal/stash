package storage

import (
	"math/rand"
)

const (
	zsetMaxLevel = 16
	zsetP        = 0.25
)

// SortedSet is the map-backed sorted set encoding: index gives O(1) score
// lookup by member, and order is a skip list keyed by (score, member) so range
// and rank queries stay logarithmic. Small sorted sets use the flat encoding
// instead and promote here when they outgrow it.
type SortedSet struct {
	index map[string]float64
	order *zsetSkipList
}

type zsetSkipList struct {
	header *zsetNode
	level  int
	length int
}

type zsetNode struct {
	member string
	score  float64
	levels []zsetLevel
}

type zsetLevel struct {
	forward *zsetNode
	span    int
}

func newSortedSet() *SortedSet {
	return &SortedSet{
		index: make(map[string]float64),
		order: newZSetSkipList(),
	}
}

func cloneSortedSet(src *SortedSet) *SortedSet {
	if src == nil {
		return nil
	}

	cloned := newSortedSet()
	for node := src.order.header.levels[0].forward; node != nil; node = node.levels[0].forward {
		cloned.add(node.member, node.score)
	}

	return cloned
}

func newZSetSkipList() *zsetSkipList {
	header := &zsetNode{levels: make([]zsetLevel, zsetMaxLevel)}
	return &zsetSkipList{header: header, level: 1}
}

func (s *SortedSet) len() int {
	return len(s.index)
}

func (s *SortedSet) add(member string, score float64) bool {
	current, exists := s.index[member]
	if exists {
		if current == score {
			return false
		}
		s.order.delete(current, member)
	}

	s.order.insert(score, member)
	s.index[member] = score
	return !exists
}

func (s *SortedSet) score(member string) (float64, bool) {
	score, ok := s.index[member]
	return score, ok
}

func (s *SortedSet) rangeByRank(start, stop int) []ZSetRangeEntry {
	if start < 0 || stop < start || start >= s.order.length {
		return nil
	}
	if stop >= s.order.length {
		// Clamp so the forward walk stops at the tail instead of dereferencing
		// past it. Callers currently pre-clamp, but the compact variant guards
		// this too; keep the two consistent.
		stop = s.order.length - 1
	}

	node := s.order.getByRank(start + 1)
	if node == nil {
		return nil
	}

	items := make([]ZSetRangeEntry, stop-start+1)
	for i := range items {
		items[i] = ZSetRangeEntry{Member: node.member, Score: node.score}
		node = node.levels[0].forward
	}

	return items
}

func (sl *zsetSkipList) insert(score float64, member string) *zsetNode {
	var (
		update [zsetMaxLevel]*zsetNode
		rank   [zsetMaxLevel]int
	)

	x := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		if i == sl.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}

		for next := x.levels[i].forward; next != nil && zsetLess(next.score, next.member, score, member); next = x.levels[i].forward {
			rank[i] += x.levels[i].span
			x = next
		}
		update[i] = x
	}

	level := randomZSetLevel()
	if level > sl.level {
		for i := sl.level; i < level; i++ {
			rank[i] = 0
			update[i] = sl.header
			update[i].levels[i].span = sl.length
		}
		sl.level = level
	}

	x = &zsetNode{member: member, score: score, levels: make([]zsetLevel, level)}
	for i := 0; i < level; i++ {
		x.levels[i].forward = update[i].levels[i].forward
		update[i].levels[i].forward = x

		x.levels[i].span = update[i].levels[i].span - (rank[0] - rank[i])
		update[i].levels[i].span = (rank[0] - rank[i]) + 1
	}

	for i := level; i < sl.level; i++ {
		update[i].levels[i].span++
	}

	sl.length++
	return x
}

func (sl *zsetSkipList) delete(score float64, member string) bool {
	var update [zsetMaxLevel]*zsetNode

	x := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		for next := x.levels[i].forward; next != nil && zsetLess(next.score, next.member, score, member); next = x.levels[i].forward {
			x = next
		}
		update[i] = x
	}

	x = x.levels[0].forward
	if x == nil || x.score != score || x.member != member {
		return false
	}

	sl.deleteNode(x, update)
	return true
}

func (sl *zsetSkipList) deleteNode(node *zsetNode, update [zsetMaxLevel]*zsetNode) {
	for i := 0; i < sl.level; i++ {
		if update[i].levels[i].forward == node {
			update[i].levels[i].span += node.levels[i].span - 1
			update[i].levels[i].forward = node.levels[i].forward
		} else {
			update[i].levels[i].span--
		}
	}

	for sl.level > 1 && sl.header.levels[sl.level-1].forward == nil {
		sl.level--
	}
	sl.length--
}

func (s *SortedSet) rangeByScore(scoreRange ScoreRange) []ZSetRangeEntry {
	var items []ZSetRangeEntry
	for node := s.order.firstInScoreRange(scoreRange); node != nil; node = node.levels[0].forward {
		if scoreRange.aboveMax(node.score) {
			break
		}
		items = append(items, ZSetRangeEntry{Member: node.member, Score: node.score})
	}

	return items
}

// firstInScoreRange returns the first node whose score satisfies the range's
// lower bound, or nil when no node does. The caller checks the upper bound.
func (sl *zsetSkipList) firstInScoreRange(scoreRange ScoreRange) *zsetNode {
	x := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		for next := x.levels[i].forward; next != nil && scoreRange.belowMin(next.score); next = x.levels[i].forward {
			x = next
		}
	}

	return x.levels[0].forward
}

func (sl *zsetSkipList) getByRank(rank int) *zsetNode {
	if rank <= 0 || rank > sl.length {
		return nil
	}

	traversed := 0
	x := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		for next := x.levels[i].forward; next != nil && traversed+x.levels[i].span <= rank; next = x.levels[i].forward {
			traversed += x.levels[i].span
			x = next
		}
		if traversed == rank {
			return x
		}
	}

	return nil
}

func randomZSetLevel() int {
	level := 1
	for level < zsetMaxLevel && rand.Float64() < zsetP {
		level++
	}
	return level
}

func zsetLess(leftScore float64, leftMember string, rightScore float64, rightMember string) bool {
	if leftScore != rightScore {
		return leftScore < rightScore
	}

	return leftMember < rightMember
}
