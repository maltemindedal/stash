package storage

import (
	"maps"
	"sort"
)

// setValue is a set in one of its two encodings. Exactly one of ints and table
// is set: a set of at most intSetMaxEntries integers is a sorted slice, and it
// becomes a hash table, for good, when an SADD outgrows that.
type setValue struct {
	ints  *IntSet
	table map[string]struct{}
}

// setOf returns the set v holds, or ErrWrongType when v holds a value of
// another kind.
func setOf(v *ValueObject) (*setValue, error) {
	if set, ok := v.data.(*setValue); ok {
		return set, nil
	}
	return nil, ErrWrongType
}

// newSetValueForMembers returns a set of members, as integers when they allow
// it.
func newSetValueForMembers(members [][]byte) setValue {
	if ints, ok := newIntSet(members); ok && ints.len() <= intSetMaxEntries {
		return setValue{ints: ints}
	}

	table := make(map[string]struct{}, len(members))
	addSetMembers(table, members)
	return setValue{table: table}
}

func addSetMembers(table map[string]struct{}, members [][]byte) int64 {
	added := int64(0)
	for _, member := range members {
		if _, exists := table[string(member)]; exists {
			continue
		}
		table[string(member)] = struct{}{}
		added++
	}
	return added
}

func (s *setValue) len() int {
	if s.ints != nil {
		return s.ints.len()
	}
	return len(s.table)
}

// add adds members and returns how many are new. An integer set becomes a hash
// table when a member is not an integer, or when it grows past
// intSetMaxEntries, so that later adds and removes stop paying the sorted
// slice's O(n) cost.
func (s *setValue) add(members [][]byte) int64 {
	if s.ints == nil {
		return addSetMembers(s.table, members)
	}

	added, ok := s.ints.addMany(members)
	if !ok {
		s.ints, s.table = nil, s.ints.generalSet()
		return addSetMembers(s.table, members)
	}
	if s.ints.len() > intSetMaxEntries {
		s.ints, s.table = nil, s.ints.generalSet()
	}
	return added
}

func (s *setValue) contains(member []byte) bool {
	if s.ints != nil {
		return s.ints.contains(member)
	}
	_, exists := s.table[string(member)]
	return exists
}

// remove removes members and returns how many were in the set.
func (s *setValue) remove(members [][]byte) int64 {
	removed := int64(0)
	if s.ints != nil {
		for _, member := range members {
			if s.ints.remove(member) {
				removed++
			}
		}
		return removed
	}

	for _, member := range members {
		if _, exists := s.table[string(member)]; !exists {
			continue
		}
		delete(s.table, string(member))
		removed++
	}
	return removed
}

// members returns a copy of every member, in no particular order.
func (s *setValue) members() [][]byte {
	if s.ints != nil {
		return s.ints.members()
	}

	members := make([][]byte, 0, len(s.table))
	for member := range s.table {
		members = append(members, []byte(member))
	}
	return members
}

func (s *setValue) kind() kindID { return kindSet }

func (s *setValue) approxSize() int64 {
	size := approxCollectionOverhead
	if s.ints != nil {
		return size + int64(s.ints.len())*8
	}
	size += int64(len(s.table)) * approxSetEntryOverhead
	for member := range s.table {
		size += int64(len(member))
	}
	return size
}

// clone copies the integers or the hash table.
func (s *setValue) clone(header ValueObject) *ValueObject {
	return newKindValue(header, setValue{ints: cloneIntSet(s.ints), table: maps.Clone(s.table)})
}

func (s *setValue) empty() bool { return s.len() == 0 }

func (s *setValue) image(a *arena) kindImage {
	if s.ints != nil {
		// members formats each integer into bytes of its own.
		return a.sets.hold(s.ints.members())
	}

	members := make([][]byte, 0, len(s.table))
	for member := range s.table {
		members = append(members, a.string(member))
	}
	return a.sets.hold(members)
}

// setImage is a set in a keyspace image: its members, in no order until the
// first recreate sorts them.
type setImage [][]byte

// recreate emits SADD commands of the members in byte order, so that the same
// set always recreates with the same commands.
func (img *setImage) recreate(key string, emit func(args [][]byte) error) error {
	members := *img
	sort.Slice(members, func(i, j int) bool { return string(members[i]) < string(members[j]) })
	return emitChunked(emit, "SADD", key, len(members), 1, func(args [][]byte, i int) [][]byte {
		return append(args, members[i])
	})
}

// SAdd adds the supplied members to the set stored at key and returns the
// number of newly added members. When maxmemory is configured it first evicts
// keys to make room.
func (s *Store) SAdd(changes Changes, key string, members [][]byte) (int64, error) {
	if len(members) == 0 {
		return 0, ErrSyntax
	}

	return mutateKey(s, changes, key, kindSet, mayGrow, func(cur *ValueObject, _ int64) (*ValueObject, int64, error) {
		if cur == nil {
			created := newSetValueForMembers(members)
			return newKindValue(ValueObject{}, created), int64(created.len()), nil
		}
		set, err := setOf(cur)
		if err != nil {
			return nil, 0, err
		}
		return cur, set.add(members), nil
	})
}

// SIsMember reports whether the supplied member is contained in the set stored at key.
func (s *Store) SIsMember(key string, member []byte) (bool, error) {
	exists, _, err := readKey(s, key, func(value *ValueObject) (bool, error) {
		set, err := setOf(value)
		if err != nil {
			return false, err
		}
		return set.contains(member), nil
	})
	return exists, err
}

// SRem removes the supplied members from the set stored at key and returns the
// number of members actually removed. An empty set is deleted from the store.
func (s *Store) SRem(changes Changes, key string, members [][]byte) (int64, error) {
	if len(members) == 0 {
		return 0, ErrSyntax
	}

	removed, err := mutateKey(s, changes, key, kindSet, shrinks, func(cur *ValueObject, _ int64) (*ValueObject, int64, error) {
		if cur == nil {
			return nil, 0, errUnchanged
		}
		set, err := setOf(cur)
		if err != nil {
			return nil, 0, err
		}
		removed := set.remove(members)
		if removed == 0 {
			return cur, 0, errUnchanged
		}
		return cur, removed, nil
	})
	return removed, err
}

// SMembers returns every member of the set stored at key. Order is not
// guaranteed and mirrors Redis semantics.
func (s *Store) SMembers(key string) ([][]byte, error) {
	members, found, err := readKey(s, key, func(value *ValueObject) ([][]byte, error) {
		set, err := setOf(value)
		if err != nil {
			return nil, err
		}
		return set.members(), nil
	})
	if err != nil {
		return nil, err
	}
	if !found {
		return [][]byte{}, nil
	}
	return members, nil
}
