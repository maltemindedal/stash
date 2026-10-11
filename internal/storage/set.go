package storage

// SAdd adds the supplied members to the set stored at key and returns the
// number of newly added members. When maxmemory is configured it first frees
// space and reports the keys evicted to make room; otherwise it evicts nothing.
func (s *Store) SAdd(key string, members [][]byte) (int64, []string, error) {
	if len(members) == 0 {
		return 0, nil, ErrSyntax
	}

	return mutateKey(s, key, ValueKindSet, mayGrow, func(cur *ValueObject, _ int64) (*ValueObject, int64, error) {
		if cur == nil {
			created := newSetValueForMembers(members, 0)
			n, err := created.setLen()
			return created, int64(n), err
		}
		added, err := cur.setAdd(members)
		return cur, added, err
	})
}

// SIsMember reports whether the supplied member is contained in the set stored at key.
func (s *Store) SIsMember(key string, member []byte) (bool, error) {
	exists, _, err := readKey(s, key, func(value *ValueObject) (bool, error) {
		return value.setContains(member)
	})
	return exists, err
}

// SRem removes the supplied members from the set stored at key and returns the
// number of members actually removed. An empty set is deleted from the store.
func (s *Store) SRem(key string, members [][]byte) (int64, error) {
	if len(members) == 0 {
		return 0, ErrSyntax
	}

	removed, _, err := mutateKey(s, key, ValueKindSet, shrinks, func(cur *ValueObject, _ int64) (*ValueObject, int64, error) {
		if cur == nil {
			return nil, 0, nil
		}
		removed, err := cur.setRemove(members)
		return cur, removed, err
	})
	return removed, err
}

// SMembers returns every member of the set stored at key. Order is not
// guaranteed and mirrors Redis semantics.
func (s *Store) SMembers(key string) ([][]byte, error) {
	members, found, err := readKey(s, key, func(value *ValueObject) ([][]byte, error) {
		return value.setMembers()
	})
	if err != nil {
		return nil, err
	}
	if !found {
		return [][]byte{}, nil
	}
	return members, nil
}
