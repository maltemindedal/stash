package storage

// SAdd adds the supplied members to the set stored at key and returns the
// number of newly added members. When maxmemory is configured it first frees
// space and reports the keys evicted to make room; otherwise it evicts nothing.
func (s *Store) SAdd(key string, members [][]byte) (int64, []string, error) {
	if len(members) == 0 {
		return 0, nil, ErrSyntax
	}

	return writeKey(s, key, func(w keyWrite) (int64, []string, error) {
		var (
			newValue *ValueObject
			added    int64
			err      error
		)
		if w.current != nil {
			newValue = w.current
			if w.accounting {
				// Size the write against the pre-write value, and leave the set
				// untouched if it turns out to breach maxmemory, by mutating a copy
				// rather than the value still stored under the key.
				newValue, err = w.current.cloneSetValue(w.current.ExpiresAt)
				if err != nil {
					return 0, nil, err
				}
			}
			added, err = newValue.setAdd(members)
			if err != nil {
				return 0, nil, err
			}
		} else {
			newValue = newSetValueForMembers(members, 0)
			newLen, lenErr := newValue.setLen()
			if lenErr != nil {
				return 0, nil, lenErr
			}
			added = int64(newLen)
		}

		evicted, err := w.commit(newValue)
		if err != nil {
			return 0, nil, err
		}
		return added, evicted, nil
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

	shard := s.shardForKey(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	now := s.now()
	value, ok := shard.data[key]
	if ok && isExpired(value, now) {
		s.deleteKeyLocked(shard, key)
		ok = false
	}
	if !ok {
		return 0, nil
	}
	accounting := s.maxMemoryEnabled()
	var oldSize int64
	if accounting {
		oldSize = s.approximateValueObjectSize(key, value)
	}

	removed, err := value.setRemove(members)
	if err != nil {
		return 0, err
	}

	value.touch(now)
	setLen, err := value.setLen()
	if err != nil {
		return 0, err
	}
	if setLen == 0 {
		s.deleteKeyWithSizeLocked(shard, key, oldSize)
		return removed, nil
	}
	if accounting {
		newSize := s.approximateValueObjectSize(key, value)
		s.usedMemory.Add(newSize - oldSize)
	}

	return removed, nil
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
