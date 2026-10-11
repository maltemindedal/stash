package storage

// HSet assigns one or more field/value pairs on the hash stored at key and
// returns the number of newly created fields. Existing fields are overwritten.
// When maxmemory is configured it first frees space and reports the keys
// evicted to make room; otherwise it evicts nothing.
func (s *Store) HSet(key string, pairs []HashFieldValue) (int64, []string, error) {
	if len(pairs) == 0 {
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
				// Size the write against the pre-write value, and leave the hash
				// untouched if it turns out to breach maxmemory, by mutating a copy
				// rather than the value still stored under the key.
				newValue, err = w.current.cloneHashValue(w.current.ExpiresAt)
				if err != nil {
					return 0, nil, err
				}
			}
			added, err = newValue.hashSet(pairs)
			if err != nil {
				return 0, nil, err
			}
		} else {
			newValue = newHashValueForPairs(pairs, 0)
			newLen, lenErr := newValue.hashLen()
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

// HGet returns the value of the supplied field on the hash stored at key.
func (s *Store) HGet(key, field string) ([]byte, bool, error) {
	// A missing field and a missing key are both reported as not found, but only
	// the field case can carry a zero-length value, so the two travel together
	// rather than relying on a nil payload to mean absent.
	type hashField struct {
		value  []byte
		exists bool
	}

	result, _, err := readKey(s, key, func(value *ValueObject) (hashField, error) {
		raw, exists, err := value.hashGet(field)
		if err != nil || !exists {
			return hashField{}, err
		}
		return hashField{value: cloneBytes(raw), exists: true}, nil
	})
	if err != nil {
		return nil, false, err
	}
	return result.value, result.exists, nil
}

// HDel removes the named fields from the hash stored at key and returns the
// number of fields actually removed. An empty hash is deleted from the store.
func (s *Store) HDel(key string, fields []string) (int64, error) {
	if len(fields) == 0 {
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

	removed, err := value.hashDel(fields)
	if err != nil {
		return 0, err
	}

	value.touch(now)
	hashLen, err := value.hashLen()
	if err != nil {
		return 0, err
	}
	if hashLen == 0 {
		s.deleteKeyWithSizeLocked(shard, key, oldSize)
		return removed, nil
	}
	if accounting {
		newSize := s.approximateValueObjectSize(key, value)
		s.usedMemory.Add(newSize - oldSize)
	}

	return removed, nil
}

// HGetAll returns every field/value pair on the hash stored at key. Order is
// not guaranteed and mirrors Redis semantics.
func (s *Store) HGetAll(key string) ([]HashFieldValue, error) {
	entries, found, err := readKey(s, key, func(value *ValueObject) ([]HashFieldValue, error) {
		entries, err := value.hashEntries()
		if err != nil {
			return nil, err
		}
		for i := range entries {
			entries[i].Value = cloneBytes(entries[i].Value)
		}
		return entries, nil
	})
	if err != nil {
		return nil, err
	}
	if !found {
		return []HashFieldValue{}, nil
	}
	return entries, nil
}
