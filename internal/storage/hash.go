package storage

// HSet assigns one or more field/value pairs on the hash stored at key and
// returns the number of newly created fields. Existing fields are overwritten.
// When maxmemory is configured it first frees space and reports the keys
// evicted to make room; otherwise it evicts nothing.
func (s *Store) HSet(key string, pairs []HashFieldValue) (int64, []string, error) {
	if len(pairs) == 0 {
		return 0, nil, ErrSyntax
	}

	return mutateKey(s, key, ValueKindHash, mayGrow, func(cur *ValueObject, _ int64) (*ValueObject, int64, error) {
		if cur == nil {
			created := newHashValueForPairs(pairs, 0)
			n, err := created.hashLen()
			return created, int64(n), err
		}
		added, err := cur.hashSet(pairs)
		return cur, added, err
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

	removed, _, err := mutateKey(s, key, ValueKindHash, shrinks, func(cur *ValueObject, _ int64) (*ValueObject, int64, error) {
		if cur == nil {
			return nil, 0, nil
		}
		removed, err := cur.hashDel(fields)
		return cur, removed, err
	})
	return removed, err
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
