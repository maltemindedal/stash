package storage

func (v *ValueObject) hashLen() (int, error) {
	if v == nil {
		return 0, errInvalidValueObjectState
	}
	if v.Kind != ValueKindHash {
		return 0, ErrWrongType
	}
	if v.HashEncoding == ValueEncodingCompact {
		if v.CompactHash == nil {
			return 0, errInvalidValueObjectState
		}
		return v.CompactHash.len(), nil
	}
	if v.Hash == nil {
		return 0, errInvalidValueObjectState
	}
	return len(v.Hash), nil
}

func (v *ValueObject) hashGet(field string) ([]byte, bool, error) {
	if v == nil {
		return nil, false, errInvalidValueObjectState
	}
	if v.Kind != ValueKindHash {
		return nil, false, ErrWrongType
	}
	if v.HashEncoding == ValueEncodingCompact {
		if v.CompactHash == nil {
			return nil, false, errInvalidValueObjectState
		}
		value, ok := v.CompactHash.get(field)
		return value, ok, nil
	}
	if v.Hash == nil {
		return nil, false, errInvalidValueObjectState
	}
	value, ok := v.Hash[field]
	return value, ok, nil
}

func (v *ValueObject) hashSet(pairs []HashFieldValue) (int64, error) {
	if v == nil {
		return 0, errInvalidValueObjectState
	}
	if v.Kind != ValueKindHash {
		return 0, ErrWrongType
	}

	added := int64(0)
	if v.HashEncoding == ValueEncodingCompact {
		if v.CompactHash == nil {
			return 0, errInvalidValueObjectState
		}
		if !compactHashCanSet(v.CompactHash, pairs) {
			v.upgradeCompactHash()
			return v.hashSet(pairs)
		}
		for _, pair := range pairs {
			if v.CompactHash.set(pair.Field, pair.Value) {
				added++
			}
		}
		return added, nil
	}

	if v.Hash == nil {
		return 0, errInvalidValueObjectState
	}
	return hashSetGeneral(v.Hash, pairs), nil
}

func (v *ValueObject) hashDel(fields []string) (int64, error) {
	if v == nil {
		return 0, errInvalidValueObjectState
	}
	if v.Kind != ValueKindHash {
		return 0, ErrWrongType
	}

	removed := int64(0)
	if v.HashEncoding == ValueEncodingCompact {
		if v.CompactHash == nil {
			return 0, errInvalidValueObjectState
		}
		for _, field := range fields {
			if v.CompactHash.del(field) {
				removed++
			}
		}
		return removed, nil
	}

	if v.Hash == nil {
		return 0, errInvalidValueObjectState
	}
	for _, field := range fields {
		if _, exists := v.Hash[field]; exists {
			delete(v.Hash, field)
			removed++
		}
	}
	return removed, nil
}

func (v *ValueObject) hashEntries() ([]HashFieldValue, error) {
	if v == nil {
		return nil, errInvalidValueObjectState
	}
	if v.Kind != ValueKindHash {
		return nil, ErrWrongType
	}
	if v.HashEncoding == ValueEncodingCompact {
		if v.CompactHash == nil {
			return nil, errInvalidValueObjectState
		}
		return v.CompactHash.all(), nil
	}
	if v.Hash == nil {
		return nil, errInvalidValueObjectState
	}

	entries := make([]HashFieldValue, 0, len(v.Hash))
	for field, raw := range v.Hash {
		entries = append(entries, HashFieldValue{Field: field, Value: raw})
	}
	return entries, nil
}

func hashSetGeneral(hash map[string][]byte, pairs []HashFieldValue) int64 {
	added := int64(0)
	for _, pair := range pairs {
		if _, exists := hash[pair.Field]; !exists {
			added++
		}
		hash[pair.Field] = cloneBytes(pair.Value)
	}
	return added
}

func newHashValueForPairs(pairs []HashFieldValue, expiresAt int64) *ValueObject {
	fields, compactEligible := hashMapForPairs(pairs)
	if compactEligible && len(fields) <= compactHashMaxEntries {
		return newCompactHashValue(newCompactHashFromMap(fields), expiresAt)
	}

	return newHashValue(fields, expiresAt)
}

func hashMapForPairs(pairs []HashFieldValue) (map[string][]byte, bool) {
	fields := make(map[string][]byte, len(pairs))
	compactEligible := true
	for _, pair := range pairs {
		if len(pair.Field) > compactHashMaxStringSize || len(pair.Value) > compactHashMaxStringSize {
			compactEligible = false
		}
		fields[pair.Field] = cloneBytes(pair.Value)
	}
	return fields, compactEligible
}

func newCompactHashFromMap(fields map[string][]byte) *CompactHash {
	hash := &CompactHash{entries: make([]compactHashEntry, 0, len(fields))}
	for field, value := range fields {
		hash.set(field, value)
	}
	return hash
}

func compactHashCanSet(hash *CompactHash, pairs []HashFieldValue) bool {
	projectedLen := hash.len()
	newFields := make(map[string]struct{}, len(pairs))
	for _, pair := range pairs {
		if len(pair.Field) > compactHashMaxStringSize || len(pair.Value) > compactHashMaxStringSize {
			return false
		}
		if _, exists := hash.get(pair.Field); exists {
			continue
		}
		if _, exists := newFields[pair.Field]; exists {
			continue
		}
		newFields[pair.Field] = struct{}{}
		projectedLen++
		if projectedLen > compactHashMaxEntries {
			return false
		}
	}
	return true
}

func (v *ValueObject) upgradeCompactHash() {
	v.Hash = compactHashGeneralMap(v.CompactHash)
	v.CompactHash = nil
	v.HashEncoding = ValueEncodingGeneral
}

func compactHashGeneralMap(hash *CompactHash) map[string][]byte {
	fields := make(map[string][]byte, hash.len())
	for _, entry := range hash.entries {
		fields[hash.field(entry)] = cloneBytes(hash.value(entry))
	}
	return fields
}
