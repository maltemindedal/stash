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

func (v *ValueObject) zsetLen() (int, error) {
	if v == nil {
		return 0, errInvalidValueObjectState
	}
	if v.Kind != ValueKindZSet {
		return 0, ErrWrongType
	}
	if v.ZSetEncoding == ValueEncodingCompact {
		if v.CompactZSet == nil {
			return 0, errInvalidValueObjectState
		}
		return v.CompactZSet.len(), nil
	}
	if v.ZSet == nil {
		return 0, errInvalidValueObjectState
	}
	return v.ZSet.len(), nil
}

func (v *ValueObject) zsetAdd(entries []ZSetEntry) (int64, error) {
	if v == nil {
		return 0, errInvalidValueObjectState
	}
	if v.Kind != ValueKindZSet {
		return 0, ErrWrongType
	}

	added := int64(0)
	if v.ZSetEncoding == ValueEncodingCompact {
		if v.CompactZSet == nil {
			return 0, errInvalidValueObjectState
		}
		if !compactZSetCanAdd(v.CompactZSet, entries) {
			v.upgradeCompactZSet()
			return v.zsetAdd(entries)
		}
		for _, entry := range entries {
			if v.CompactZSet.add(entry.Member, entry.Score) {
				added++
			}
		}
		return added, nil
	}

	if v.ZSet == nil {
		return 0, errInvalidValueObjectState
	}
	return zsetAddGeneral(v.ZSet, entries), nil
}

func (v *ValueObject) zsetScore(member []byte) (float64, bool, error) {
	if v == nil {
		return 0, false, errInvalidValueObjectState
	}
	if v.Kind != ValueKindZSet {
		return 0, false, ErrWrongType
	}
	if v.ZSetEncoding == ValueEncodingCompact {
		if v.CompactZSet == nil {
			return 0, false, errInvalidValueObjectState
		}
		score, ok := v.CompactZSet.score(member)
		return score, ok, nil
	}
	if v.ZSet == nil {
		return 0, false, errInvalidValueObjectState
	}
	score, ok := v.ZSet.score(string(member))
	return score, ok, nil
}

func (v *ValueObject) zsetRangeByRank(start, stop int) ([]ZSetRangeEntry, error) {
	if v == nil {
		return nil, errInvalidValueObjectState
	}
	if v.Kind != ValueKindZSet {
		return nil, ErrWrongType
	}
	if v.ZSetEncoding == ValueEncodingCompact {
		if v.CompactZSet == nil {
			return nil, errInvalidValueObjectState
		}
		return v.CompactZSet.rangeByRank(start, stop), nil
	}
	if v.ZSet == nil {
		return nil, errInvalidValueObjectState
	}
	return v.ZSet.rangeByRank(start, stop), nil
}

func (v *ValueObject) zsetRangeByScore(scoreRange ScoreRange) ([]ZSetRangeEntry, error) {
	if v == nil {
		return nil, errInvalidValueObjectState
	}
	if v.Kind != ValueKindZSet {
		return nil, ErrWrongType
	}
	if v.ZSetEncoding == ValueEncodingCompact {
		if v.CompactZSet == nil {
			return nil, errInvalidValueObjectState
		}
		return v.CompactZSet.rangeByScore(scoreRange), nil
	}
	if v.ZSet == nil {
		return nil, errInvalidValueObjectState
	}
	return v.ZSet.rangeByScore(scoreRange), nil
}

func (v *ValueObject) setLen() (int, error) {
	if v == nil {
		return 0, errInvalidValueObjectState
	}
	if v.Kind != ValueKindSet {
		return 0, ErrWrongType
	}
	if v.SetEncoding == ValueEncodingCompact {
		if v.IntSet == nil {
			return 0, errInvalidValueObjectState
		}
		return v.IntSet.len(), nil
	}
	if v.Set == nil {
		return 0, errInvalidValueObjectState
	}
	return len(v.Set), nil
}

func (v *ValueObject) setAdd(members [][]byte) (int64, error) {
	if v == nil {
		return 0, errInvalidValueObjectState
	}
	if v.Kind != ValueKindSet {
		return 0, ErrWrongType
	}

	if v.SetEncoding == ValueEncodingCompact {
		if v.IntSet == nil {
			return 0, errInvalidValueObjectState
		}
		added, ok := v.IntSet.addMany(members)
		if !ok {
			set := v.IntSet.generalSet()
			v.IntSet = nil
			v.Set = set
			v.SetEncoding = ValueEncodingGeneral
			return mustAddSetMembers(set, members), nil
		}
		if v.IntSet.len() > intSetMaxEntries {
			// Upgrade a large integer-only set to the hashtable encoding so
			// subsequent adds/removes stop paying the sorted-slice O(n) cost.
			v.Set = v.IntSet.generalSet()
			v.IntSet = nil
			v.SetEncoding = ValueEncodingGeneral
		}
		return added, nil
	}

	if v.Set == nil {
		return 0, errInvalidValueObjectState
	}
	return mustAddSetMembers(v.Set, members), nil
}

func (v *ValueObject) setContains(member []byte) (bool, error) {
	if v == nil {
		return false, errInvalidValueObjectState
	}
	if v.Kind != ValueKindSet {
		return false, ErrWrongType
	}
	if v.SetEncoding == ValueEncodingCompact {
		if v.IntSet == nil {
			return false, errInvalidValueObjectState
		}
		return v.IntSet.contains(member), nil
	}
	if v.Set == nil {
		return false, errInvalidValueObjectState
	}
	_, exists := v.Set[string(member)]
	return exists, nil
}

func (v *ValueObject) setRemove(members [][]byte) (int64, error) {
	if v == nil {
		return 0, errInvalidValueObjectState
	}
	if v.Kind != ValueKindSet {
		return 0, ErrWrongType
	}

	removed := int64(0)
	if v.SetEncoding == ValueEncodingCompact {
		if v.IntSet == nil {
			return 0, errInvalidValueObjectState
		}
		for _, member := range members {
			if v.IntSet.remove(member) {
				removed++
			}
		}
		return removed, nil
	}

	if v.Set == nil {
		return 0, errInvalidValueObjectState
	}
	for _, member := range members {
		if _, exists := v.Set[string(member)]; !exists {
			continue
		}
		delete(v.Set, string(member))
		removed++
	}
	return removed, nil
}

func (v *ValueObject) setMembers() ([][]byte, error) {
	if v == nil {
		return nil, errInvalidValueObjectState
	}
	if v.Kind != ValueKindSet {
		return nil, ErrWrongType
	}
	if v.SetEncoding == ValueEncodingCompact {
		if v.IntSet == nil {
			return nil, errInvalidValueObjectState
		}
		return v.IntSet.members(), nil
	}
	if v.Set == nil {
		return nil, errInvalidValueObjectState
	}

	members := make([][]byte, 0, len(v.Set))
	for member := range v.Set {
		members = append(members, []byte(member))
	}
	return members, nil
}

func newSetValueForMembers(members [][]byte, expiresAt int64) *ValueObject {
	if set, ok := newIntSet(members); ok && set.len() <= intSetMaxEntries {
		return newIntSetValue(set, expiresAt)
	}

	set := make(map[string]struct{}, len(members))
	mustAddSetMembers(set, members)
	return newSetValue(set, expiresAt)
}

func mustAddSetMembers(set map[string]struct{}, members [][]byte) int64 {
	added := int64(0)
	for _, member := range members {
		if _, exists := set[string(member)]; exists {
			continue
		}
		set[string(member)] = struct{}{}
		added++
	}
	return added
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

func zsetAddGeneral(set *SortedSet, entries []ZSetEntry) int64 {
	added := int64(0)
	for _, entry := range entries {
		if set.add(string(entry.Member), entry.Score) {
			added++
		}
	}
	return added
}

func newZSetValueForEntries(entries []ZSetEntry, expiresAt int64) *ValueObject {
	members, compactEligible := zsetMapForEntries(entries)
	if compactEligible && len(members) <= compactZSetMaxEntries {
		return newCompactZSetValue(newCompactZSetFromMap(members), expiresAt)
	}

	return newZSetValue(newSortedSetFromMap(members), expiresAt)
}

func zsetMapForEntries(entries []ZSetEntry) (map[string]float64, bool) {
	members := make(map[string]float64, len(entries))
	compactEligible := true
	for _, entry := range entries {
		if len(entry.Member) > compactZSetMaxMemberLen {
			compactEligible = false
		}
		members[string(entry.Member)] = entry.Score
	}
	return members, compactEligible
}

func newCompactZSetFromMap(members map[string]float64) *CompactZSet {
	set := &CompactZSet{entries: make([]compactZSetEntry, 0, len(members))}
	for member, score := range members {
		set.add([]byte(member), score)
	}
	return set
}

func newSortedSetFromMap(members map[string]float64) *SortedSet {
	set := newSortedSet()
	for member, score := range members {
		set.add(member, score)
	}
	return set
}

func compactZSetCanAdd(set *CompactZSet, entries []ZSetEntry) bool {
	projectedLen := set.len()
	newMembers := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		if len(entry.Member) > compactZSetMaxMemberLen {
			return false
		}
		member := string(entry.Member)
		if compactZSetContains(set, entry.Member) {
			continue
		}
		if _, exists := newMembers[member]; exists {
			continue
		}
		newMembers[member] = struct{}{}
		projectedLen++
		if projectedLen > compactZSetMaxEntries {
			return false
		}
	}
	return true
}

func compactZSetContains(set *CompactZSet, member []byte) bool {
	for _, entry := range set.entries {
		if set.memberEqual(entry, member) {
			return true
		}
	}
	return false
}

func (v *ValueObject) upgradeCompactZSet() {
	v.ZSet = compactZSetGeneralSet(v.CompactZSet)
	v.CompactZSet = nil
	v.ZSetEncoding = ValueEncodingGeneral
}

func compactZSetGeneralSet(compact *CompactZSet) *SortedSet {
	set := newSortedSet()
	for _, entry := range compact.entries {
		set.add(compact.member(entry), entry.score)
	}
	return set
}
