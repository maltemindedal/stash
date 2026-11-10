package storage

import (
	"maps"
	"sort"
)

// hashValue is a hash in one of its two encodings. Exactly one of compact and
// table is set: a small hash of short fields and values is compact, and it
// becomes a hash table, for good, when an HSET outgrows that.
type hashValue struct {
	compact *CompactHash
	table   map[string][]byte
}

// hashOf returns the hash v holds, or ErrWrongType when v holds a value of
// another kind.
func hashOf(v *ValueObject) (*hashValue, error) {
	if hash, ok := v.data.(*hashValue); ok {
		return hash, nil
	}
	return nil, ErrWrongType
}

// newHashValueForPairs returns a hash of pairs, compact when they allow it. A
// field given twice keeps its last value. The hash owns copies of the values.
func newHashValueForPairs(pairs []HashFieldValue) hashValue {
	table := make(map[string][]byte, len(pairs))
	compactEligible := true
	for _, pair := range pairs {
		if len(pair.Field) > compactHashMaxStringSize || len(pair.Value) > compactHashMaxStringSize {
			compactEligible = false
		}
		table[pair.Field] = cloneBytes(pair.Value)
	}
	if !compactEligible || len(table) > compactHashMaxEntries {
		return hashValue{table: table}
	}

	compact := &CompactHash{entries: make([]compactHashEntry, 0, len(table))}
	for field, value := range table {
		compact.set(field, value)
	}
	return hashValue{compact: compact}
}

func (h *hashValue) len() int {
	if h.compact != nil {
		return h.compact.len()
	}
	return len(h.table)
}

// get returns the stored bytes of field, which the caller copies before it
// releases the lock.
func (h *hashValue) get(field string) ([]byte, bool) {
	if h.compact != nil {
		return h.compact.get(field)
	}
	value, ok := h.table[field]
	return value, ok
}

// set assigns pairs and returns how many fields are new. A compact hash that
// the pairs outgrow becomes a hash table first.
func (h *hashValue) set(pairs []HashFieldValue) int64 {
	if h.compact != nil && !compactHashCanSet(h.compact, pairs) {
		table := make(map[string][]byte, h.compact.len())
		for _, entry := range h.compact.entries {
			table[h.compact.field(entry)] = cloneBytes(h.compact.value(entry))
		}
		h.compact, h.table = nil, table
	}

	added := int64(0)
	if h.compact != nil {
		for _, pair := range pairs {
			if h.compact.set(pair.Field, pair.Value) {
				added++
			}
		}
		return added
	}
	for _, pair := range pairs {
		if _, exists := h.table[pair.Field]; !exists {
			added++
		}
		h.table[pair.Field] = cloneBytes(pair.Value)
	}
	return added
}

// del removes fields and returns how many were in the hash.
func (h *hashValue) del(fields []string) int64 {
	removed := int64(0)
	if h.compact != nil {
		for _, field := range fields {
			if h.compact.del(field) {
				removed++
			}
		}
		return removed
	}
	for _, field := range fields {
		if _, exists := h.table[field]; exists {
			delete(h.table, field)
			removed++
		}
	}
	return removed
}

// entries returns every field with its stored bytes, in no particular order.
// The caller copies the values before it releases the lock.
func (h *hashValue) entries() []HashFieldValue {
	if h.compact != nil {
		return h.compact.all()
	}
	entries := make([]HashFieldValue, 0, len(h.table))
	for field, raw := range h.table {
		entries = append(entries, HashFieldValue{Field: field, Value: raw})
	}
	return entries
}

func (h *hashValue) kind() kindID { return kindHash }

func (h *hashValue) approxSize() int64 {
	size := approxCollectionOverhead
	if h.compact != nil {
		return size + int64(len(h.compact.entries))*approxHashEntryOverhead + int64(len(h.compact.arena))
	}
	size += int64(len(h.table)) * approxHashEntryOverhead
	for field, raw := range h.table {
		size += int64(len(field) + len(raw))
	}
	return size
}

// clone copies the hash table, or the compact entries and their arena.
func (h *hashValue) clone(header ValueObject) *ValueObject {
	return newKindValue(header, hashValue{compact: cloneCompactHash(h.compact), table: maps.Clone(h.table)})
}

func (h *hashValue) empty() bool { return h.len() == 0 }

func (h *hashValue) image(a *arena) kindImage {
	entries := h.entries()
	for i := range entries {
		entries[i].Value = a.bytes(entries[i].Value)
	}
	return a.hashes.hold(entries)
}

// hashImage is a hash in a keyspace image: its fields and values, in no order
// until the first recreate sorts them.
type hashImage []HashFieldValue

// recreate emits HSET commands of the field and value pairs in field order, so
// that the same hash always recreates with the same commands.
func (img *hashImage) recreate(key string, emit func(args [][]byte) error) error {
	entries := *img
	sort.Slice(entries, func(i, j int) bool { return entries[i].Field < entries[j].Field })
	return emitChunked(emit, "HSET", key, len(entries), 2, func(args [][]byte, i int) [][]byte {
		return append(args, []byte(entries[i].Field), entries[i].Value)
	})
}

// compactHashCanSet reports whether hash stays within the compact encoding's
// limits after pairs are set on it.
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

// HSet assigns one or more field/value pairs on the hash stored at key and
// returns the number of newly created fields. Existing fields are overwritten.
// When maxmemory is configured it first evicts keys to make room.
func (s *Store) HSet(changes Changes, key string, pairs []HashFieldValue) (int64, error) {
	if len(pairs) == 0 {
		return 0, ErrSyntax
	}

	return mutateKey(s, changes, key, kindHash, mayGrow, func(cur *ValueObject, _ int64) (*ValueObject, int64, error) {
		if cur == nil {
			created := newHashValueForPairs(pairs)
			return newKindValue(ValueObject{}, created), int64(created.len()), nil
		}
		hash, err := hashOf(cur)
		if err != nil {
			return nil, 0, err
		}
		return cur, hash.set(pairs), nil
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
		hash, err := hashOf(value)
		if err != nil {
			return hashField{}, err
		}
		raw, exists := hash.get(field)
		if !exists {
			return hashField{}, nil
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
func (s *Store) HDel(changes Changes, key string, fields []string) (int64, error) {
	if len(fields) == 0 {
		return 0, ErrSyntax
	}

	removed, err := mutateKey(s, changes, key, kindHash, shrinks, func(cur *ValueObject, _ int64) (*ValueObject, int64, error) {
		if cur == nil {
			return nil, 0, errUnchanged
		}
		hash, err := hashOf(cur)
		if err != nil {
			return nil, 0, err
		}
		removed := hash.del(fields)
		if removed == 0 {
			return cur, 0, errUnchanged
		}
		return cur, removed, nil
	})
	return removed, err
}

// HGetAll returns every field/value pair on the hash stored at key. Order is
// not guaranteed and mirrors Redis semantics.
func (s *Store) HGetAll(key string) ([]HashFieldValue, error) {
	entries, found, err := readKey(s, key, func(value *ValueObject) ([]HashFieldValue, error) {
		hash, err := hashOf(value)
		if err != nil {
			return nil, err
		}
		entries := hash.entries()
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
