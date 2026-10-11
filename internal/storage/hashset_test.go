package storage

import (
	"errors"
	"sort"
	"testing"
	"time"
)

func TestStoreListPopVariants(t *testing.T) {
	t.Run("RightPop returns tail and deletes empty list", func(t *testing.T) {
		store := NewStore()
		if _, _, err := store.RightPush("jobs", [][]byte{[]byte("a"), []byte("b")}); err != nil {
			t.Fatalf("RightPush() error = %v", err)
		}

		value, ok, err := store.RightPop("jobs")
		if err != nil {
			t.Fatalf("RightPop() error = %v", err)
		}
		if !ok || string(value) != "b" {
			t.Fatalf("RightPop() = (%q, %v), want (%q, true)", string(value), ok, "b")
		}

		if _, _, err := store.RightPop("jobs"); err != nil {
			t.Fatalf("RightPop() error = %v", err)
		}
		_, ok, err = store.RightPop("jobs")
		if err != nil || ok {
			t.Fatalf("RightPop() on empty = (%v, %v), want (false, nil)", ok, err)
		}
		if got := store.Len(); got != 0 {
			t.Fatalf("Len() after draining = %d, want 0", got)
		}
	})

	t.Run("LeftPopN returns up to count head items", func(t *testing.T) {
		store := NewStore()
		if _, _, err := store.RightPush("jobs", [][]byte{[]byte("a"), []byte("b"), []byte("c")}); err != nil {
			t.Fatalf("RightPush() error = %v", err)
		}

		values, ok, err := store.LeftPopN("jobs", 2)
		if err != nil || !ok {
			t.Fatalf("LeftPopN() = (%v, %v, %v), want (_, true, nil)", values, ok, err)
		}
		if len(values) != 2 || string(values[0]) != "a" || string(values[1]) != "b" {
			t.Fatalf("LeftPopN() values = [%q, %q], want [a, b]", string(values[0]), string(values[1]))
		}

		values, ok, err = store.LeftPopN("jobs", 10)
		if err != nil || !ok {
			t.Fatalf("LeftPopN() = (_, %v, %v), want (true, nil)", ok, err)
		}
		if len(values) != 1 || string(values[0]) != "c" {
			t.Fatalf("LeftPopN() drained = %v, want [c]", values)
		}
		if got := store.Len(); got != 0 {
			t.Fatalf("Len() = %d, want 0", got)
		}
	})

	t.Run("RightPopN returns tail items in right-to-left order", func(t *testing.T) {
		store := NewStore()
		if _, _, err := store.RightPush("jobs", [][]byte{[]byte("a"), []byte("b"), []byte("c")}); err != nil {
			t.Fatalf("RightPush() error = %v", err)
		}

		values, ok, err := store.RightPopN("jobs", 2)
		if err != nil || !ok {
			t.Fatalf("RightPopN() = (_, %v, %v), want (true, nil)", ok, err)
		}
		if len(values) != 2 || string(values[0]) != "c" || string(values[1]) != "b" {
			t.Fatalf("RightPopN() values = [%q, %q], want [c, b]", string(values[0]), string(values[1]))
		}
	})

	t.Run("LeftPopN rejects negative count", func(t *testing.T) {
		store := NewStore()
		if _, _, err := store.LeftPopN("jobs", -1); !errors.Is(err, ErrSyntax) {
			t.Fatalf("LeftPopN() error = %v, want ErrSyntax", err)
		}
	})

	t.Run("Pop on wrong type returns WRONGTYPE", func(t *testing.T) {
		store := NewStore()
		_, _ = store.Set("plain", []byte("x"), 0)

		if _, _, err := store.LeftPop("plain"); !errors.Is(err, ErrWrongType) {
			t.Fatalf("LeftPop() error = %v, want ErrWrongType", err)
		}
		if _, _, err := store.RightPop("plain"); !errors.Is(err, ErrWrongType) {
			t.Fatalf("RightPop() error = %v, want ErrWrongType", err)
		}
		if _, _, err := store.LeftPopN("plain", 1); !errors.Is(err, ErrWrongType) {
			t.Fatalf("LeftPopN() error = %v, want ErrWrongType", err)
		}
	})
}

func TestStoreHash(t *testing.T) {
	t.Run("HSet then HGet round trip", func(t *testing.T) {
		store := NewStore()

		added, _, err := store.HSet("h", []HashFieldValue{
			{Field: "f1", Value: []byte("v1")},
			{Field: "f2", Value: []byte("v2")},
		})
		if err != nil {
			t.Fatalf("HSet() error = %v", err)
		}
		if added != 2 {
			t.Fatalf("HSet() added = %d, want 2", added)
		}

		got, ok, err := store.HGet("h", "f1")
		if err != nil || !ok || string(got) != "v1" {
			t.Fatalf("HGet(f1) = (%q, %v, %v)", string(got), ok, err)
		}

		// Update existing
		added, _, err = store.HSet("h", []HashFieldValue{{Field: "f1", Value: []byte("v1b")}})
		if err != nil {
			t.Fatalf("HSet() error = %v", err)
		}
		if added != 0 {
			t.Fatalf("HSet() overwrite added = %d, want 0", added)
		}

		got, ok, err = store.HGet("h", "f1")
		if err != nil || !ok || string(got) != "v1b" {
			t.Fatalf("HGet(f1) after update = (%q, %v, %v)", string(got), ok, err)
		}
	})

	t.Run("HGet returns defensive copy", func(t *testing.T) {
		store := NewStore()
		if _, _, err := store.HSet("h", []HashFieldValue{{Field: "f", Value: []byte("v")}}); err != nil {
			t.Fatalf("HSet() error = %v", err)
		}

		got, ok, err := store.HGet("h", "f")
		if err != nil || !ok {
			t.Fatalf("HGet() = (_, %v, %v)", ok, err)
		}
		got[0] = 'X'

		again, ok, err := store.HGet("h", "f")
		if err != nil || !ok || string(again) != "v" {
			t.Fatalf("HGet() after mutation = (%q, %v, %v)", string(again), ok, err)
		}
	})

	t.Run("HDel removes fields and drops empty hash", func(t *testing.T) {
		store := NewStore()
		if _, _, err := store.HSet("h", []HashFieldValue{
			{Field: "a", Value: []byte("1")},
			{Field: "b", Value: []byte("2")},
		}); err != nil {
			t.Fatalf("HSet() error = %v", err)
		}

		removed, err := store.HDel("h", []string{"a", "missing"})
		if err != nil {
			t.Fatalf("HDel() error = %v", err)
		}
		if removed != 1 {
			t.Fatalf("HDel() = %d, want 1", removed)
		}

		removed, err = store.HDel("h", []string{"b"})
		if err != nil || removed != 1 {
			t.Fatalf("HDel() = (%d, %v)", removed, err)
		}
		if got := store.Len(); got != 0 {
			t.Fatalf("Len() = %d, want 0 after empty hash drop", got)
		}
	})

	t.Run("HGetAll returns all pairs (order-independent)", func(t *testing.T) {
		store := NewStore()
		if _, _, err := store.HSet("h", []HashFieldValue{
			{Field: "a", Value: []byte("1")},
			{Field: "b", Value: []byte("2")},
			{Field: "c", Value: []byte("3")},
		}); err != nil {
			t.Fatalf("HSet() error = %v", err)
		}

		entries, err := store.HGetAll("h")
		if err != nil {
			t.Fatalf("HGetAll() error = %v", err)
		}
		sort.Slice(entries, func(i, j int) bool { return entries[i].Field < entries[j].Field })
		if len(entries) != 3 {
			t.Fatalf("HGetAll() len = %d, want 3", len(entries))
		}
		if entries[0].Field != "a" || string(entries[0].Value) != "1" {
			t.Fatalf("HGetAll() entries[0] = %+v", entries[0])
		}
	})

	t.Run("Hash ops on wrong type return WRONGTYPE", func(t *testing.T) {
		store := NewStore()
		_, _ = store.Set("plain", []byte("x"), 0)

		if _, _, err := store.HSet("plain", []HashFieldValue{{Field: "f", Value: []byte("v")}}); !errors.Is(err, ErrWrongType) {
			t.Fatalf("HSet() error = %v, want ErrWrongType", err)
		}
		if _, _, err := store.HGet("plain", "f"); !errors.Is(err, ErrWrongType) {
			t.Fatalf("HGet() error = %v, want ErrWrongType", err)
		}
		if _, err := store.HDel("plain", []string{"f"}); !errors.Is(err, ErrWrongType) {
			t.Fatalf("HDel() error = %v, want ErrWrongType", err)
		}
		if _, err := store.HGetAll("plain"); !errors.Is(err, ErrWrongType) {
			t.Fatalf("HGetAll() error = %v, want ErrWrongType", err)
		}
	})

	t.Run("HSet empty pairs returns ErrSyntax", func(t *testing.T) {
		store := NewStore()
		if _, _, err := store.HSet("h", nil); !errors.Is(err, ErrSyntax) {
			t.Fatalf("HSet() error = %v, want ErrSyntax", err)
		}
	})

	t.Run("Hash passively expires", func(t *testing.T) {
		store, _ := newStoreWithManualClock()
		if _, _, err := store.HSet("h", []HashFieldValue{{Field: "f", Value: []byte("v")}}); err != nil {
			t.Fatalf("HSet() error = %v", err)
		}
		if ok := store.expireKeyForTest("h", store.deadlineAfter(-time.Second)); !ok {
			t.Fatal("expireKeyForTest() ok = false, want true")
		}

		if _, ok, err := store.HGet("h", "f"); err != nil || ok {
			t.Fatalf("HGet() after expiry = (%v, %v)", ok, err)
		}
	})

	t.Run("Small hash uses compact encoding across commands and snapshots", func(t *testing.T) {
		store := NewStore()
		added, _, err := store.HSet("h", []HashFieldValue{
			{Field: "a", Value: []byte("1")},
			{Field: "b", Value: []byte("2")},
		})
		if err != nil {
			t.Fatalf("HSet() error = %v", err)
		}
		if added != 2 {
			t.Fatalf("HSet() added = %d, want 2", added)
		}
		stored := store.valueObjectForTest("h")
		if stored == nil || stored.Kind != ValueKindHash || stored.HashEncoding != ValueEncodingCompact || stored.CompactHash == nil {
			t.Fatalf("stored hash = %#v, want compact hash", stored)
		}

		added, _, err = store.HSet("h", []HashFieldValue{{Field: "a", Value: []byte("one")}})
		if err != nil || added != 0 {
			t.Fatalf("HSet() update = (%d, %v), want (0, nil)", added, err)
		}
		got, ok, err := store.HGet("h", "a")
		if err != nil || !ok || string(got) != "one" {
			t.Fatalf("HGet() = (%q, %v, %v), want (one, true, nil)", string(got), ok, err)
		}
		removed, err := store.HDel("h", []string{"b"})
		if err != nil || removed != 1 {
			t.Fatalf("HDel() = (%d, %v), want (1, nil)", removed, err)
		}
		entries, err := store.HGetAll("h")
		if err != nil {
			t.Fatalf("HGetAll() error = %v", err)
		}
		if len(entries) != 1 || entries[0].Field != "a" || string(entries[0].Value) != "one" {
			t.Fatalf("HGetAll() = %#v, want a/one", entries)
		}

		snapshot, stats := store.SnapshotAll()
		if stats.TotalKeys != 1 || stats.ExportedKeys != 1 {
			t.Fatalf("SnapshotAll() stats = %+v, want total/exported 1", stats)
		}
		if len(snapshot) != 1 || snapshot[0].Kind != ValueKindHash || len(snapshot[0].Hash) != 1 {
			t.Fatalf("SnapshotAll() = %#v, want one logical hash", snapshot)
		}
		if snapshot[0].Hash[0].Field != "a" || string(snapshot[0].Hash[0].Value) != "one" {
			t.Fatalf("SnapshotAll() hash = %#v, want a/one", snapshot[0].Hash)
		}
	})

	t.Run("Hash creation chooses compact encoding by distinct fields", func(t *testing.T) {
		store := NewStore()
		pairs := make([]HashFieldValue, compactHashMaxEntries+1)
		for i := range pairs {
			pairs[i] = HashFieldValue{Field: "same", Value: []byte("value")}
		}

		if _, _, err := store.HSet("h", pairs); err != nil {
			t.Fatalf("HSet() error = %v", err)
		}
		stored := store.valueObjectForTest("h")
		if stored == nil || stored.HashEncoding != ValueEncodingCompact {
			t.Fatalf("stored hash = %#v, want compact hash for one distinct field", stored)
		}
	})

	t.Run("Compact hash upgrades when entry count exceeds threshold", func(t *testing.T) {
		store, _ := newStoreWithManualClock()
		for i := 0; i < compactHashMaxEntries; i++ {
			if _, _, err := store.HSet("h", []HashFieldValue{{Field: string(rune('a' + i)), Value: []byte("v")}}); err != nil {
				t.Fatalf("HSet() seed error = %v", err)
			}
		}
		expiresAt := store.deadlineAfter(time.Hour)
		if ok := store.expireKeyForTest("h", expiresAt); !ok {
			t.Fatal("expireKeyForTest() ok = false, want true")
		}

		added, _, err := store.HSet("h", []HashFieldValue{{Field: "overflow", Value: []byte("v")}})
		if err != nil || added != 1 {
			t.Fatalf("HSet() overflow = (%d, %v), want (1, nil)", added, err)
		}
		stored := store.valueObjectForTest("h")
		if stored == nil || stored.Kind != ValueKindHash || stored.HashEncoding != ValueEncodingGeneral || stored.Hash == nil {
			t.Fatalf("stored hash = %#v, want upgraded general hash", stored)
		}
		if stored.ExpiresAt != expiresAt {
			t.Fatalf("upgraded hash ExpiresAt = %d, want %d", stored.ExpiresAt, expiresAt)
		}
		got, ok, err := store.HGet("h", "a")
		if err != nil || !ok || string(got) != "v" {
			t.Fatalf("HGet(a) after upgrade = (%q, %v, %v), want (v, true, nil)", string(got), ok, err)
		}
		got, ok, err = store.HGet("h", "overflow")
		if err != nil || !ok || string(got) != "v" {
			t.Fatalf("HGet(overflow) after upgrade = (%q, %v, %v), want (v, true, nil)", string(got), ok, err)
		}
	})

	t.Run("Compact hash upgrades when string length exceeds threshold", func(t *testing.T) {
		longText := make([]byte, compactHashMaxStringSize+1)
		for i := range longText {
			longText[i] = 'x'
		}

		tests := []struct {
			name      string
			seed      HashFieldValue
			update    HashFieldValue
			wantAdded int64
			wantField string
			wantLen   int
		}{
			{
				name:      "field length",
				seed:      HashFieldValue{Field: "f", Value: []byte("v")},
				update:    HashFieldValue{Field: string(longText), Value: []byte("v")},
				wantAdded: 1,
				wantField: string(longText),
				wantLen:   1,
			},
			{
				name:      "value length",
				seed:      HashFieldValue{Field: "f", Value: []byte("v")},
				update:    HashFieldValue{Field: "f", Value: longText},
				wantAdded: 0,
				wantField: "f",
				wantLen:   len(longText),
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				store := NewStore()
				if _, _, err := store.HSet("h", []HashFieldValue{tt.seed}); err != nil {
					t.Fatalf("HSet() seed error = %v", err)
				}

				added, _, err := store.HSet("h", []HashFieldValue{tt.update})
				if err != nil || added != tt.wantAdded {
					t.Fatalf("HSet() long update = (%d, %v), want (%d, nil)", added, err, tt.wantAdded)
				}
				stored := store.valueObjectForTest("h")
				if stored == nil || stored.HashEncoding != ValueEncodingGeneral || stored.Hash == nil {
					t.Fatalf("stored hash = %#v, want upgraded general hash", stored)
				}
				got, ok, err := store.HGet("h", tt.wantField)
				if err != nil || !ok || len(got) != tt.wantLen {
					t.Fatalf("HGet(%q) after long update = (len %d, %v, %v), want len %d", tt.wantField, len(got), ok, err, tt.wantLen)
				}
			})
		}
	})
}

func TestStoreSet(t *testing.T) {
	t.Run("SAdd returns newly added count", func(t *testing.T) {
		store := NewStore()
		added, _, err := store.SAdd("s", [][]byte{[]byte("a"), []byte("b"), []byte("a")})
		if err != nil {
			t.Fatalf("SAdd() error = %v", err)
		}
		if added != 2 {
			t.Fatalf("SAdd() = %d, want 2", added)
		}

		added, _, err = store.SAdd("s", [][]byte{[]byte("b"), []byte("c")})
		if err != nil || added != 1 {
			t.Fatalf("SAdd() second = (%d, %v)", added, err)
		}
	})

	t.Run("SIsMember reports membership", func(t *testing.T) {
		store := NewStore()
		if _, _, err := store.SAdd("s", [][]byte{[]byte("x")}); err != nil {
			t.Fatalf("SAdd() error = %v", err)
		}

		ok, err := store.SIsMember("s", []byte("x"))
		if err != nil || !ok {
			t.Fatalf("SIsMember(x) = (%v, %v)", ok, err)
		}
		ok, err = store.SIsMember("s", []byte("y"))
		if err != nil || ok {
			t.Fatalf("SIsMember(y) = (%v, %v)", ok, err)
		}
		ok, err = store.SIsMember("missing", []byte("x"))
		if err != nil || ok {
			t.Fatalf("SIsMember(missing) = (%v, %v)", ok, err)
		}
	})

	t.Run("SRem drops empty set", func(t *testing.T) {
		store := NewStore()
		if _, _, err := store.SAdd("s", [][]byte{[]byte("x"), []byte("y")}); err != nil {
			t.Fatalf("SAdd() error = %v", err)
		}

		removed, err := store.SRem("s", [][]byte{[]byte("x"), []byte("missing")})
		if err != nil || removed != 1 {
			t.Fatalf("SRem() = (%d, %v), want (1, nil)", removed, err)
		}

		removed, err = store.SRem("s", [][]byte{[]byte("y")})
		if err != nil || removed != 1 {
			t.Fatalf("SRem() = (%d, %v)", removed, err)
		}
		if got := store.Len(); got != 0 {
			t.Fatalf("Len() = %d, want 0", got)
		}
	})

	t.Run("SMembers returns every member (order-independent)", func(t *testing.T) {
		store := NewStore()
		if _, _, err := store.SAdd("s", [][]byte{[]byte("a"), []byte("b"), []byte("c")}); err != nil {
			t.Fatalf("SAdd() error = %v", err)
		}

		members, err := store.SMembers("s")
		if err != nil {
			t.Fatalf("SMembers() error = %v", err)
		}
		got := make([]string, 0, len(members))
		for _, m := range members {
			got = append(got, string(m))
		}
		sort.Strings(got)
		want := []string{"a", "b", "c"}
		for i, v := range want {
			if got[i] != v {
				t.Fatalf("SMembers() = %v, want %v", got, want)
			}
		}
	})

	t.Run("Set ops on wrong type return WRONGTYPE", func(t *testing.T) {
		store := NewStore()
		_, _ = store.Set("plain", []byte("x"), 0)

		if _, _, err := store.SAdd("plain", [][]byte{[]byte("m")}); !errors.Is(err, ErrWrongType) {
			t.Fatalf("SAdd() error = %v, want ErrWrongType", err)
		}
		if _, err := store.SIsMember("plain", []byte("m")); !errors.Is(err, ErrWrongType) {
			t.Fatalf("SIsMember() error = %v, want ErrWrongType", err)
		}
		if _, err := store.SRem("plain", [][]byte{[]byte("m")}); !errors.Is(err, ErrWrongType) {
			t.Fatalf("SRem() error = %v, want ErrWrongType", err)
		}
		if _, err := store.SMembers("plain"); !errors.Is(err, ErrWrongType) {
			t.Fatalf("SMembers() error = %v, want ErrWrongType", err)
		}
	})

	t.Run("Integer-only set uses intset encoding across commands and snapshots", func(t *testing.T) {
		store := NewStore()
		added, _, err := store.SAdd("s", [][]byte{[]byte("2"), []byte("1"), []byte("2"), []byte("-5")})
		if err != nil {
			t.Fatalf("SAdd() error = %v", err)
		}
		if added != 3 {
			t.Fatalf("SAdd() added = %d, want 3", added)
		}
		stored := store.valueObjectForTest("s")
		if stored == nil || stored.Kind != ValueKindSet || stored.SetEncoding != ValueEncodingCompact || stored.IntSet == nil {
			t.Fatalf("stored set = %#v, want compact intset", stored)
		}

		exists, err := store.SIsMember("s", []byte("1"))
		if err != nil || !exists {
			t.Fatalf("SIsMember(1) = (%v, %v), want (true, nil)", exists, err)
		}
		exists, err = store.SIsMember("s", []byte("3"))
		if err != nil || exists {
			t.Fatalf("SIsMember(3) = (%v, %v), want (false, nil)", exists, err)
		}

		removed, err := store.SRem("s", [][]byte{[]byte("2"), []byte("missing")})
		if err != nil || removed != 1 {
			t.Fatalf("SRem() = (%d, %v), want (1, nil)", removed, err)
		}
		members, err := store.SMembers("s")
		if err != nil {
			t.Fatalf("SMembers() error = %v", err)
		}
		got := sortedStrings(members)
		want := []string{"-5", "1"}
		if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
			t.Fatalf("SMembers() = %v, want %v", got, want)
		}

		snapshot, stats := store.SnapshotAll()
		if stats.TotalKeys != 1 || stats.ExportedKeys != 1 {
			t.Fatalf("SnapshotAll() stats = %+v, want total/exported 1", stats)
		}
		if len(snapshot) != 1 || snapshot[0].Kind != ValueKindSet {
			t.Fatalf("SnapshotAll() = %#v, want one logical set", snapshot)
		}
		got = sortedStrings(snapshot[0].Set)
		if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
			t.Fatalf("SnapshotAll().Set = %v, want %v", got, want)
		}
	})

	t.Run("Non-integer and mixed sets use general encoding", func(t *testing.T) {
		store := NewStore()
		if _, _, err := store.SAdd("strings", [][]byte{[]byte("a"), []byte("b")}); err != nil {
			t.Fatalf("SAdd(strings) error = %v", err)
		}
		if _, _, err := store.SAdd("mixed", [][]byte{[]byte("1"), []byte("b")}); err != nil {
			t.Fatalf("SAdd(mixed) error = %v", err)
		}
		if _, _, err := store.SAdd("noncanonical", [][]byte{[]byte("01")}); err != nil {
			t.Fatalf("SAdd(noncanonical) error = %v", err)
		}

		for _, key := range []string{"strings", "mixed", "noncanonical"} {
			stored := store.valueObjectForTest(key)
			if stored == nil || stored.SetEncoding != ValueEncodingGeneral || stored.Set == nil {
				t.Fatalf("stored set %q = %#v, want general set", key, stored)
			}
		}
	})

	t.Run("SRem deletes empty intset", func(t *testing.T) {
		store := NewStore()
		if _, _, err := store.SAdd("s", [][]byte{[]byte("1")}); err != nil {
			t.Fatalf("SAdd() error = %v", err)
		}
		removed, err := store.SRem("s", [][]byte{[]byte("1")})
		if err != nil || removed != 1 {
			t.Fatalf("SRem() = (%d, %v), want (1, nil)", removed, err)
		}
		if got := store.Len(); got != 0 {
			t.Fatalf("Len() = %d, want 0", got)
		}
	})

	t.Run("Integer set upgrades when non-integer member is added", func(t *testing.T) {
		store, _ := newStoreWithManualClock()
		if _, _, err := store.SAdd("s", [][]byte{[]byte("1"), []byte("2")}); err != nil {
			t.Fatalf("SAdd() seed error = %v", err)
		}
		expiresAt := store.deadlineAfter(time.Hour)
		if ok := store.expireKeyForTest("s", expiresAt); !ok {
			t.Fatal("expireKeyForTest() ok = false, want true")
		}

		added, _, err := store.SAdd("s", [][]byte{[]byte("2"), []byte("three")})
		if err != nil || added != 1 {
			t.Fatalf("SAdd() mixed update = (%d, %v), want (1, nil)", added, err)
		}
		stored := store.valueObjectForTest("s")
		if stored == nil || stored.Kind != ValueKindSet || stored.SetEncoding != ValueEncodingGeneral || stored.Set == nil {
			t.Fatalf("stored set = %#v, want upgraded general set", stored)
		}
		if stored.ExpiresAt != expiresAt {
			t.Fatalf("upgraded set ExpiresAt = %d, want %d", stored.ExpiresAt, expiresAt)
		}
		for _, member := range []string{"1", "2", "three"} {
			exists, err := store.SIsMember("s", []byte(member))
			if err != nil || !exists {
				t.Fatalf("SIsMember(%q) after upgrade = (%v, %v), want (true, nil)", member, exists, err)
			}
		}
	})
}

func sortedStrings(values [][]byte) []string {
	items := make([]string, 0, len(values))
	for _, value := range values {
		items = append(items, string(value))
	}
	sort.Strings(items)
	return items
}
