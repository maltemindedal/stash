package storage

import (
	"strconv"
	"testing"
)

// TestSetUpgradesPastIntSetThreshold verifies a growing integer-only set
// upgrades from the compact sorted-slice encoding to the hashtable once it
// exceeds intSetMaxEntries, so large sets stop paying the O(n) per-member cost.
func TestSetUpgradesPastIntSetThreshold(t *testing.T) {
	total := intSetMaxEntries + 10
	members := make([][]byte, 0, total)
	for i := 0; i < total; i++ {
		members = append(members, []byte(strconv.Itoa(i)))
	}

	// Exactly at the threshold: still compact.
	store := NewStore()
	if _, err := store.SAdd(nil, "s", members[:intSetMaxEntries]); err != nil {
		t.Fatalf("SAdd() error = %v", err)
	}
	if got := store.encodingForTest("s"); got != "set:compact" {
		t.Fatalf("encodingForTest(s) = %q at the threshold, want set:compact", got)
	}

	// Adding past the threshold upgrades to the general encoding.
	if _, err := store.SAdd(nil, "s", members[intSetMaxEntries:]); err != nil {
		t.Fatalf("SAdd() error = %v", err)
	}
	if got := store.encodingForTest("s"); got != "set:general" {
		t.Fatalf("encodingForTest(s) = %q past the threshold, want set:general", got)
	}

	got, err := store.SMembers("s")
	if err != nil {
		t.Fatalf("SMembers() error = %v", err)
	}
	if len(got) != total {
		t.Fatalf("the set has %d members, want %d (no members lost on upgrade)", len(got), total)
	}
}

// TestLargeIntSetInitialBatchUsesGeneralEncoding verifies an initial SADD larger
// than the threshold skips the compact encoding entirely.
func TestLargeIntSetInitialBatchUsesGeneralEncoding(t *testing.T) {
	total := intSetMaxEntries + 1
	members := make([][]byte, 0, total)
	for i := 0; i < total; i++ {
		members = append(members, []byte(strconv.Itoa(i)))
	}

	store := NewStore()
	if _, err := store.SAdd(nil, "s", members); err != nil {
		t.Fatalf("SAdd() error = %v", err)
	}
	if got := store.encodingForTest("s"); got != "set:general" {
		t.Fatalf("encodingForTest(s) = %q for an oversized first batch, want set:general", got)
	}
}
