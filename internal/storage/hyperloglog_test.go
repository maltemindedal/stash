package storage

import (
	"errors"
	"fmt"
	"math"
	"testing"
)

func pfAddForTest(t *testing.T, store *Store, key string, elements [][]byte) (int64, error) {
	t.Helper()

	changed, _, err := store.PFAdd(key, elements)
	return changed, err
}

func TestStoreHyperLogLogBehavior(t *testing.T) {
	tests := []struct {
		name string
		run  func(*testing.T, *Store)
	}{
		{
			name: "PFAdd creates a fixed-size HyperLogLog value and reports changes",
			run: func(t *testing.T, store *Store) {
				t.Helper()

				changed, err := pfAddForTest(t, store, "visitors", [][]byte{[]byte("alice")})
				if err != nil {
					t.Fatalf("PFAdd() error = %v", err)
				}
				if changed != 1 {
					t.Fatalf("PFAdd() changed = %d, want 1", changed)
				}

				raw, ok, err := store.Get("visitors")
				if err != nil || !ok {
					t.Fatalf("Get() = %v, %v, want value", ok, err)
				}
				if len(raw) != hllValueSize {
					t.Fatalf("len(value) = %d, want %d", len(raw), hllValueSize)
				}
			},
		},
		{
			name: "PFAdd returns zero for repeated elements",
			run: func(t *testing.T, store *Store) {
				t.Helper()

				if _, err := pfAddForTest(t, store, "visitors", [][]byte{[]byte("alice"), []byte("bob")}); err != nil {
					t.Fatalf("PFAdd() first error = %v", err)
				}

				changed, err := pfAddForTest(t, store, "visitors", [][]byte{[]byte("alice"), []byte("bob")})
				if err != nil {
					t.Fatalf("PFAdd() second error = %v", err)
				}
				if changed != 0 {
					t.Fatalf("PFAdd() repeated changed = %d, want 0", changed)
				}
			},
		},
		{
			name: "PFAdd with no elements creates the key once",
			run: func(t *testing.T, store *Store) {
				t.Helper()

				changed, err := pfAddForTest(t, store, "visitors", nil)
				if err != nil {
					t.Fatalf("PFAdd() error = %v", err)
				}
				if changed != 1 {
					t.Fatalf("PFAdd() on missing key changed = %d, want 1", changed)
				}

				changed, err = pfAddForTest(t, store, "visitors", nil)
				if err != nil {
					t.Fatalf("PFAdd() second error = %v", err)
				}
				if changed != 0 {
					t.Fatalf("PFAdd() on existing key changed = %d, want 0", changed)
				}

				count, err := store.PFCount([]string{"visitors"})
				if err != nil {
					t.Fatalf("PFCount() error = %v", err)
				}
				if count != 0 {
					t.Fatalf("PFCount() = %d, want 0", count)
				}
			},
		},
		{
			name: "PFCount returns zero for missing keys",
			run: func(t *testing.T, store *Store) {
				t.Helper()

				count, err := store.PFCount([]string{"missing", "also-missing"})
				if err != nil {
					t.Fatalf("PFCount() error = %v", err)
				}
				if count != 0 {
					t.Fatalf("PFCount() = %d, want 0", count)
				}
			},
		},
		{
			name: "PFCount counts small cardinalities exactly",
			run: func(t *testing.T, store *Store) {
				t.Helper()

				elements := [][]byte{[]byte("alice"), []byte("bob"), []byte("carol")}
				if _, err := pfAddForTest(t, store, "visitors", elements); err != nil {
					t.Fatalf("PFAdd() error = %v", err)
				}

				count, err := store.PFCount([]string{"visitors"})
				if err != nil {
					t.Fatalf("PFCount() error = %v", err)
				}
				if count != 3 {
					t.Fatalf("PFCount() = %d, want 3", count)
				}
			},
		},
		{
			name: "PFCount approximates large cardinalities within tolerance",
			run: func(t *testing.T, store *Store) {
				t.Helper()

				const unique = 10000
				elements := make([][]byte, 0, unique)
				for i := 0; i < unique; i++ {
					elements = append(elements, fmt.Appendf(nil, "element-%d", i))
				}
				if _, err := pfAddForTest(t, store, "visitors", elements); err != nil {
					t.Fatalf("PFAdd() error = %v", err)
				}

				count, err := store.PFCount([]string{"visitors"})
				if err != nil {
					t.Fatalf("PFCount() error = %v", err)
				}
				if count < unique*95/100 || count > unique*105/100 {
					t.Fatalf("PFCount() = %d, want within 5%% of %d", count, unique)
				}
			},
		},
		{
			name: "PFCount unions registers across multiple keys",
			run: func(t *testing.T, store *Store) {
				t.Helper()

				first := [][]byte{[]byte("alice"), []byte("bob")}
				second := [][]byte{[]byte("bob"), []byte("carol"), []byte("dave")}
				if _, err := pfAddForTest(t, store, "morning", first); err != nil {
					t.Fatalf("PFAdd(morning) error = %v", err)
				}
				if _, err := pfAddForTest(t, store, "evening", second); err != nil {
					t.Fatalf("PFAdd(evening) error = %v", err)
				}

				count, err := store.PFCount([]string{"morning", "evening", "missing"})
				if err != nil {
					t.Fatalf("PFCount() error = %v", err)
				}
				if count != 4 {
					t.Fatalf("PFCount() union = %d, want 4", count)
				}
			},
		},
		{
			name: "PFAdd and PFCount reject non-string values with wrong type",
			run: func(t *testing.T, store *Store) {
				t.Helper()
				if _, _, err := store.LeftPush("numbers", [][]byte{[]byte("one")}); err != nil {
					t.Fatalf("LeftPush() error = %v", err)
				}

				if _, err := pfAddForTest(t, store, "numbers", [][]byte{[]byte("alice")}); !errors.Is(err, ErrWrongType) {
					t.Fatalf("PFAdd() error = %v, want ErrWrongType", err)
				}
				if _, err := store.PFCount([]string{"numbers"}); !errors.Is(err, ErrWrongType) {
					t.Fatalf("PFCount() error = %v, want ErrWrongType", err)
				}
			},
		},
		{
			name: "PFAdd and PFCount reject plain string values as invalid HyperLogLogs",
			run: func(t *testing.T, store *Store) {
				t.Helper()
				_, _ = store.Set("greeting", []byte("hello"), 0)

				if _, err := pfAddForTest(t, store, "greeting", [][]byte{[]byte("alice")}); !errors.Is(err, ErrNotHyperLogLog) {
					t.Fatalf("PFAdd() error = %v, want ErrNotHyperLogLog", err)
				}
				if _, err := store.PFCount([]string{"greeting"}); !errors.Is(err, ErrNotHyperLogLog) {
					t.Fatalf("PFCount() error = %v, want ErrNotHyperLogLog", err)
				}
			},
		},
		{
			name: "PFAdd and PFCount reject out-of-range register values",
			run: func(t *testing.T, store *Store) {
				t.Helper()

				payload := make([]byte, hllValueSize)
				writeHyperLogLogHeader(payload)
				for i := hllHeaderSize; i < len(payload); i++ {
					payload[i] = 0xFF
				}
				_, _ = store.Set("forged", payload, 0)

				if _, err := pfAddForTest(t, store, "forged", [][]byte{[]byte("alice")}); !errors.Is(err, ErrNotHyperLogLog) {
					t.Fatalf("PFAdd() error = %v, want ErrNotHyperLogLog", err)
				}
				if _, err := store.PFCount([]string{"forged"}); !errors.Is(err, ErrNotHyperLogLog) {
					t.Fatalf("PFCount() error = %v, want ErrNotHyperLogLog", err)
				}
			},
		},
		{
			name: "PFCount clamps saturated estimates instead of overflowing",
			run: func(t *testing.T, store *Store) {
				t.Helper()

				payload := make([]byte, hllValueSize)
				writeHyperLogLogHeader(payload)
				for i := hllHeaderSize; i < len(payload); i++ {
					payload[i] = hllMaxRank
				}
				_, _ = store.Set("saturated", payload, 0)

				count, err := store.PFCount([]string{"saturated"})
				if err != nil {
					t.Fatalf("PFCount() error = %v", err)
				}
				if count != math.MaxInt64 {
					t.Fatalf("PFCount() = %d, want math.MaxInt64", count)
				}
			},
		},
		{
			name: "PFAdd recreates expired HyperLogLog keys",
			run: func(t *testing.T, store *Store) {
				t.Helper()

				if _, err := pfAddForTest(t, store, "visitors", [][]byte{[]byte("alice")}); err != nil {
					t.Fatalf("PFAdd() error = %v", err)
				}
				if !store.expireKeyForTest("visitors", 1) {
					t.Fatal("expireKeyForTest() = false, want true")
				}

				changed, err := pfAddForTest(t, store, "visitors", [][]byte{[]byte("bob")})
				if err != nil {
					t.Fatalf("PFAdd() after expiry error = %v", err)
				}
				if changed != 1 {
					t.Fatalf("PFAdd() after expiry changed = %d, want 1", changed)
				}

				count, err := store.PFCount([]string{"visitors"})
				if err != nil {
					t.Fatalf("PFCount() error = %v", err)
				}
				if count != 1 {
					t.Fatalf("PFCount() = %d, want 1", count)
				}
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			store := NewStore()
			tt.run(t, store)
		})
	}
}
