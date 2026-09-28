package storage

import (
	"bytes"
	"fmt"
	"math/rand"
	"runtime"
	"testing"
)

// TestListOperationsMatchSliceModel drives every list operation with random
// sequences and checks the store against a plain slice after each step. The
// sequences push more than they pop, so lists grow past several reallocations,
// and they run with and without maxmemory accounting because a write takes a
// different path in each.
func TestListOperationsMatchSliceModel(t *testing.T) {
	for _, accounting := range []bool{false, true} {
		for seed := int64(0); seed < 150; seed++ {
			t.Run(fmt.Sprintf("accounting=%v/seed=%d", accounting, seed), func(t *testing.T) {
				rng := rand.New(rand.NewSource(seed))
				store := NewStore()
				if accounting {
					store.ConfigureMaxMemory(1<<40, 16)
				}
				var model [][]byte
				next := 0
				newValues := func(n int) [][]byte {
					values := make([][]byte, n)
					for i := range values {
						values[i] = []byte(fmt.Sprintf("v%d", next))
						next++
					}
					return values
				}

				for step := 0; step < 300; step++ {
					switch op := rng.Intn(10); {
					case op < 3: // LPUSH
						values := newValues(1 + rng.Intn(3))
						length, _, err := store.LeftPush("k", values)
						if err != nil {
							t.Fatalf("step %d: LeftPush error = %v", step, err)
						}
						for _, v := range values {
							model = append([][]byte{v}, model...)
						}
						if length != int64(len(model)) {
							t.Fatalf("step %d: LeftPush length = %d, want %d", step, length, len(model))
						}
					case op < 6: // RPUSH
						values := newValues(1 + rng.Intn(3))
						length, _, err := store.RightPush("k", values)
						if err != nil {
							t.Fatalf("step %d: RightPush error = %v", step, err)
						}
						model = append(model, values...)
						if length != int64(len(model)) {
							t.Fatalf("step %d: RightPush length = %d, want %d", step, length, len(model))
						}
					case op == 6: // LPOP
						got, ok, err := store.LeftPop("k")
						if err != nil {
							t.Fatalf("step %d: LeftPop error = %v", step, err)
						}
						if len(model) == 0 {
							if ok {
								t.Fatalf("step %d: LeftPop on an empty list returned %q", step, got)
							}
							break
						}
						if !ok || !bytes.Equal(got, model[0]) {
							t.Fatalf("step %d: LeftPop = (%q, %v), want %q", step, got, ok, model[0])
						}
						model = model[1:]
					case op == 7: // RPOP
						got, ok, err := store.RightPop("k")
						if err != nil {
							t.Fatalf("step %d: RightPop error = %v", step, err)
						}
						if len(model) == 0 {
							if ok {
								t.Fatalf("step %d: RightPop on an empty list returned %q", step, got)
							}
							break
						}
						if !ok || !bytes.Equal(got, model[len(model)-1]) {
							t.Fatalf("step %d: RightPop = (%q, %v), want %q", step, got, ok, model[len(model)-1])
						}
						model = model[:len(model)-1]
					case op == 8: // LPOP count
						count := int64(1 + rng.Intn(4))
						got, ok, err := store.LeftPopN("k", count)
						if err != nil {
							t.Fatalf("step %d: LeftPopN error = %v", step, err)
						}
						n := min(int(count), len(model))
						if ok != (len(model) > 0) || len(got) != n {
							t.Fatalf("step %d: LeftPopN(%d) = (%d values, %v), want %d values", step, count, len(got), ok, n)
						}
						for i := 0; i < n; i++ {
							if !bytes.Equal(got[i], model[i]) {
								t.Fatalf("step %d: LeftPopN value %d = %q, want %q", step, i, got[i], model[i])
							}
						}
						model = model[n:]
					default: // RPOP count
						count := int64(1 + rng.Intn(4))
						got, ok, err := store.RightPopN("k", count)
						if err != nil {
							t.Fatalf("step %d: RightPopN error = %v", step, err)
						}
						n := min(int(count), len(model))
						if ok != (len(model) > 0) || len(got) != n {
							t.Fatalf("step %d: RightPopN(%d) = (%d values, %v), want %d values", step, count, len(got), ok, n)
						}
						for i := 0; i < n; i++ {
							if want := model[len(model)-1-i]; !bytes.Equal(got[i], want) {
								t.Fatalf("step %d: RightPopN value %d = %q, want %q", step, i, got[i], want)
							}
						}
						model = model[:len(model)-n]
					}

					// Nothing the operation did may show up in the model's own storage.
					model = append([][]byte(nil), model...)
					got, err := store.ListRange("k", 0, -1)
					if err != nil {
						t.Fatalf("step %d: ListRange error = %v", step, err)
					}
					if len(got) != len(model) {
						t.Fatalf("step %d: list has %d values, model has %d", step, len(got), len(model))
					}
					for i := range model {
						if !bytes.Equal(got[i], model[i]) {
							t.Fatalf("step %d: list[%d] = %q, model has %q", step, i, got[i], model[i])
						}
					}
				}
			})
		}
	}
}

// TestLeftPushDoesNotCopyTheList pins the cost of pushing to the front of a long
// list. LPUSH used to build a new array holding the whole list on every call, so
// LPUSH+LPOP at 10,000 values allocated about 245 KB per pair; now the front slot
// an LPOP frees is reused, and a push that does have to move the list leaves
// headroom so it does so once per doubling.
func TestLeftPushDoesNotCopyTheList(t *testing.T) {
	store := NewStore()
	values := make([][]byte, 10_000)
	for i := range values {
		values[i] = []byte("v")
	}
	if _, _, err := store.RightPush("l", values); err != nil {
		t.Fatalf("RightPush() error = %v", err)
	}
	one := [][]byte{[]byte("x")}

	const rounds = 2000
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for i := 0; i < rounds; i++ {
		if _, _, err := store.LeftPush("l", one); err != nil {
			t.Fatalf("LeftPush() error = %v", err)
		}
		if _, _, err := store.LeftPop("l"); err != nil {
			t.Fatalf("LeftPop() error = %v", err)
		}
	}
	runtime.ReadMemStats(&after)

	if perPair := (after.TotalAlloc - before.TotalAlloc) / rounds; perPair > 4096 {
		t.Fatalf("LPUSH+LPOP on a 10,000 value list allocated %d bytes per pair, want a small constant (it was ~245,000 when LPUSH copied the list)", perPair)
	}
}

// TestBuildingAListWithLeftPushIsLinear pins the total work of building a list a
// value at a time from the left: it must not grow with the square of the length.
func TestBuildingAListWithLeftPushIsLinear(t *testing.T) {
	store := NewStore()
	one := [][]byte{[]byte("x")}

	const pushes = 20_000
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for i := 0; i < pushes; i++ {
		if _, _, err := store.LeftPush("l", one); err != nil {
			t.Fatalf("LeftPush() error = %v", err)
		}
	}
	runtime.ReadMemStats(&after)

	// Copying the list on every push would allocate about 24*pushes^2/2 = 4.8 GB.
	if total := after.TotalAlloc - before.TotalAlloc; total > 64<<20 {
		t.Fatalf("%d LPUSHes allocated %d MiB, want it to grow linearly with the list", pushes, total>>20)
	}
	if got, err := store.ListRange("l", 0, -1); err != nil || len(got) != pushes {
		t.Fatalf("ListRange() = (%d values, %v), want %d values", len(got), err, pushes)
	}
}
