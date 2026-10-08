package storage

import "testing"

// holdsWakeUp reports whether a list push waiter has a wake-up waiting for it,
// and takes it if so. It never waits: every wake-up is sent before the call
// that sends it returns.
func holdsWakeUp(ch chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func TestAPopThatLeavesElementsWakesTheNextWaiter(t *testing.T) {
	// A push wakes one waiter, the longest waiting. When that waiter's pop
	// leaves elements behind, it has to wake the next one, or a push of several
	// elements serves only one blocked client.
	pops := []struct {
		name string
		pop  func(s *Store, key string) (bool, error)
	}{
		{"LeftPop", func(s *Store, key string) (bool, error) {
			_, ok, err := s.LeftPop(key)
			return ok, err
		}},
		{"RightPop", func(s *Store, key string) (bool, error) {
			_, ok, err := s.RightPop(key)
			return ok, err
		}},
		{"LeftPopN", func(s *Store, key string) (bool, error) {
			_, ok, err := s.LeftPopN(key, 1)
			return ok, err
		}},
		{"RightPopN", func(s *Store, key string) (bool, error) {
			_, ok, err := s.RightPopN(key, 1)
			return ok, err
		}},
	}
	for _, tc := range pops {
		tc := tc // the subtest closure captures it (go 1.21 shares the loop variable)
		t.Run(tc.name, func(t *testing.T) {
			store := NewStore()
			first := store.SubscribeListPush("queue")
			second := store.SubscribeListPush("queue")

			if _, _, err := store.RightPush("queue", [][]byte{[]byte("a"), []byte("b")}); err != nil {
				t.Fatalf("RightPush() error = %v", err)
			}
			if !holdsWakeUp(first) {
				t.Fatal("the push did not wake the first waiter")
			}
			if holdsWakeUp(second) {
				t.Fatal("the push woke the second waiter as well as the first")
			}

			// The first waiter pops the first element; one is left.
			if ok, err := tc.pop(store, "queue"); err != nil || !ok {
				t.Fatalf("%s() of the first element = (%v, %v), want (true, nil)", tc.name, ok, err)
			}
			if !holdsWakeUp(second) {
				t.Fatalf("%s() left an element but did not wake the second waiter", tc.name)
			}

			// The second waiter pops the last element; nothing is left to wake for.
			later := store.SubscribeListPush("queue")
			if ok, err := tc.pop(store, "queue"); err != nil || !ok {
				t.Fatalf("%s() of the last element = (%v, %v), want (true, nil)", tc.name, ok, err)
			}
			if holdsWakeUp(later) {
				t.Fatalf("%s() of the last element woke a waiter", tc.name)
			}
			store.UnsubscribeListPush("queue", later)
		})
	}
}
