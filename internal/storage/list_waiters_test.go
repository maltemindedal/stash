package storage

import "testing"

func TestAPopThatLeavesElementsWakesTheNextWaiter(t *testing.T) {
	// A push wakes one waiting client. Its pop wakes the next while elements
	// remain, so a push of n elements serves up to n waiting clients in the order
	// they started waiting.
	tests := []struct {
		name string
		pop  func(s *Store, key string) error
	}{
		{name: "LeftPop", pop: func(s *Store, key string) error {
			_, _, err := s.LeftPop(key)
			return err
		}},
		{name: "RightPop", pop: func(s *Store, key string) error {
			_, _, err := s.RightPop(key)
			return err
		}},
		{name: "LeftPopN", pop: func(s *Store, key string) error {
			_, _, err := s.LeftPopN(key, 1)
			return err
		}},
		{name: "RightPopN", pop: func(s *Store, key string) error {
			_, _, err := s.RightPopN(key, 1)
			return err
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := NewStore()
			first := store.SubscribeListPush("q")
			second := store.SubscribeListPush("q")

			if _, _, err := store.RightPush("q", [][]byte{[]byte("a"), []byte("b")}); err != nil {
				t.Fatalf("RightPush() error = %v", err)
			}
			assertWoken(t, first, "the push wakes the longest-waiting client")
			assertNotWoken(t, second, "the push wakes one client")

			if err := tt.pop(store, "q"); err != nil {
				t.Fatalf("%s() error = %v", tt.name, err)
			}
			assertWoken(t, second, "the pop that leaves an element wakes the next client")

			third := store.SubscribeListPush("q")
			if err := tt.pop(store, "q"); err != nil {
				t.Fatalf("%s() error = %v", tt.name, err)
			}
			assertNotWoken(t, third, "the pop of the last element wakes nobody")
		})
	}
}

func TestACountedPopWakesTheNextWaiterOnlyWhenItTakesElementsAndLeavesSome(t *testing.T) {
	// LPOP and RPOP with a count wake the next waiting client when they remove
	// elements and leave at least one. A count of zero removes nothing, and a
	// count that empties the list leaves nothing to hand on.
	tests := []struct {
		name      string
		pop       func(s *Store, key string, count int64) ([][]byte, bool, error)
		count     int64
		wantWoken bool
	}{
		{name: "LeftPopN of two leaving one", pop: (*Store).LeftPopN, count: 2, wantWoken: true},
		{name: "RightPopN of two leaving one", pop: (*Store).RightPopN, count: 2, wantWoken: true},
		{name: "LeftPopN of zero", pop: (*Store).LeftPopN, count: 0, wantWoken: false},
		{name: "RightPopN of zero", pop: (*Store).RightPopN, count: 0, wantWoken: false},
		{name: "LeftPopN of all three", pop: (*Store).LeftPopN, count: 3, wantWoken: false},
		{name: "RightPopN of all three", pop: (*Store).RightPopN, count: 3, wantWoken: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := NewStore()
			if _, _, err := store.RightPush("q", [][]byte{[]byte("a"), []byte("b"), []byte("c")}); err != nil {
				t.Fatalf("RightPush() error = %v", err)
			}
			waiter := store.SubscribeListPush("q")

			popped, ok, err := tt.pop(store, "q", tt.count)
			if err != nil || !ok || int64(len(popped)) != tt.count {
				t.Fatalf("pop of %d = (%d values, %v, %v), want (%d values, true, nil)", tt.count, len(popped), ok, err, tt.count)
			}
			if tt.wantWoken {
				assertWoken(t, waiter, "the pop took elements and left one")
			} else {
				assertNotWoken(t, waiter, "the pop took nothing or left nothing")
			}
		})
	}
}

// assertWoken and assertNotWoken need not wait: a push or pop signals the
// waiter before it returns.
func assertWoken(t *testing.T, waiter chan struct{}, why string) {
	t.Helper()

	select {
	case <-waiter:
	default:
		t.Fatalf("waiter not signaled: %s", why)
	}
}

func assertNotWoken(t *testing.T, waiter chan struct{}, why string) {
	t.Helper()

	select {
	case <-waiter:
		t.Fatalf("waiter signaled: %s", why)
	default:
	}
}
