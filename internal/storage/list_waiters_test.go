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

			rightPush(t, store, "q", "a", "b")
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
			rightPush(t, store, "q", "a", "b", "c")
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

func TestAWakeUpNobodyReceivedPassesToTheNextWaiter(t *testing.T) {
	// A wake-up is a waiting client's turn at an element. A client can stop
	// waiting after a push or pop has signaled it but before it receives the
	// wake-up: it has already popped, it has gone, or it took its 100 ms client
	// check instead. Unsubscribing used to drop that wake-up, and the next client
	// stayed blocked with an element in the list.
	tests := []struct {
		name string
		// signal leaves first signaled and next waiting, subscribed before or
		// after the signal. The test then unsubscribes first without receiving
		// its wake-up.
		signal func(t *testing.T, s *Store) (first, next chan struct{})
	}{
		{name: "signaled by a push", signal: func(t *testing.T, s *Store) (chan struct{}, chan struct{}) {
			first := s.SubscribeListPush("q")
			next := s.SubscribeListPush("q")
			rightPush(t, s, "q", "a")
			return first, next
		}},
		{name: "signaled by a pop that leaves elements", signal: func(t *testing.T, s *Store) (chan struct{}, chan struct{}) {
			rightPush(t, s, "q", "a", "b")
			first := s.SubscribeListPush("q")
			next := s.SubscribeListPush("q")
			if _, _, err := s.LeftPop("q"); err != nil {
				t.Fatalf("LeftPop() error = %v", err)
			}
			return first, next
		}},
		{name: "signaled before the next waiter subscribed", signal: func(t *testing.T, s *Store) (chan struct{}, chan struct{}) {
			// A BLPOP that has popped keeps its waiter queued until it
			// unsubscribes. A push in that window signals it, and a client that
			// started waiting before it unsubscribes needs the turn it never used.
			first := s.SubscribeListPush("q")
			rightPush(t, s, "q", "a")
			return first, s.SubscribeListPush("q")
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := NewStore()
			first, next := tt.signal(t, store)

			store.UnsubscribeListPush("q", first)
			assertWoken(t, next, "the wake-up first never received passes to the next waiter")
			assertNotWoken(t, first, "first's wake-up has been passed on")
		})
	}
}

func TestUnsubscribingAWaiterThatHoldsNoWakeUpWakesNobody(t *testing.T) {
	// Only a wake-up nobody received is passed on. A waiter still in the queue
	// was never signaled, and one whose client received its wake-up uses it.
	tests := []struct {
		name    string
		prepare func(t *testing.T, s *Store, first chan struct{})
	}{
		{name: "still waiting", prepare: func(*testing.T, *Store, chan struct{}) {}},
		{name: "wake-up received", prepare: func(t *testing.T, s *Store, first chan struct{}) {
			rightPush(t, s, "q", "a")
			assertWoken(t, first, "the push wakes the longest-waiting client")
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := NewStore()
			first := store.SubscribeListPush("q")
			tt.prepare(t, store, first)
			second := store.SubscribeListPush("q")

			store.UnsubscribeListPush("q", first)
			assertNotWoken(t, second, "first held no wake-up to pass on")
		})
	}
}

// rightPush appends values to the list at key.
func rightPush(t *testing.T, s *Store, key string, values ...string) {
	t.Helper()

	elements := make([][]byte, 0, len(values))
	for _, value := range values {
		elements = append(elements, []byte(value))
	}
	if _, _, err := s.RightPush(key, elements); err != nil {
		t.Fatalf("RightPush() error = %v", err)
	}
}

// assertWoken and assertNotWoken need not wait: a push, a pop or an unsubscribe
// signals the waiter before it returns.
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
