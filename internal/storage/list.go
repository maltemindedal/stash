package storage

import "math"

// listValue is a list. items holds the whole array the list lives in, and head
// counts the dead (nil) slots at its front, left by pops from the left. LPUSH
// refills those slots, so a list used from its left end is neither copied on
// push nor left holding memory it no longer needs.
type listValue struct {
	items [][]byte
	head  int32
}

// listOf returns the list v holds, or ErrWrongType when v holds a value of
// another kind.
func listOf(v *ValueObject) (*listValue, error) {
	if list, ok := v.data.(*listValue); ok {
		return list, nil
	}
	return nil, ErrWrongType
}

// live returns the values of the list, which the caller copies before it
// releases the lock.
func (l *listValue) live() [][]byte {
	return l.items[l.head:]
}

// push adds additions, which the list keeps, to its left or right end and
// returns its new length. Pushed to the left, the last addition ends up first.
func (l *listValue) push(additions [][]byte, left bool) int {
	full, head := l.items, int(l.head) // the array the list lives in, and the dead slots ahead of it
	list := full[head:]
	if left {
		full, head = prependList(full, head, list, additions)
	} else {
		grown := append(list, additions...)
		if len(list) > 0 && &grown[0] == &list[0] {
			// It fitted: the list still lives in the same array, and so do the
			// dead slots ahead of it.
			full = full[:head+len(grown)]
		} else {
			full, head = grown, 0
		}
	}
	l.items, l.head = full, int32(head)
	return len(full) - head
}

// prependList puts additions in front of list, the last addition first, and
// returns the array the result lives in and how many dead slots precede it. full
// and head describe where list currently lives (see listValue); full is nil when
// there is nothing to reuse. When the dead slots ahead of list are enough it
// fills them and copies nothing. Otherwise it moves the list into a new array
// with as much headroom as the list has values, so pushing to the front copies
// the list once per doubling instead of once per push.
func prependList(full [][]byte, head int, list, additions [][]byte) ([][]byte, int) {
	k := len(additions)
	if head >= k {
		start := head - k
		for i, item := range additions {
			full[start+k-1-i] = item
		}
		return full, start
	}

	headroom := min(len(list), math.MaxInt32-k)
	grown := make([][]byte, headroom+k+len(list))
	for i, item := range additions {
		grown[headroom+k-1-i] = item
	}
	copy(grown[headroom+k:], list)
	return grown, headroom
}

// dropFront removes the n left-most values without copying: their slots become
// dead, and a later LPUSH can refill them.
func (l *listValue) dropFront(n int) {
	if head := int(l.head) + n; head <= math.MaxInt32 {
		l.head = int32(head)
		return
	}

	// Two billion pops from the left with no append moving the list into a new
	// array: copy what is left instead of letting the head overflow.
	l.items = append([][]byte(nil), l.items[int(l.head)+n:]...)
	l.head = 0
}

// dropEnd removes the n right-most values.
func (l *listValue) dropEnd(n int) {
	l.items = l.items[:len(l.items)-n]
}

// popFront removes the left-most value of a list that is not empty, and returns
// it with the number of values left. It and popBack are two methods rather than
// one taking a side so that each inlines: the call cost a pop about 3% of its
// time.
func (l *listValue) popFront() ([]byte, int) {
	list := l.live()
	item := list[0]
	list[0] = nil
	l.dropFront(1)
	return item, len(list) - 1
}

// popBack removes the right-most value of a list that is not empty, and returns
// it with the number of values left.
func (l *listValue) popBack() ([]byte, int) {
	list := l.live()
	last := len(list) - 1
	item := list[last]
	list[last] = nil
	l.dropEnd(1)
	return item, last
}

// popValues removes up to count values, which must not be negative, from the
// left or right end of a list that is not empty. It returns them in the order
// they were popped, with the number of values left. count is an int, which the
// command parses as one, so no int64 is narrowed on the way (6db82b0).
func (l *listValue) popValues(count int, left bool) ([][]byte, int) {
	list := l.live()
	n := min(count, len(list))
	popped := make([][]byte, n)
	if left {
		for i := 0; i < n; i++ {
			popped[i] = list[i]
			list[i] = nil
		}
		l.dropFront(n)
	} else {
		for i := 0; i < n; i++ {
			src := len(list) - 1 - i
			popped[i] = list[src]
			list[src] = nil
		}
		l.dropEnd(n)
	}
	return popped, len(list) - n
}

func (l *listValue) kind() kindID { return kindList }

func (l *listValue) approxSize() int64 {
	list := l.live()
	size := approxCollectionOverhead + int64(len(list))*approxListEntryOverhead
	for _, item := range list {
		size += int64(len(item))
	}
	return size
}

// clone copies the live values into an array of their own, without the dead
// slots.
func (l *listValue) clone() kindValue {
	return &listValue{items: append([][]byte(nil), l.live()...)}
}

func (l *listValue) empty() bool { return len(l.items) == int(l.head) }

func (l *listValue) image(a *arena) kindImage {
	list := l.live()
	values := make(listImage, len(list))
	for i, item := range list {
		values[i] = a.bytes(item)
	}
	return values
}

// listImage is a list in a keyspace image: its values from left to right.
type listImage [][]byte

// recreate emits RPUSH commands of the values in list order. RPUSH appends, so
// the commands rebuild the order.
func (values listImage) recreate(key string, emit func(args [][]byte) error) error {
	return emitChunked(emit, "RPUSH", key, len(values), 1, func(args [][]byte, i int) [][]byte {
		return append(args, values[i])
	})
}
