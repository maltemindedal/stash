package storage

import (
	"fmt"
	"sort"
	"strconv"
)

// Image is a keyspace image: a detached copy of every live key, its value and
// its TTL, taken at one instant. An AOF rewrite is generated from it. Other
// packages see it only through Stats and Recreate.
type Image struct {
	entries []imageEntry
	stats   SnapshotStats
	sorted  bool
}

// SnapshotStats summarizes the keys a keyspace image was taken from.
type SnapshotStats struct {
	TotalKeys          int
	ExportedKeys       int
	SkippedExpiredKeys int
}

// imageEntry is one key of a keyspace image. kind selects the payload field
// that is valid.
type imageEntry struct {
	key       string
	kind      ValueKind
	expiresAt int64
	str       []byte
	list      [][]byte
	// data is the image of a kind that has moved out of the fields above.
	data kindImage
}

// arena hands out the copies an image holds of the bytes it is taken from. It
// packs them into large chunks, so that an image of many small values is a few
// allocations.
type arena struct {
	chunk []byte
}

// bytes returns a copy of src that shares no room to grow with its neighbours.
func (a *arena) bytes(src []byte) []byte {
	var cloned []byte
	a.chunk, cloned = appendClonedBytesArena(a.chunk, src)
	return cloned
}

// string is bytes for a string.
func (a *arena) string(src string) []byte {
	var cloned []byte
	a.chunk, cloned = appendClonedStringBytesArena(a.chunk, src)
	return cloned
}

// Image copies every live key under every shard write lock, and runs barrier
// (which may be nil) just before writes resume, so that the caller can mark the
// instant the image stands for. It copies values and encodes nothing under the
// locks, and it calls no expiry listener.
func (s *Store) Image(barrier func()) *Image {
	s.writeLockAllShards()
	defer s.writeUnlockAllShards()

	img := s.imageLocked(s.now())
	if barrier != nil {
		barrier()
	}
	return img
}

// Stats reports how many keys the image was taken from and how many it holds.
func (img *Image) Stats() SnapshotStats {
	if img == nil {
		return SnapshotStats{}
	}
	return img.stats
}

// recreateItemsPerCommand bounds how many items (values, members, or field and
// value pairs) one recreation command carries. The AOF loader's parser rejects
// a command array of more than 1,048,576 elements, so one command per key made
// a collection of about a million values into a file the server could not read
// back at startup. Several bounded commands replay to the same state; Redis's
// own rewrite groups items the same way.
const recreateItemsPerCommand = 1024

// Recreate emits, in key order, the recreation commands of the image: the
// commands that rebuild every key when the command executor replays them into
// an empty Store. args[0] is the command name. A command carries at most 1,024
// items, and a TTL is written as the absolute deadline it had (SET's PXAT), so
// replay does not restart the countdown; Recreate reads no clock. It returns the
// number of keys it emitted commands for, and an error for a collection with a
// TTL, which no command recreates.
//
// emit must not keep args after it returns. An error from emit stops Recreate
// and is returned as it is. Recreate may be called again and emits the same
// commands, but not from two goroutines at once.
func (img *Image) Recreate(emit func(args [][]byte) error) (keys int, err error) {
	if img == nil {
		return 0, nil
	}
	if !img.sorted {
		sort.Slice(img.entries, func(i, j int) bool { return img.entries[i].key < img.entries[j].key })
		img.sorted = true
	}

	emitted := false
	counting := func(args [][]byte) error {
		emitted = true
		return emit(args)
	}
	for i := range img.entries {
		emitted = false
		if err := img.entries[i].recreate(counting); err != nil {
			return keys, err
		}
		if emitted {
			keys++
		}
	}
	return keys, nil
}

func (e *imageEntry) recreate(emit func(args [][]byte) error) error {
	// Only SET's PXAT recreates a TTL. No command gives a collection one today,
	// but refusing it keeps a future TTL from being dropped without a word.
	if e.kind != ValueKindString && e.expiresAt > 0 {
		return fmt.Errorf("storage: cannot recreate the TTL of %s key %q", e.kind, e.key)
	}

	if e.data != nil {
		return e.data.recreate(e.key, emit)
	}

	switch e.kind {
	case ValueKindString:
		args := [][]byte{[]byte("SET"), []byte(e.key), e.str}
		if e.expiresAt > 0 {
			args = append(args, []byte("PXAT"), strconv.AppendInt(nil, e.expiresAt, 10))
		}
		return emit(args)
	case ValueKindList:
		return emitChunked(emit, "RPUSH", e.key, len(e.list), 1, func(args [][]byte, i int) [][]byte {
			return append(args, e.list[i])
		})
	default:
		return fmt.Errorf("storage: cannot recreate value kind %q of key %q", e.kind, e.key)
	}
}

// emitChunked emits name commands that together carry count items for key, at
// most recreateItemsPerCommand items each. appendItem adds the arguments of
// item i, and argsPerItem is how many arguments that is.
func emitChunked(emit func(args [][]byte) error, name, key string, count, argsPerItem int, appendItem func(args [][]byte, i int) [][]byte) error {
	args := make([][]byte, 0, 2+min(count, recreateItemsPerCommand)*argsPerItem)
	for start := 0; start < count; start += recreateItemsPerCommand {
		args = append(args[:0], []byte(name), []byte(key))
		for i, end := start, min(start+recreateItemsPerCommand, count); i < end; i++ {
			args = appendItem(args, i)
		}
		if err := emit(args); err != nil {
			return err
		}
	}
	return nil
}
