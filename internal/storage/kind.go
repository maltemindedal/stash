package storage

// kindID numbers a value kind. It indexes kindNames and the per-kind key counts.
type kindID uint8

const (
	kindString kindID = iota
	kindList
	kindHash
	kindSet
	kindZSet
	kindStream
	kindCount
)

// kindNames names every value kind, in the order INFO lists its key_count_
// fields. It is the only list of kinds: a new kind adds its kindID above and its
// name here.
var kindNames = [kindCount]ValueKind{
	kindString: ValueKindString,
	kindList:   ValueKindList,
	kindHash:   ValueKindHash,
	kindSet:    ValueKindSet,
	kindZSet:   ValueKindZSet,
	kindStream: ValueKindStream,
}

// ValueKinds lists every value kind in INFO order.
func ValueKinds() []ValueKind {
	return append([]ValueKind(nil), kindNames[:]...)
}

// kindValue is the payload of one collection kind: what a ValueObject holds
// beside its TTL and access stamp. Each kind implements it once, in its own
// file, and the store asks it for everything that depends on the kind.
type kindValue interface {
	// kind returns the kind's index into kindNames.
	kind() kindID
	// approxSize returns the accounted size of the value beyond the per-key
	// base: what the value itself counts toward maxmemory.
	approxSize() int64
	// clone returns a copy that a growing write can edit while the value stays
	// stored under its key, so that a write refused for want of memory leaves
	// the stored value as it was. It copies the container and shares the element
	// byte strings, which nothing edits in place once they are stored.
	clone() kindValue
	// empty reports whether a write that left the value like this deletes its
	// key. A stream is never empty in this sense.
	empty() bool
	// image returns a detached copy for a keyspace image, with its bytes copied
	// into a. It runs under every shard lock: it must not lock, and it encodes
	// nothing.
	image(a *arena) kindImage
}

// kindImage is one value in a keyspace image.
type kindImage interface {
	// recreate emits the commands that rebuild the value under key, at most
	// recreateItemsPerCommand items each. It runs with no lock held and reads
	// no clock. emit does not keep args.
	recreate(key string, emit func(args [][]byte) error) error
}
