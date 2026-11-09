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
