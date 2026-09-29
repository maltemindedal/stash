package protocol

import "fmt"

// Limits caps what a single frame may declare. The zero value, and any zero
// field, means the package defaults: 1,048,576 array elements and 512 MiB per
// bulk string. Limits only ever tighten those defaults.
type Limits struct {
	// MaxArrayElements is the most elements an array header may declare.
	MaxArrayElements int
	// MaxBulkStringLength is the most bytes a bulk string header may declare.
	MaxBulkStringLength int
}

// UnauthenticatedLimits is what a client that has not authenticated may send: at
// most 10 elements per array and 16 KiB per bulk string, the same limits Redis
// applies before AUTH. Every command an unauthenticated client may run fits in
// that (AUTH and PING), and a server that requires a password would otherwise
// buffer up to 512 MiB per connection for anyone who can reach it.
var UnauthenticatedLimits = Limits{MaxArrayElements: 10, MaxBulkStringLength: 16 * 1024}

func (l Limits) arrayElements() int {
	if l.MaxArrayElements > 0 && l.MaxArrayElements < maxArrayElements {
		return l.MaxArrayElements
	}
	return maxArrayElements
}

func (l Limits) bulkLength() int {
	if l.MaxBulkStringLength > 0 && l.MaxBulkStringLength < maxBulkStringLength {
		return l.MaxBulkStringLength
	}
	return maxBulkStringLength
}

// checkArrayLength reports an array header that declares more elements than allowed.
func (l Limits) checkArrayLength(count int) error {
	limit := l.arrayElements()
	if count <= limit {
		return nil
	}
	if limit < maxArrayElements {
		return fmt.Errorf("protocol: array length %d exceeds %d element limit for a client that has not authenticated", count, limit)
	}
	return fmt.Errorf("protocol: array length %d exceeds %d element limit", count, limit)
}

// checkBulkLength reports a bulk string header that declares more bytes than allowed.
func (l Limits) checkBulkLength(length int) error {
	limit := l.bulkLength()
	if length <= limit {
		return nil
	}
	if limit < maxBulkStringLength {
		return fmt.Errorf("protocol: bulk string length %d exceeds %d byte limit for a client that has not authenticated", length, limit)
	}
	return fmt.Errorf("protocol: bulk string length %d exceeds %d byte limit", length, limit)
}
