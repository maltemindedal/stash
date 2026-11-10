package storage

// slab hands out pointers to values of T that it allocates many at a time. A
// keyspace image holds one small header per collection key, and a pointer into
// a slab goes into an interface without the allocation that the header itself
// would cost there.
type slab[T any] struct {
	chunk []T
}

const (
	slabMinChunk = 16
	slabMaxChunk = 4096
)

// hold returns a pointer to a copy of v that lives as long as anything refers
// to it.
func (s *slab[T]) hold(v T) *T {
	if len(s.chunk) == cap(s.chunk) {
		s.chunk = make([]T, 0, min(max(2*cap(s.chunk), slabMinChunk), slabMaxChunk))
	}
	s.chunk = append(s.chunk, v)
	return &s.chunk[len(s.chunk)-1]
}
