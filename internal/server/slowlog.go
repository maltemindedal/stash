package server

import (
	"sync"
	"sync/atomic"
	"time"
)

const defaultSlowlogCapacity = 128

// SlowlogEntry stores metadata for one command whose execution exceeded the
// configured slowlog threshold.
type SlowlogEntry struct {
	ID         int64
	Timestamp  time.Time
	Duration   time.Duration
	Command    []string
	ClientAddr string
}

// SlowlogRegistry stores a bounded, thread-safe log of slow commands.
type SlowlogRegistry struct {
	mu      sync.RWMutex
	entries []SlowlogEntry
	start   int
	count   int
	nextID  atomic.Int64
}

// NewSlowlogRegistry constructs a slowlog registry with Redis' default length.
func NewSlowlogRegistry() *SlowlogRegistry {
	return &SlowlogRegistry{entries: make([]SlowlogEntry, defaultSlowlogCapacity)}
}

// Record appends entry to the bounded log, assigning its monotonic ID.
func (r *SlowlogRegistry) Record(entry SlowlogEntry) {
	if r == nil || len(r.entries) == 0 {
		return
	}

	entry.ID = r.nextID.Add(1) - 1
	entry.Command = cloneStringSlice(entry.Command)

	r.mu.Lock()
	defer r.mu.Unlock()

	idx := (r.start + r.count) % len(r.entries)
	if r.count == len(r.entries) {
		idx = r.start
		r.start = (r.start + 1) % len(r.entries)
	} else {
		r.count++
	}
	r.entries[idx] = entry
}

// Entries returns up to limit entries ordered newest-first. A negative limit
// returns all retained entries.
func (r *SlowlogRegistry) Entries(limit int) []SlowlogEntry {
	if r == nil {
		return nil
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	count := r.count
	if limit >= 0 && limit < count {
		count = limit
	}
	entries := make([]SlowlogEntry, 0, count)
	for i := 0; i < count; i++ {
		idx := (r.start + r.count - 1 - i + len(r.entries)) % len(r.entries)
		entry := r.entries[idx]
		entry.Command = cloneStringSlice(entry.Command)
		entries = append(entries, entry)
	}

	return entries
}

// Len reports the number of retained slowlog entries.
func (r *SlowlogRegistry) Len() int {
	if r == nil {
		return 0
	}

	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.count
}

// Reset clears all retained slowlog entries while preserving monotonic IDs.
func (r *SlowlogRegistry) Reset() {
	if r == nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	for i := range r.entries {
		r.entries[i] = SlowlogEntry{}
	}
	r.start = 0
	r.count = 0
}

func cloneStringSlice(values []string) []string {
	if values == nil {
		return nil
	}

	cloned := make([]string, len(values))
	copy(cloned, values)
	return cloned
}
