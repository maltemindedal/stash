// Package recordtest provides in-memory stand-ins for the append-only file and
// the replication stream a record.Recorder writes to, for tests at the seam
// with no files and no sockets.
package recordtest

import (
	"bytes"
	"sync"
)

// Log stands in for the append-only file. It keeps a copy of each payload it
// accepts, as the real one must, since the Recorder may reuse the buffer it
// passed.
type Log struct {
	mu       sync.Mutex
	appends  int
	payloads [][]byte
	err      error
	withhold bool
}

// Fail makes every later Append fail with err and report withhold, as the real
// file does under appendfsync always (withhold) or under everysec and no.
// A nil err makes Append succeed again.
func (l *Log) Fail(err error, withhold bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.err, l.withhold = err, withhold
}

// Append records a copy of payload, or fails as Fail set it up to.
func (l *Log) Append(payload []byte) (withhold bool, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.appends++
	if l.err != nil {
		return l.withhold, l.err
	}
	l.payloads = append(l.payloads, bytes.Clone(payload))
	return false, nil
}

// Appends returns how many times Append was called, failed calls included.
func (l *Log) Appends() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.appends
}

// Payloads returns copies of the payloads appended so far, oldest first.
func (l *Log) Payloads() [][]byte {
	l.mu.Lock()
	defer l.mu.Unlock()
	return clonePayloads(l.payloads)
}

// Bytes returns everything appended so far, as the file would hold it.
func (l *Log) Bytes() []byte {
	l.mu.Lock()
	defer l.mu.Unlock()
	return bytes.Join(l.payloads, nil)
}

// Stream stands in for the replication stream. It counts each payload into an
// offset and keeps a copy of it, as the real one must.
type Stream struct {
	mu       sync.Mutex
	end      int64
	payloads [][]byte
	ordered  []bool
	inactive bool
}

// SetActive sets what Active reports. A new Stream is active.
func (s *Stream) SetActive(active bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inactive = !active
}

// Active reports whether the stream stands for an attached Replica.
func (s *Stream) Active() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return !s.inactive
}

// Propagate records a copy of payload and whether the caller held the write's
// ordering, and returns the offset at its end.
func (s *Stream) Propagate(payload []byte, ordered bool) (end int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.end += int64(len(payload))
	s.payloads = append(s.payloads, bytes.Clone(payload))
	s.ordered = append(s.ordered, ordered)
	return s.end
}

// Payloads returns copies of the payloads propagated so far, oldest first.
func (s *Stream) Payloads() [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return clonePayloads(s.payloads)
}

// Ordered returns, for each payload propagated so far, whether its caller said
// it held the write's ordering.
func (s *Stream) Ordered() []bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]bool(nil), s.ordered...)
}

// Bytes returns everything propagated so far, as a Replica would receive it.
func (s *Stream) Bytes() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return bytes.Join(s.payloads, nil)
}

func clonePayloads(payloads [][]byte) [][]byte {
	clones := make([][]byte, len(payloads))
	for i, payload := range payloads {
		clones[i] = bytes.Clone(payload)
	}
	return clones
}
