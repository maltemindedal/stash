package output

import "github.com/maltemindedal/stash/internal/protocol"

// outputRetainedBuffer is the capacity above which a drained buffer is released
// rather than kept for the next batch: a large Reply would otherwise leave its
// size on every connection that asked for one.
const outputRetainedBuffer = 2 << 20

// chain holds the bytes that wait for one connection, in the order of its
// stream, and nothing else. It makes no decision and keeps no count: how many
// bytes wait, which of them a writer may write and what a limit counts are the
// Ledger's to say, and a test checks after every operation that the chain
// holds as many bytes as the Ledger counts. The Output changes both under one
// hold of its lock.
type chain struct {
	// inflight is the buffer being written; inflight[sent:] is still to go.
	inflight []byte
	sent     int
	// ahead are whole buffers that follow inflight, in order. A large Reply is
	// queued here without being copied, behind the buffer that was queued
	// until then.
	ahead [][]byte
	// queued follows ahead, and is where small Replies and Pushed messages are
	// appended.
	queued []byte
	// held are the Pushed messages waiting behind the current request's
	// Reply. They are not in the stream yet.
	held []byte
	// spare is an emptied buffer kept for what is queued while another buffer
	// is being written.
	spare []byte
}

// reply appends the encoding of values to the stream and returns how many
// bytes that was.
func (c *chain) reply(values []protocol.Value) (int, error) {
	before := len(c.queued)
	queued, err := protocol.AppendValues(c.queued, values)
	if err != nil {
		return 0, err
	}
	c.queued = queued
	return len(queued) - before, nil
}

// whole appends buffer to the stream without copying it. What is in queued is
// ahead of it, so that buffer goes first.
func (c *chain) whole(buffer []byte) {
	if len(c.queued) > 0 {
		c.ahead = append(c.ahead, c.queued)
		c.queued, c.spare = c.spare[:0], nil
	}
	c.ahead = append(c.ahead, buffer)
}

// push appends a Pushed message to the stream.
func (c *chain) push(frame []byte) {
	c.queued = append(c.queued, frame...)
}

// hold keeps a Pushed message for the Reply that releases it.
func (c *chain) hold(frame []byte) {
	c.held = append(c.held, frame...)
}

// release appends what is held to the stream, behind the Reply that was just
// appended.
func (c *chain) release() {
	switch {
	case len(c.held) == 0:
	case len(c.held) >= outputRetainedBuffer:
		c.whole(c.held)
		c.held = nil
	default:
		c.queued = append(c.queued, c.held...)
		c.held = c.held[:0]
		if cap(c.held) > outputRetainedBuffer {
			c.held = nil
		}
	}
}

// clear drops everything, when the connection is closed. A writer in the
// middle of a write keeps the bytes it is writing until that write returns.
func (c *chain) clear() {
	*c = chain{}
}

// waiting is how many bytes the chain holds, held ones included.
func (c *chain) waiting() int {
	n := len(c.inflight) - c.sent + len(c.queued) + len(c.held)
	for _, buffer := range c.ahead {
		n += len(buffer)
	}
	return n
}

// offered reports whether the buffer being written has bytes still to go.
func (c *chain) offered() bool {
	return c.sent < len(c.inflight)
}

// next returns the bytes at the front of the stream to offer the socket, no
// more than n of them, and fewer when the buffer they are in ends before that.
// The caller knows that n bytes are in the stream. fresh reports that they
// begin a buffer of which nothing was offered before.
func (c *chain) next(n int64) (chunk []byte, fresh bool) {
	if c.sent == len(c.inflight) {
		fresh = true
		if len(c.ahead) > 0 {
			c.inflight, c.sent = c.ahead[0], 0
			// Moved down, not resliced, so that the list keeps its array for
			// the next large Reply.
			rest := copy(c.ahead, c.ahead[1:])
			c.ahead[rest] = nil
			c.ahead = c.ahead[:rest]
		} else {
			c.inflight, c.queued, c.spare, c.sent = c.queued, c.spare[:0], nil, 0
		}
	}
	chunk = c.inflight[c.sent:]
	if int64(len(chunk)) > n {
		chunk = chunk[:n]
	}
	return chunk, fresh
}

// drop removes the n bytes the socket took from the front of the stream.
func (c *chain) drop(n int) {
	c.sent += n
	if c.sent < len(c.inflight) {
		return
	}
	// The buffer is written. It is kept for what is queued next unless a large
	// Reply grew it: the connection may stay idle. Nothing queued meanwhile
	// means the next bytes fill this same buffer, the one in the processor's
	// cache, and the other stays spare; a client that reads each pipeline's
	// replies before it sends the next then uses one buffer, not two in turn.
	written := c.inflight[:0]
	if cap(written) > outputRetainedBuffer {
		written = nil
	}
	c.inflight, c.sent = nil, 0
	if len(c.queued) == 0 {
		c.queued, c.spare = written, c.queued
	} else {
		c.spare = written
	}
}
