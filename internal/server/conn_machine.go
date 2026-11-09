package server

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/maltemindedal/stash/internal/protocol"
)

// ConnMachineState identifies the lifecycle phase of a connection state machine.
type ConnMachineState int

const (
	// ConnStateActive accepts readable input, executes parsed requests, and
	// buffers responses.
	ConnStateActive ConnMachineState = iota
	// ConnStateClosing rejects new input while draining already-parsed
	// requests and pending output before closing.
	ConnStateClosing
	// ConnStateClosed has released connection resources; all operations fail.
	ConnStateClosed
)

// defaultMaxReadBuffer bounds the bytes a single incomplete frame may buffer
// before the machine rejects it as a protocol error, preventing a client from
// exhausting memory with an oversized or never-terminated frame. It mirrors the
// role of Redis's proto-max-bulk-len query-buffer limit.
const defaultMaxReadBuffer = 512 * 1024 * 1024

// defaultMaxWriteBuffer bounds the response bytes buffered for a client that
// is not draining its socket, so a stalled or slow consumer is disconnected
// instead of growing server memory without limit. It mirrors the role of
// Redis's client-output-buffer-limit.
const defaultMaxWriteBuffer = 512 * 1024 * 1024

// retainedReadBufferCap is the capacity above which the machine releases its
// read buffer once only a small remainder is left in it. The buffer grows to hold
// one large request and would otherwise keep that capacity until the connection
// closes, so many idle connections that each once received a large value would
// pin all of it. The goroutine-per-connection mode likewise keeps only a bufio
// buffer of a few kilobytes between requests.
//
// The bound sits well above what a connection that keeps sending ordinary
// requests needs, because releasing that buffer on every read costs an
// allocation and a copy per read. Between reads such a connection holds part of
// one request plus the next read, which the event loop makes at most
// eventLoopReadChunk (64 KiB) at a time. For requests up to a chunk in size that
// is at most two chunks, and append rounds the capacity up to about 144 KiB for
// it, so four chunks leaves room above that.
const retainedReadBufferCap = 256 * 1024

// ConnCommandRunner executes one parsed request with connection-scoped state
// and returns the RESP responses to buffer for the client. A non-nil error is
// fatal for the connection.
type ConnCommandRunner func(ctx context.Context, request protocol.Value) ([]protocol.Value, error)

// ErrConnMachineClosed reports an operation on a connection state machine that
// no longer accepts it.
var ErrConnMachineClosed = errors.New("server: connection state machine closed")

// connEvent is one ordered unit of work parsed from readable input: either a
// complete request or a permanent protocol error whose RESP error reply must
// be emitted after the responses of earlier requests.
type connEvent struct {
	request  protocol.Value
	protoErr error
}

// ConnMachine is an explicit non-blocking connection state machine. It buffers
// readable bytes, parses complete RESP requests, executes them through a
// command runner, buffers RESP responses, and flushes pending output
// incrementally, without assuming a dedicated blocking goroutine per client.
//
// Authentication, subscription, transaction, and monitor state live on the
// bound ClientState, which the machine detaches from shared registries when
// the connection closes.
//
// On a permanent protocol error the machine emits an ordered error reply and
// then closes the connection, as the goroutine-per-connection handler does and
// as Redis does: the byte stream can no longer be resynchronized.
//
// A ConnMachine is not safe for concurrent use; a single driving loop must own
// it. Its output is the connection's Output, which other goroutines push to
// under its own lock.
type ConnMachine struct {
	client *ClientState
	out    *Output

	state          ConnMachineState
	err            error
	readBuf        []byte
	decoder        protocol.Decoder
	resumeAt       int
	maxReadBuffer  int
	maxWriteBuffer int
	pending        []connEvent

	// requestLimits, when set, is asked before each frame is decoded what that
	// frame may declare. decodePaused is set after a frame was decoded under
	// limits, which hold further decoding back until that frame has run: the
	// request may be the AUTH that lifts them.
	requestLimits func() protocol.Limits
	decodePaused  bool
}

// NewConnMachine constructs an active connection state machine bound to the
// connection-scoped client state, writing through the Output bound to it. A
// client with none is given one that is written only by Flush.
func NewConnMachine(client *ClientState) *ConnMachine {
	out := client.Output()
	if out == nil {
		out = newOutput(newOutputs(), unscheduledOutputDriver{}, discardLogger)
		if client != nil {
			client.BindOutput(out)
		}
	}
	return &ConnMachine{
		client:         client,
		out:            out,
		maxReadBuffer:  defaultMaxReadBuffer,
		maxWriteBuffer: defaultMaxWriteBuffer,
	}
}

// State returns the current lifecycle phase.
func (m *ConnMachine) State() ConnMachineState {
	return m.state
}

// Err returns the error that moved the machine toward close, if any.
func (m *ConnMachine) Err() error {
	return m.err
}

// PendingRequests reports how many parsed requests await execution.
func (m *ConnMachine) PendingRequests() int {
	count := 0
	for _, event := range m.pending {
		if event.protoErr == nil {
			count++
		}
	}
	return count
}

// HasPendingOutput reports whether buffered response bytes await flushing.
func (m *ConnMachine) HasPendingOutput() bool {
	return m.PendingOutputBytes() > 0
}

// PendingOutputBytes reports how many buffered response bytes await flushing.
func (m *ConnMachine) PendingOutputBytes() int {
	return m.out.Backlog()
}

// Feed appends readable bytes to the read buffer and parses every complete
// RESP request they finish. Incomplete trailing frames stay buffered until
// more bytes arrive; the machine defers re-decoding until the buffer reaches
// the byte count the pending frame is known to need, and its decoder keeps its
// place in the frame, so a frame arriving in many small chunks is not rescanned
// on every append. A permanent protocol
// error, including a frame exceeding the read-buffer limit, queues an ordered
// RESP error reply and transitions the machine to the closing state, so callers
// should check State after feeding.
func (m *ConnMachine) Feed(data []byte) error {
	if m.state != ConnStateActive {
		return ErrConnMachineClosed
	}

	m.readBuf = append(m.readBuf, data...)
	if m.decodePaused {
		// The request decoded before these bytes has not run yet. Decoding
		// resumes after it, under whatever limits apply then.
		m.checkReadBufferLimit()
		return nil
	}
	if m.resumeAt > 0 && len(m.readBuf) < m.resumeAt {
		return nil
	}

	m.decodeBuffered()
	return nil
}

// SetRequestLimits installs a function consulted before each frame is decoded.
// It returns the limits that frame must respect, or the zero Limits for none.
// The server uses it to hold a client that has not authenticated to small
// frames.
func (m *ConnMachine) SetRequestLimits(limits func() protocol.Limits) {
	m.requestLimits = limits
}

// decodeBuffered parses every complete request at the front of the read buffer.
// While limits apply it stops after one request, because that request may change
// which limits apply to the next (see decodePaused).
func (m *ConnMachine) decodeBuffered() {
	consumed := 0
	for {
		var limits protocol.Limits
		if m.requestLimits != nil {
			limits = m.requestLimits()
		}
		m.decoder.SetLimits(limits)

		value, n, err := m.decoder.Decode(m.readBuf[consumed:])
		if errors.Is(err, protocol.ErrIncomplete) {
			m.resumeAt = 0
			var incomplete *protocol.IncompleteError
			if errors.As(err, &incomplete) {
				m.resumeAt = incomplete.Need
			}
			break
		}
		if err != nil {
			m.enterProtocolError(err)
			return
		}

		m.pending = append(m.pending, connEvent{request: value})
		consumed += n
		if limits != (protocol.Limits{}) {
			m.decodePaused = true
			break
		}
	}

	if consumed > 0 {
		m.discardRead(consumed)
	}

	m.checkReadBufferLimit()
}

// discardRead drops the consumed prefix of the read buffer and keeps the bytes
// behind it. A buffer that grew past retainedReadBufferCap is not kept for them
// when they are fewer than that: they move to a buffer of their own, or to none when
// nothing is left, and the large one is garbage. A larger remainder stays where
// it is, because it is the start of the next large frame.
func (m *ConnMachine) discardRead(consumed int) {
	rest := m.readBuf[consumed:]
	if cap(m.readBuf) > retainedReadBufferCap && len(rest) < retainedReadBufferCap {
		// append to a nil slice returns nil when there is nothing to copy.
		m.readBuf = append([]byte(nil), rest...)
		return
	}
	m.readBuf = append(m.readBuf[:0], rest...)
}

func (m *ConnMachine) checkReadBufferLimit() {
	if len(m.readBuf) > m.maxReadBuffer || m.resumeAt > m.maxReadBuffer {
		m.enterProtocolError(fmt.Errorf("protocol: frame exceeds %d byte read-buffer limit", m.maxReadBuffer))
	}
}

// enterProtocolError queues an ordered error reply, records the fatal cause,
// and transitions to the closing state so buffered replies still drain.
func (m *ConnMachine) enterProtocolError(err error) {
	m.pending = append(m.pending, connEvent{protoErr: err})
	m.err = err
	m.state = ConnStateClosing
	m.readBuf = nil
	m.decoder.Reset()
	m.resumeAt = 0
	m.decodePaused = false
}

// ProcessPending executes parsed requests in arrival order through run and
// buffers their RESP responses. A queued protocol error emits its RESP error
// reply in order. A runner error is fatal and closes the machine immediately.
func (m *ConnMachine) ProcessPending(ctx context.Context, run ConnCommandRunner) error {
	if m.state == ConnStateClosed {
		return ErrConnMachineClosed
	}

	for {
		more, err := m.ProcessNext(ctx, run)
		if err != nil {
			return err
		}
		if !more {
			return nil
		}
	}
}

// ProcessNext executes at most one pending parsed request (or emits one queued
// protocol-error reply) and reports whether more remain. It lets a driving
// loop interleave output flushing between requests, so a large pipeline is not
// forced to buffer its entire response set before the first byte reaches the
// socket. A runner error is fatal and closes the machine immediately.
func (m *ConnMachine) ProcessNext(ctx context.Context, run ConnCommandRunner) (bool, error) {
	if m.state == ConnStateClosed {
		return false, ErrConnMachineClosed
	}
	if len(m.pending) == 0 {
		return false, nil
	}

	event := m.pending[0]
	// Clear the slot as well as reslicing past it: the queue's backing array lives
	// until the next request makes it grow, and a decoded request, which can be as
	// large as the value it carries, would stay reachable from it.
	m.pending[0] = connEvent{}
	m.pending = m.pending[1:]

	if event.protoErr != nil {
		// A replica's connection carries the replication stream and nothing
		// else, so a replica is closed unanswered once its output has drained.
		if m.client.IsReplica() {
			return len(m.pending) > 0, nil
		}
		if err := m.bufferResponses([]protocol.Value{protocol.ErrorValue{Message: "ERR " + event.protoErr.Error()}}); err != nil {
			m.Close(err)
			return false, err
		}
		return len(m.pending) > 0, nil
	}

	responses, err := run(ctx, event.request)
	if err != nil {
		m.Close(err)
		return false, err
	}
	if err := m.bufferResponses(responses); err != nil {
		m.Close(err)
		return false, err
	}

	if m.decodePaused && len(m.pending) == 0 && m.state == ConnStateActive {
		// The request that held decoding back has run; the bytes behind it are
		// decoded now, under the limits that apply after it.
		m.decodePaused = false
		m.decodeBuffered()
	}

	return len(m.pending) > 0, nil
}

// Flush writes the connection's pending output to w and consumes whatever w
// accepts, so a partial write leaves the remainder for a later call
// (Output.writeTo). Once a closing machine has drained its parsed requests and
// pending output, Flush completes the transition to the closed state.
func (m *ConnMachine) Flush(w io.Writer) error {
	if m.state == ConnStateClosed {
		return ErrConnMachineClosed
	}

	// A writer that took only part of the output is not a failed one.
	if err := m.out.writeTo(w); err != nil && !errors.Is(err, io.ErrShortWrite) {
		return err
	}

	if m.state == ConnStateClosing && !m.HasPendingOutput() && len(m.pending) == 0 {
		m.Close(nil)
	}

	return nil
}

// Close transitions to the closed state, discards buffered input, closes the
// connection's Output, and abandons the connection-scoped client state.
func (m *ConnMachine) Close(err error) {
	if m.state == ConnStateClosed {
		return
	}

	if m.err == nil {
		m.err = err
	}
	m.state = ConnStateClosed
	m.readBuf = nil
	m.resumeAt = 0
	m.decodePaused = false
	m.pending = nil
	m.out.Close(err)
	m.client.Abandon()
}

// bufferResponses queues the Replies to the request that just ran, which may
// be none: the Output releases the Pushed messages that request held either
// way.
func (m *ConnMachine) bufferResponses(values []protocol.Value) error {
	if err := m.out.Reply(values...); err != nil {
		return err
	}
	if m.PendingOutputBytes() > m.maxWriteBuffer {
		return fmt.Errorf("server: pending output exceeds %d byte write-buffer limit", m.maxWriteBuffer)
	}
	return nil
}
