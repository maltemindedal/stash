package record

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/maltemindedal/stash/internal/protocol"
)

// Dest is where a command frame goes: the append-only file, the Replicas, or
// both.
type Dest uint8

const (
	// ToAOF sends a command frame to the append-only file.
	ToAOF Dest = 1 << iota
	// ToReplicas sends a command frame to the Replicas.
	ToReplicas
	// ToBoth sends a command frame to the append-only file and the Replicas.
	ToBoth = ToAOF | ToReplicas
)

// commandFrame is one command frame of a Write and where it goes.
type commandFrame struct {
	cmd protocol.Array
	to  Dest
}

// Write is the command frames of one applied write, in the order they recreate
// it, and the Origin that decides where they may go. Its zero value holds no
// frames and has the zero Origin, which Record refuses.
type Write struct {
	Origin Origin
	frames []commandFrame
}

// Add appends cmd, to go where to says. Producers state only the destinations
// a command frame has: both by default, ToReplicas for PUBLISH, ToAOF for XADD.
// Record decides what the Origin allows.
func (w *Write) Add(cmd protocol.Array, to Dest) {
	if to&ToBoth == 0 {
		return
	}
	w.frames = append(w.frames, commandFrame{cmd: cmd, to: to & ToBoth})
}

// Extend appends the command frames of queued, in order: EXEC's write is the
// writes of the commands it ran.
func (w *Write) Extend(queued *Write) {
	w.frames = append(w.frames, queued.frames...)
}

// Reset drops the command frames, keeping the Origin.
func (w *Write) Reset() {
	w.frames = w.frames[:0]
}

// Len returns the number of command frames.
func (w *Write) Len() int {
	return len(w.frames)
}

// Frames returns the command frames that go to every destination in to, in
// order, as Add was given them. The Origin is not applied: a frame from the
// Master that was added ToBoth is returned for ToReplicas too.
func (w *Write) Frames(to Dest) []protocol.Value {
	var frames []protocol.Value
	for _, frame := range w.frames {
		if frame.to&to == to {
			frames = append(frames, frame.cmd)
		}
	}
	return frames
}

// DeleteFrame builds the DEL command frame for keys the server removed without
// a client asking: a memory-pressure eviction during a write, an active TTL
// eviction, or the evictions at startup.
func DeleteFrame(keys []string) protocol.Array {
	elements := make([]protocol.Value, 0, len(keys)+1)
	elements = append(elements, protocol.TextBulkString{Value: "DEL"})
	for _, key := range keys {
		elements = append(elements, protocol.BulkString{Data: []byte(key)})
	}

	return protocol.Array{Elements: elements}
}

// Log is the append-only file (*aof.Writer). Append applies the file's fsync
// policy itself: withhold reports that the write must not be acknowledged or
// propagated, which under appendfsync always is any failed write, flush or
// fsync. It keeps no reference to payload after it returns.
type Log interface {
	Append(payload []byte) (withhold bool, err error)
}

// Stream is the replication stream. Propagate counts payload into the
// replication offset and queues it for every Replica, and returns the offset
// at its end. ordered says that the caller holds the write's ordering in the
// command sequencer, so a stream may skip work the ordering already does; a
// write the server makes on its own can be published holding none. Active
// reports whether a Replica is attached or attaching. Neither keeps a
// reference to payload after it returns.
type Stream interface {
	Propagate(payload []byte, ordered bool) (end int64)
	Active() bool
}

// Outcome is what recording a write did.
type Outcome struct {
	// End is the replication offset at the end of the write's command frames,
	// or 0 if nothing was propagated.
	End int64
	// Withheld reports that the write could not be made durable and was not
	// propagated: a client is answered ERR persistence failure.
	Withheld bool
	// Err is the append failure, if any, or the refusal of a Write without an
	// Origin.
	Err error
}

// ErrZeroOrigin refuses a Write whose Origin was not set.
var ErrZeroOrigin = errors.New("record: zero origin")

// Recorder records each applied write: it sends the write's command frames to
// the append-only file, the replication stream, or both, and decides what a
// failed append does to the write's reply and propagation:
//
//	Origin  Command frames  Append fails under always      under everysec/no
//	Client  as added        withheld: ERR persistence      logged; propagated
//	                        failure
//	Server  as added        logged; withheld               logged; propagated
//	Master  AOF only        returned; the link ends        returned; the link ends
//	Replay  none            -                              -
//
// Its caller must hold the write's ordering, so that writes that do not
// commute reach each destination in the order they were applied. Writes on
// different stripes commute, and the Recorder takes no lock of its own.
type Recorder struct {
	stream Stream
	logger *slog.Logger
	log    atomic.Pointer[logPort]
}

// logPort boxes a Log so that it can be swapped atomically.
type logPort struct{ Log }

// New returns a Recorder that propagates to stream and logs append failures to
// logger. It records to no append-only file until SetLog is called.
func New(stream Stream, logger *slog.Logger) *Recorder {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Recorder{stream: stream, logger: logger}
}

// SetLog makes the Recorder append to log, the append-only file opened at
// startup, or to none when log is nil.
func (r *Recorder) SetLog(log Log) {
	if log == nil {
		r.log.Store(nil)
		return
	}
	r.log.Store(&logPort{Log: log})
}

func (r *Recorder) currentLog() Log {
	if port := r.log.Load(); port != nil {
		return port.Log
	}
	return nil
}

// Records reports whether anything records writes at the moment, an
// append-only file or a Replica attached or attaching, so that the command
// sequencer must order them.
func (r *Recorder) Records() bool {
	return r.log.Load() != nil || r.stream.Active()
}

// Record sends w's command frames where its Origin allows and applies the
// table on Recorder to a failed append. A Write without frames records
// nothing, whatever its Origin. The caller must hold w's ordering.
//
// Each command frame is encoded once, into a pooled buffer. When every frame
// goes to the same destinations, the append-only file and the stream are
// handed the same bytes; otherwise each destination's payload is assembled
// from the frames already encoded.
func (r *Recorder) Record(w *Write) Outcome {
	if len(w.frames) == 0 {
		return Outcome{}
	}
	switch w.Origin {
	case OriginClient, OriginServer, OriginMaster:
	case OriginReplay:
		return Outcome{}
	case 0:
		return Outcome{Err: ErrZeroOrigin}
	default:
		return Outcome{Err: fmt.Errorf("record: unknown origin %d", w.Origin)}
	}

	log := r.currentLog()
	var allowed Dest
	if log != nil {
		allowed |= ToAOF
	}
	if w.Origin.Propagates() {
		allowed |= ToReplicas
	}

	buffer := getBuffer()
	defer putBuffer(buffer)
	aofPayload, streamPayload, err := encode(buffer, w.frames, allowed)
	if err != nil {
		// Only a frame of a type the encoder does not know fails, which is a
		// bug in the command that built it. Nothing was sent anywhere.
		err = fmt.Errorf("record: encode command frames: %w", err)
		r.logger.Error("failed to encode a write's command frames", "error", err)
		return Outcome{Withheld: true, Err: err}
	}

	var outcome Outcome
	if len(aofPayload) > 0 {
		withhold, err := log.Append(aofPayload)
		if err != nil {
			outcome.Err = err
			outcome.Withheld = withhold
			if r.appendFailed(w, withhold, err) {
				return outcome
			}
		}
	}
	if len(streamPayload) > 0 {
		outcome.End = r.stream.Propagate(streamPayload, w.Origin != OriginServer)
	}
	return outcome
}

// maxPooledBuffer is the largest encoding buffer kept for reuse, so that one
// huge write does not pin its buffer for good.
const maxPooledBuffer = 1 << 20

var buffers = sync.Pool{New: func() any {
	buffer := make([]byte, 0, 512)
	return &buffer
}}

func getBuffer() *[]byte { return buffers.Get().(*[]byte) }

func putBuffer(buffer *[]byte) {
	if cap(*buffer) > maxPooledBuffer {
		return
	}
	*buffer = (*buffer)[:0]
	buffers.Put(buffer)
}

// span is where one encoded command frame sits in the buffer, and where it
// goes.
type span struct {
	start, end int
	to         Dest
}

// encode encodes each of frames that goes somewhere allowed once into
// *buffer, and returns the payload for the append-only file and the one for
// the stream, either of which may be empty. The payloads live in *buffer and
// are valid until it is reused.
func encode(buffer *[]byte, frames []commandFrame, allowed Dest) (aof, stream []byte, err error) {
	buf := (*buffer)[:0]
	defer func() { *buffer = buf }()

	// The common case: every frame that goes anywhere goes to the same
	// destinations, so both payloads are the same bytes.
	var uniform Dest
	mixed := false
	for _, frame := range frames {
		to := frame.to & allowed
		switch {
		case to == 0:
		case uniform == 0:
			uniform = to
		case to != uniform:
			mixed = true
		}
	}
	if uniform == 0 {
		return nil, nil, nil
	}
	if !mixed {
		for _, frame := range frames {
			if frame.to&allowed == 0 {
				continue
			}
			if buf, err = protocol.AppendValue(buf, frame.cmd); err != nil {
				return nil, nil, err
			}
		}
		return payloadFor(buf, uniform, ToAOF), payloadFor(buf, uniform, ToReplicas), nil
	}

	// Mixed destinations: encode each frame once, then copy the encoded
	// frames into each destination's payload, in order.
	var fixed [8]span
	spans := fixed[:0]
	for _, frame := range frames {
		to := frame.to & allowed
		if to == 0 {
			continue
		}
		start := len(buf)
		if buf, err = protocol.AppendValue(buf, frame.cmd); err != nil {
			return nil, nil, err
		}
		spans = append(spans, span{start: start, end: len(buf), to: to})
	}
	aofStart := len(buf)
	buf = appendSpans(buf, spans, ToAOF)
	streamStart := len(buf)
	buf = appendSpans(buf, spans, ToReplicas)
	return buf[aofStart:streamStart], buf[streamStart:], nil
}

func payloadFor(encoded []byte, to, dest Dest) []byte {
	if to&dest == 0 {
		return nil
	}
	return encoded
}

func appendSpans(buf []byte, spans []span, dest Dest) []byte {
	for _, s := range spans {
		if s.to&dest != 0 {
			buf = append(buf, buf[s.start:s.end]...)
		}
	}
	return buf
}

// appendFailed logs a failed append as its Origin requires, and reports
// whether the write must stop there, without being propagated.
func (r *Recorder) appendFailed(w *Write, withhold bool, err error) (stop bool) {
	switch w.Origin {
	case OriginMaster:
		// The caller ends the link, and logs why.
		return true
	case OriginServer:
		r.logger.Error("failed to append the server's own write to AOF", "error", err, "frames", len(w.frames))
	default:
		if withhold {
			r.logger.Error("failed to append command to AOF before responding", "error", err)
		} else {
			r.logger.Warn("failed to append command to AOF", "error", err)
		}
	}
	return withhold
}
