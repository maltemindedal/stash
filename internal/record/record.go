package record

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
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
// table on Recorder to a failed append. The caller must hold w's ordering.
func (r *Recorder) Record(w *Write) Outcome {
	switch w.Origin {
	case OriginClient, OriginServer, OriginMaster:
	case OriginReplay:
		return Outcome{}
	case 0:
		return Outcome{Err: ErrZeroOrigin}
	default:
		return Outcome{Err: fmt.Errorf("record: unknown origin %d", w.Origin)}
	}
	if len(w.frames) == 0 {
		return Outcome{}
	}

	var outcome Outcome
	if log := r.currentLog(); log != nil {
		if frames := w.Frames(ToAOF); len(frames) > 0 {
			withhold, err := appendFrames(log, frames)
			if err != nil {
				outcome.Err = err
				outcome.Withheld = withhold
				if r.appendFailed(w, withhold, err) {
					return outcome
				}
			}
		}
	}

	if !w.Origin.Propagates() {
		return outcome
	}
	frames := w.Frames(ToReplicas)
	if len(frames) == 0 {
		return outcome
	}
	payload, err := protocol.EncodeValues(frames)
	if err != nil {
		r.logger.Error("failed to encode command frames for the replicas", "error", err)
		return outcome
	}
	outcome.End = r.stream.Propagate(payload, w.Origin != OriginServer)
	return outcome
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

func appendFrames(log Log, frames []protocol.Value) (withhold bool, err error) {
	payload, err := protocol.EncodeValues(frames)
	if err != nil {
		return true, fmt.Errorf("record: encode command frames: %w", err)
	}
	return log.Append(payload)
}
