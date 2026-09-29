package protocol

import (
	"bytes"
	"errors"
	"fmt"
)

// ErrIncomplete reports that a buffer does not yet contain a complete RESP
// frame. Callers should retry Decode after more bytes arrive.
var ErrIncomplete = errors.New("protocol: incomplete frame")

// ErrMissingCRLF reports a line or bulk payload that is not terminated by CRLF.
// It is a typed sentinel so callers can match it with errors.Is rather than the
// message text. AOF replay relies on this to tell a torn trailing record (a
// recoverable truncated tail) apart from genuine corruption.
var ErrMissingCRLF = errors.New("protocol: line missing CRLF terminator")

// IncompleteError reports an incomplete frame along with Need, a lower bound on
// the total buffer length the frame requires. Callers can defer re-decoding
// until the buffer reaches Need bytes to avoid rescanning the same prefix on
// every append. Need is only populated when the shortfall is known from a
// length prefix; a line still missing its terminator reports the bare
// ErrIncomplete sentinel instead. errors.Is(err, ErrIncomplete) matches both.
type IncompleteError struct {
	Need int
}

// Error reports the same message as the bare ErrIncomplete sentinel, so the
// byte hint stays an implementation detail of the error value.
func (e *IncompleteError) Error() string { return ErrIncomplete.Error() }

// Unwrap returns ErrIncomplete so errors.Is matches both error shapes.
func (e *IncompleteError) Unwrap() error { return ErrIncomplete }

// incompleteNeed returns an incomplete-frame error carrying a total-bytes hint,
// or the bare sentinel when the hint is non-positive.
func incompleteNeed(total int) error {
	if total <= 0 {
		return ErrIncomplete
	}
	return &IncompleteError{Need: total}
}

// maxNestingDepth bounds RESP array nesting so a maliciously deep frame cannot
// exhaust the goroutine stack through Decode's recursion.
const maxNestingDepth = 128

// Decode parses one RESP value from the front of buf without blocking.
// It returns the parsed value and the number of bytes consumed. When buf does
// not yet hold a complete frame, Decode returns ErrIncomplete and consumes
// nothing. Any other error is a permanent protocol error. Returned values do
// not retain buf, so callers may reuse or compact it after Decode returns.
//
// Decode keeps no state between calls, so retrying it on a growing buffer
// re-reads the whole frame each time. Use a Decoder for that.
func Decode(buf []byte) (Value, int, error) {
	var d Decoder
	return d.Decode(buf)
}

// Decoder decodes RESP values from a buffer that grows between calls. It
// remembers how far into an unfinished frame it got, so each byte of a frame
// that arrives over many reads is examined once rather than once per read. A
// Decoder is not safe for concurrent use; the zero value is ready to use.
type Decoder struct {
	// open holds the arrays whose elements are still arriving, outermost first.
	open []openArray
	// pos counts the bytes of the current frame already folded into open.
	pos int
	// limits tightens what the frames decoded next may declare.
	limits Limits
}

// SetLimits tightens what the following frames may declare, until it is called
// again. The zero Limits restores the defaults. It is meant to be called between
// frames: array and bulk headers already read are not checked again.
func (d *Decoder) SetLimits(limits Limits) {
	d.limits = limits
}

// openArray is an array whose header has been read but whose elements have not
// all arrived.
type openArray struct {
	want     int
	elements []Value
}

// Decode parses one RESP value from the front of buf, with the semantics of the
// package-level Decode. After ErrIncomplete the Decoder keeps its progress, so
// the next call must pass a buffer that starts with the same bytes plus
// whatever arrived since. After a value or a permanent error it starts over.
func (d *Decoder) Decode(buf []byte) (Value, int, error) {
	for {
		value, n, err := d.token(buf[d.pos:])
		if err != nil {
			return nil, 0, d.fail(err)
		}
		d.pos += n
		if value == nil {
			// The header of a non-empty array; its elements follow.
			continue
		}

		for {
			if len(d.open) == 0 {
				consumed := d.pos
				d.Reset()
				return value, consumed, nil
			}
			top := &d.open[len(d.open)-1]
			top.elements = append(top.elements, value)
			if len(top.elements) < top.want {
				break
			}
			value = Array{Elements: top.elements}
			*top = openArray{}
			d.open = d.open[:len(d.open)-1]
		}
	}
}

// Reset discards any partially decoded frame.
func (d *Decoder) Reset() {
	clear(d.open)
	d.open = d.open[:0]
	d.pos = 0
}

// fail turns a token error into the error Decode reports. An incomplete frame
// keeps the decoder's progress and carries a byte hint for the whole frame. A
// permanent error is wrapped once per enclosing array, innermost first, and
// discards the partial frame.
func (d *Decoder) fail(err error) error {
	if errors.Is(err, ErrIncomplete) {
		var incomplete *IncompleteError
		if errors.As(err, &incomplete) {
			return incompleteNeed(d.pos + incomplete.Need)
		}
		return ErrIncomplete
	}

	for i := len(d.open) - 1; i >= 0; i-- {
		err = fmt.Errorf("protocol: parse array element %d: %w", len(d.open[i].elements), err)
	}
	d.Reset()
	return err
}

// token reads the next value or array header from the front of buf. It returns
// a nil Value, and the header's size, when it opened a non-empty array.
func (d *Decoder) token(buf []byte) (Value, int, error) {
	if len(buf) == 0 {
		return nil, 0, ErrIncomplete
	}

	switch prefix := buf[0]; prefix {
	case '+':
		line, n, err := decodeLine(buf[1:])
		if err != nil {
			return nil, 0, err
		}
		return SimpleString{Value: string(line)}, 1 + n, nil
	case '-':
		line, n, err := decodeLine(buf[1:])
		if err != nil {
			return nil, 0, err
		}
		return ErrorValue{Message: string(line)}, 1 + n, nil
	case ':':
		line, n, err := decodeLine(buf[1:])
		if err != nil {
			return nil, 0, err
		}
		value, err := integerFromLine(line)
		if err != nil {
			return nil, 0, err
		}
		return value, 1 + n, nil
	case '#':
		line, n, err := decodeLine(buf[1:])
		if err != nil {
			return nil, 0, err
		}
		value, err := booleanFromMarker(line)
		if err != nil {
			return nil, 0, err
		}
		return value, 1 + n, nil
	case '_':
		line, n, err := decodeLine(buf[1:])
		if err != nil {
			return nil, 0, err
		}
		value, err := nullFromPayload(line)
		if err != nil {
			return nil, 0, err
		}
		return value, 1 + n, nil
	case '$':
		return d.decodeBulkString(buf)
	case '*':
		return d.openArray(buf)
	default:
		return nil, 0, fmt.Errorf("protocol: unsupported frame prefix %q", string(prefix))
	}
}

func (d *Decoder) decodeBulkString(buf []byte) (Value, int, error) {
	line, n, err := decodeLine(buf[1:])
	if err != nil {
		return nil, 0, err
	}
	consumed := 1 + n

	length, err := parseDecimalInt(line)
	if err != nil {
		return nil, 0, fmt.Errorf("protocol: parse bulk string length: %w", err)
	}
	if length == -1 {
		return BulkString{Null: true}, consumed, nil
	}
	if length < -1 {
		return nil, 0, fmt.Errorf("protocol: invalid bulk string length %d", length)
	}
	// Only a tightened limit is checked here. The default is left to the caller's
	// buffer limit, which also bounds a length too large to add up as a hint.
	if d.limits.bulkLength() < maxBulkStringLength {
		if err := d.limits.checkBulkLength(length); err != nil {
			return nil, 0, err
		}
	}

	remaining := buf[consumed:]
	if length > len(remaining)-2 {
		return nil, 0, incompleteNeed(consumed + length + 2)
	}
	if remaining[length] != '\r' || remaining[length+1] != '\n' {
		return nil, 0, fmt.Errorf("protocol: bulk string payload missing CRLF terminator: %w", ErrMissingCRLF)
	}

	return BulkString{Data: bytes.Clone(remaining[:length])}, consumed + length + 2, nil
}

// openArray reads an array header. A null or empty array is a complete value; a
// non-empty one is pushed onto the open stack and reported as a nil Value.
func (d *Decoder) openArray(buf []byte) (Value, int, error) {
	if len(d.open) >= maxNestingDepth {
		return nil, 0, fmt.Errorf("protocol: array nesting exceeds %d levels", maxNestingDepth)
	}

	line, n, err := decodeLine(buf[1:])
	if err != nil {
		return nil, 0, err
	}
	consumed := 1 + n

	count, err := parseDecimalInt(line)
	if err != nil {
		return nil, 0, fmt.Errorf("protocol: parse array length: %w", err)
	}
	if count == -1 {
		return Array{Null: true}, consumed, nil
	}
	if count < -1 {
		return nil, 0, fmt.Errorf("protocol: invalid array length %d", count)
	}
	if err := d.limits.checkArrayLength(count); err != nil {
		return nil, 0, err
	}
	if count == 0 {
		return Array{Elements: []Value{}}, consumed, nil
	}

	d.open = append(d.open, openArray{want: count, elements: make([]Value, 0, min(count, 64))})
	return nil, consumed, nil
}

// decodeLine locates a CRLF-terminated line at the front of buf and returns
// the line content and the number of bytes consumed including the terminator.
func decodeLine(buf []byte) ([]byte, int, error) {
	// Look for the terminator only within the line limit, so a stream that never
	// sends one is rejected instead of rescanned on every read.
	idx := bytes.IndexByte(buf[:min(len(buf), maxLineLength)], '\n')
	if idx < 0 {
		if len(buf) >= maxLineLength {
			return nil, 0, fmt.Errorf("protocol: line exceeds %d byte limit", maxLineLength)
		}
		return nil, 0, ErrIncomplete
	}

	content, err := trimCRLF(buf[:idx+1])
	if err != nil {
		return nil, 0, err
	}

	return content, idx + 1, nil
}
