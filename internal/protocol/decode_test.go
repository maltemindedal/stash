package protocol

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
)

func TestDecodeCompleteFrames(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  Value
	}{
		{name: "simple string", input: "+OK\r\n", want: SimpleString{Value: "OK"}},
		{name: "error", input: "-ERR boom\r\n", want: ErrorValue{Message: "ERR boom"}},
		{name: "integer", input: ":42\r\n", want: Integer{Value: 42}},
		{name: "negative integer", input: ":-7\r\n", want: Integer{Value: -7}},
		{name: "bulk string", input: "$5\r\nhello\r\n", want: BulkString{Data: []byte("hello")}},
		{name: "empty bulk string", input: "$0\r\n\r\n", want: BulkString{Data: []byte{}}},
		{name: "null bulk string", input: "$-1\r\n", want: BulkString{Null: true}},
		{name: "boolean true", input: "#t\r\n", want: Boolean{Value: true}},
		{name: "boolean false", input: "#f\r\n", want: Boolean{Value: false}},
		{name: "null", input: "_\r\n", want: Null{}},
		{name: "null array", input: "*-1\r\n", want: Array{Null: true}},
		{name: "empty array", input: "*0\r\n", want: Array{Elements: []Value{}}},
		{
			name:  "array of bulk strings",
			input: "*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n",
			want: Array{Elements: []Value{
				BulkString{Data: []byte("ECHO")},
				BulkString{Data: []byte("hello")},
			}},
		},
		{
			name:  "nested array",
			input: "*2\r\n*1\r\n:1\r\n+done\r\n",
			want: Array{Elements: []Value{
				Array{Elements: []Value{Integer{Value: 1}}},
				SimpleString{Value: "done"},
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value, consumed, err := Decode([]byte(tt.input))
			if err != nil {
				t.Fatalf("Decode() error = %v", err)
			}
			if consumed != len(tt.input) {
				t.Fatalf("Decode() consumed = %d, want %d", consumed, len(tt.input))
			}
			if !reflect.DeepEqual(value, tt.want) {
				t.Fatalf("Decode() = %#v, want %#v", value, tt.want)
			}
		})
	}
}

func TestDecodeIncompleteAtEveryBoundary(t *testing.T) {
	frames := []string{
		"+OK\r\n",
		"-ERR boom\r\n",
		":42\r\n",
		"$5\r\nhello\r\n",
		"$-1\r\n",
		"#t\r\n",
		"_\r\n",
		"*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n",
		"*2\r\n*1\r\n:1\r\n+done\r\n",
	}

	for _, frame := range frames {
		for cut := 0; cut < len(frame); cut++ {
			value, consumed, err := Decode([]byte(frame[:cut]))
			if !errors.Is(err, ErrIncomplete) {
				t.Fatalf("Decode(%q) error = %v, want ErrIncomplete", frame[:cut], err)
			}
			if value != nil || consumed != 0 {
				t.Fatalf("Decode(%q) = (%#v, %d), want (nil, 0)", frame[:cut], value, consumed)
			}
		}
	}
}

func TestDecodeLeavesTrailingBytes(t *testing.T) {
	input := []byte("+first\r\n+second\r\n")

	value, consumed, err := Decode(input)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if want := (SimpleString{Value: "first"}); value != want {
		t.Fatalf("Decode() = %#v, want %#v", value, want)
	}
	if want := len("+first\r\n"); consumed != want {
		t.Fatalf("Decode() consumed = %d, want %d", consumed, want)
	}

	value, consumed, err = Decode(input[consumed:])
	if err != nil {
		t.Fatalf("Decode() second frame error = %v", err)
	}
	if want := (SimpleString{Value: "second"}); value != want {
		t.Fatalf("Decode() second frame = %#v, want %#v", value, want)
	}
	if want := len("+second\r\n"); consumed != want {
		t.Fatalf("Decode() second frame consumed = %d, want %d", consumed, want)
	}
}

func TestDecodeDoesNotRetainBuffer(t *testing.T) {
	input := []byte("$5\r\nhello\r\n")

	value, _, err := Decode(input)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}

	copy(input, "$5\r\nXXXXX\r\n")

	bulk, ok := value.(BulkString)
	if !ok {
		t.Fatalf("Decode() = %#v, want BulkString", value)
	}
	if got := string(bulk.Data); got != "hello" {
		t.Fatalf("bulk string payload = %q, want %q after buffer reuse", got, "hello")
	}
}

func TestDecodeIntegerAcceptsLeadingPlus(t *testing.T) {
	// A leading '+' sign is valid in a RESP integer frame and must decode the
	// same way the blocking parser does, not as a protocol error.
	value, consumed, err := Decode([]byte(":+42\r\n"))
	if err != nil {
		t.Fatalf("Decode(:+42) error = %v", err)
	}
	if want := (Integer{Value: 42}); value != want {
		t.Fatalf("Decode(:+42) = %#v, want %#v", value, want)
	}
	if consumed != len(":+42\r\n") {
		t.Fatalf("Decode(:+42) consumed = %d, want %d", consumed, len(":+42\r\n"))
	}
}

func TestDecodeIncompleteCarriesByteHint(t *testing.T) {
	// A bulk string missing payload bytes reports the total frame size so a
	// caller can defer re-decoding until enough bytes arrive.
	_, _, err := Decode([]byte("$5\r\nhe"))
	var incomplete *IncompleteError
	if !errors.As(err, &incomplete) {
		t.Fatalf("Decode error = %v, want *IncompleteError", err)
	}
	if want := len("$5\r\nhello\r\n"); incomplete.Need != want {
		t.Fatalf("IncompleteError.Need = %d, want %d", incomplete.Need, want)
	}
	if !errors.Is(err, ErrIncomplete) {
		t.Fatal("errors.Is(err, ErrIncomplete) = false, want true")
	}
}

func TestDecodeArrayIncompleteCarriesByteHint(t *testing.T) {
	// The hint on a partially-arrived array element must account for the array
	// header bytes so it reflects the whole frame's size.
	_, _, err := Decode([]byte("*1\r\n$5\r\nhe"))
	var incomplete *IncompleteError
	if !errors.As(err, &incomplete) {
		t.Fatalf("Decode error = %v, want *IncompleteError", err)
	}
	if want := len("*1\r\n$5\r\nhello\r\n"); incomplete.Need != want {
		t.Fatalf("IncompleteError.Need = %d, want %d", incomplete.Need, want)
	}
}

func TestDecodeHugeBulkLengthReportsIncomplete(t *testing.T) {
	// A near-MaxInt declared length must not overflow the payload-availability
	// check into a false "complete" read that then indexes out of range.
	for _, length := range []string{"9223372036854775807", "9223372036854775806"} {
		input := []byte("$" + length + "\r\n")
		value, consumed, err := Decode(input)
		if !errors.Is(err, ErrIncomplete) {
			t.Fatalf("Decode($%s) error = %v, want ErrIncomplete", length, err)
		}
		if value != nil || consumed != 0 {
			t.Fatalf("Decode($%s) = (%#v, %d), want (nil, 0)", length, value, consumed)
		}
	}
}

func TestDecodeDeeplyNestedArrayIsBounded(t *testing.T) {
	// Deep nesting must fail as a protocol error rather than recursing until
	// the goroutine stack overflows.
	var buf []byte
	for i := 0; i < maxNestingDepth+16; i++ {
		buf = append(buf, "*1\r\n"...)
	}
	buf = append(buf, ":1\r\n"...)

	_, consumed, err := Decode(buf)
	if err == nil || errors.Is(err, ErrIncomplete) {
		t.Fatalf("Decode(deeply nested) error = %v, want permanent protocol error", err)
	}
	if consumed != 0 {
		t.Fatalf("Decode(deeply nested) consumed = %d, want 0", consumed)
	}
}

func TestDecodeNestingAtLimitSucceeds(t *testing.T) {
	// One level below the guard must still decode so the limit does not reject
	// legitimately nested frames.
	depth := maxNestingDepth - 1
	var buf []byte
	for i := 0; i < depth; i++ {
		buf = append(buf, "*1\r\n"...)
	}
	buf = append(buf, ":1\r\n"...)

	value, consumed, err := Decode(buf)
	if err != nil {
		t.Fatalf("Decode(nested to limit) error = %v", err)
	}
	if consumed != len(buf) {
		t.Fatalf("Decode(nested to limit) consumed = %d, want %d", consumed, len(buf))
	}
	for i := 0; i < depth; i++ {
		array, ok := value.(Array)
		if !ok || len(array.Elements) != 1 {
			t.Fatalf("level %d = %#v, want single-element array", i, value)
		}
		value = array.Elements[0]
	}
	if want := (Integer{Value: 1}); value != want {
		t.Fatalf("innermost value = %#v, want %#v", value, want)
	}
}

func TestDecodeProtocolErrors(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{name: "unsupported prefix", input: "X hello\r\n"},
		{name: "line missing carriage return", input: "+OK\n"},
		{name: "non-numeric integer", input: ":abc\r\n"},
		{name: "invalid boolean marker", input: "#x\r\n"},
		{name: "non-empty null payload", input: "_x\r\n"},
		{name: "invalid bulk string length", input: "$-2\r\n"},
		{name: "bulk string missing terminator", input: "$5\r\nhelloXX"},
		{name: "invalid array length", input: "*-2\r\n"},
		{name: "invalid array element", input: "*1\r\nX\r\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, consumed, err := Decode([]byte(tt.input))
			if err == nil || errors.Is(err, ErrIncomplete) {
				t.Fatalf("Decode(%q) error = %v, want permanent protocol error", tt.input, err)
			}
			if consumed != 0 {
				t.Fatalf("Decode(%q) consumed = %d, want 0", tt.input, consumed)
			}
		})
	}
}

func TestDecoderResumesAcrossArbitrarySplits(t *testing.T) {
	frames := []string{
		"+OK\r\n",
		"$5\r\nhello\r\n",
		"*0\r\n",
		"*-1\r\n",
		"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
		"*3\r\n$3\r\nSET\r\n*2\r\n:1\r\n$2\r\nhi\r\n$-1\r\n",
		"*2\r\n*1\r\n*1\r\n:1\r\n+done\r\n",
	}

	for _, frame := range frames {
		want, wantN, err := Decode([]byte(frame))
		if err != nil {
			t.Fatalf("Decode(%q) error = %v", frame, err)
		}

		for step := 1; step <= 4; step++ {
			var d Decoder
			var got Value
			var gotN int
			for end := step; ; end += step {
				end = min(end, len(frame))
				got, gotN, err = d.Decode([]byte(frame[:end]))
				if !errors.Is(err, ErrIncomplete) {
					break
				}
				if end == len(frame) {
					t.Fatalf("Decode(%q) still incomplete at the full frame", frame)
				}
			}
			if err != nil {
				t.Fatalf("step %d: Decode(%q) error = %v", step, frame, err)
			}
			if !reflect.DeepEqual(got, want) || gotN != wantN {
				t.Fatalf("step %d: Decode(%q) = (%#v, %d), want (%#v, %d)", step, frame, got, gotN, want, wantN)
			}
		}
	}
}

func TestDecoderKeepsItsPlaceInAnUnfinishedFrame(t *testing.T) {
	// The decoder must never look at a byte twice: after each read it has folded
	// in everything up to the last incomplete token, so the work per read does not
	// grow with the bytes already received.
	const args = 5000
	var frame []byte
	frame = append(frame, "*5000\r\n"...)
	for i := 0; i < args; i++ {
		frame = append(frame, "$1\r\nx\r\n"...)
	}

	var d Decoder
	lastPos := 0
	for end := 100; end < len(frame); end += 100 {
		_, _, err := d.Decode(frame[:end])
		if !errors.Is(err, ErrIncomplete) {
			t.Fatalf("Decode(frame[:%d]) error = %v, want ErrIncomplete", end, err)
		}
		if d.pos < lastPos {
			t.Fatalf("progress moved backwards from %d to %d", lastPos, d.pos)
		}
		if lag := end - d.pos; lag >= len("$1\r\nx\r\n") {
			t.Fatalf("after %d bytes the decoder had only consumed %d, want it within one token of the end", end, d.pos)
		}
		lastPos = d.pos
	}

	value, n, err := d.Decode(frame)
	if err != nil {
		t.Fatalf("Decode(full frame) error = %v", err)
	}
	if array, ok := value.(Array); !ok || len(array.Elements) != args || n != len(frame) {
		t.Fatalf("Decode(full frame) = (%d elements?, %d), want %d elements and %d bytes", len(array.Elements), n, args, len(frame))
	}
}

func TestDecoderStartsOverAfterAnError(t *testing.T) {
	var d Decoder
	if _, _, err := d.Decode([]byte("*2\r\n:1\r\nX")); err == nil || errors.Is(err, ErrIncomplete) {
		t.Fatalf("Decode(bad element) error = %v, want permanent protocol error", err)
	}

	value, n, err := d.Decode([]byte("+OK\r\n"))
	if err != nil || n != len("+OK\r\n") || value != (SimpleString{Value: "OK"}) {
		t.Fatalf("Decode(after error) = (%#v, %d, %v), want a clean decode of the next frame", value, n, err)
	}
}

func TestDecodeErrorNamesEveryEnclosingArrayElement(t *testing.T) {
	_, _, err := Decode([]byte("*3\r\n:1\r\n*2\r\n:2\r\nX\r\n"))
	want := `protocol: parse array element 1: protocol: parse array element 1: protocol: unsupported frame prefix "X"`
	if err == nil || err.Error() != want {
		t.Fatalf("Decode() error = %v, want %q", err, want)
	}
}

func TestDecodeBoundsLinesAndArrayLengths(t *testing.T) {
	line := func(prefix string, bodyLen int, terminated bool) []byte {
		frame := append([]byte(prefix), bytes.Repeat([]byte("A"), bodyLen)...)
		if terminated {
			frame = append(frame, "\r\n"...)
		}
		return frame
	}

	tests := []struct {
		name         string
		input        []byte
		wantComplete bool
		wantErr      bool
	}{
		{name: "line at the limit decodes", input: line("+", maxLineLength-2, true), wantComplete: true},
		{name: "line one byte under the limit still waits for its terminator", input: line("+", maxLineLength-1, false)},
		{name: "unterminated line at the limit is an error", input: line("+", maxLineLength, false), wantErr: true},
		{name: "terminated line over the limit is an error", input: line("+", maxLineLength-1, true), wantErr: true},
		{name: "array header at the element limit waits for elements", input: []byte("*1048576\r\n")},
		{name: "array header over the element limit is an error", input: []byte("*1048577\r\n"), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := Decode(tt.input)
			switch {
			case tt.wantComplete && err != nil:
				t.Fatalf("Decode() error = %v, want a complete value", err)
			case tt.wantErr && (err == nil || errors.Is(err, ErrIncomplete)):
				t.Fatalf("Decode() error = %v, want a permanent protocol error", err)
			case !tt.wantComplete && !tt.wantErr && !errors.Is(err, ErrIncomplete):
				t.Fatalf("Decode() error = %v, want ErrIncomplete", err)
			}
		})
	}
}
