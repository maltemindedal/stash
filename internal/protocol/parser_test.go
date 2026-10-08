package protocol

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"testing/iotest"
	"time"
)

// TestParseMissingCRLFReturnsSentinel locks the typed ErrMissingCRLF sentinel,
// so a reworded parser message can't silently change what errors.Is matches.
func TestParseMissingCRLFReturnsSentinel(t *testing.T) {
	// A bulk string whose declared payload is present but not CRLF-terminated.
	parser := NewParser(strings.NewReader("$4\r\nPINGxx"))
	if _, err := parser.Parse(); !errors.Is(err, ErrMissingCRLF) {
		t.Fatalf("Parse() error = %v, want ErrMissingCRLF", err)
	}
}

type chunkReader struct {
	chunks [][]byte
}

func (r *chunkReader) Read(p []byte) (int, error) {
	if len(r.chunks) == 0 {
		return 0, io.EOF
	}

	chunk := r.chunks[0]
	n := copy(p, chunk)
	if n == len(chunk) {
		r.chunks = r.chunks[1:]
	} else {
		r.chunks[0] = chunk[n:]
	}

	return n, nil
}

func TestParserParse(t *testing.T) {
	tests := []struct {
		name   string
		reader func() io.Reader
		assert func(*testing.T, Value, error)
	}{
		{
			name:   "array of bulk strings",
			reader: func() io.Reader { return stringsReader("*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n") },
			assert: func(t *testing.T, value Value, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("Parse() error = %v", err)
				}
				array, ok := value.(Array)
				if !ok {
					t.Fatalf("Parse() type = %T, want Array", value)
				}
				if len(array.Elements) != 2 {
					t.Fatalf("len(array.Elements) = %d, want 2", len(array.Elements))
				}
				assertBulkString(t, array.Elements[0], "ECHO")
				assertBulkString(t, array.Elements[1], "hello")
			},
		},
		{
			name: "fragmented input",
			reader: func() io.Reader {
				return &chunkReader{chunks: [][]byte{
					[]byte("*2\r\n$4\r\nEC"),
					[]byte("HO\r\n$5\r\nhe"),
					[]byte("llo\r\n"),
				}}
			},
			assert: func(t *testing.T, value Value, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("Parse() error = %v", err)
				}
				array := value.(Array)
				assertBulkString(t, array.Elements[0], "ECHO")
				assertBulkString(t, array.Elements[1], "hello")
			},
		},
		{
			name:   "malformed bulk string",
			reader: func() io.Reader { return stringsReader("$5\r\nhell\r\n") },
			assert: func(t *testing.T, _ Value, err error) {
				t.Helper()
				if err == nil {
					t.Fatal("Parse() error = nil, want malformed bulk string error")
				}
			},
		},
		{
			name:   "RESP3 null placeholder",
			reader: func() io.Reader { return stringsReader("_\r\n") },
			assert: func(t *testing.T, value Value, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("Parse() error = %v", err)
				}
				if _, ok := value.(Null); !ok {
					t.Fatalf("Parse() type = %T, want Null", value)
				}
			},
		},
		{
			name: "long line over buffered reader capacity",
			reader: func() io.Reader {
				return bufio.NewReaderSize(strings.NewReader("+"+strings.Repeat("a", 64)+"\r\n"), 8)
			},
			assert: func(t *testing.T, value Value, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("Parse() error = %v", err)
				}
				simple, ok := value.(SimpleString)
				if !ok {
					t.Fatalf("Parse() type = %T, want SimpleString", value)
				}
				if simple.Value != strings.Repeat("a", 64) {
					t.Fatalf("simple string = %q, want %q", simple.Value, strings.Repeat("a", 64))
				}
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.reader())
			value, err := parser.Parse()
			tt.assert(t, value, err)
		})
	}
}

// TestParserRejectsOversizedFrames verifies that forged length prefixes and
// terminator-less lines are rejected with a permanent protocol error rather
// than triggering a giant allocation (which for a near-MaxInt length would
// panic and, unrecovered, crash the whole process).
func TestParserRejectsOversizedFrames(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "bulk length near MaxInt does not allocate",
			input: "$9223372036854775807\r\n",
		},
		{
			name:  "bulk length just over cap",
			input: "$536870913\r\n", // maxBulkStringLength + 1
		},
		{
			name:  "array count near MaxInt does not allocate",
			input: "*9223372036854775807\r\n",
		},
		{
			name:  "array count just over cap",
			input: "*1048577\r\n", // maxArrayElements + 1
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(stringsReader(tt.input))
			if _, err := parser.Parse(); err == nil {
				t.Fatalf("Parse(%q) error = nil, want oversized-frame rejection", tt.input)
			}
		})
	}
}

// TestParserRejectsUnboundedLine verifies that a line which never sends its
// terminator is rejected once it passes maxLineLength instead of growing the
// line buffer without bound.
func TestParserRejectsUnboundedLine(t *testing.T) {
	input := "+" + strings.Repeat("a", maxLineLength*2) // no trailing CRLF
	parser := NewParser(strings.NewReader(input))
	if _, err := parser.Parse(); err == nil {
		t.Fatal("Parse() error = nil, want unbounded-line rejection")
	}
}

// TestParserBoundsALineAsTheDecoderDoes pins the two RESP decoders to one line
// bound: the LF must be among the first maxLineLength bytes after the type byte,
// whatever size of bufio.Reader or chunks the Parser reads through. The Parser
// used to count only the fragments that ended without an LF, so a line of up to
// 69,632 bytes (17 fragments of 4 KiB) got through, and a caller's larger
// bufio.Reader raised that bound further.
func TestParserBoundsALineAsTheDecoderDoes(t *testing.T) {
	// echoFrame is ECHO x whose bulk length line, CRLF included, is n bytes after
	// the "$": zeros, then the digit 1.
	echoFrame := func(n int) []byte {
		return []byte("*2\r\n$4\r\nECHO\r\n$" + strings.Repeat("0", n-3) + "1\r\nx\r\n")
	}
	// simpleString is a "+" line of n bytes after the "+", with or without its CRLF.
	simpleString := func(terminated bool) func(n int) []byte {
		return func(n int) []byte {
			if terminated {
				return []byte("+" + strings.Repeat("a", n-2) + "\r\n")
			}
			return []byte("+" + strings.Repeat("a", n))
		}
	}

	inputs := []struct {
		name  string
		build func(n int) []byte
		// over reports whether a line of n bytes is past the bound. A terminated
		// line may fill the 65,536 bytes; an unterminated one must stop short of
		// them, because the LF would have to be among them.
		over func(n int) bool
	}{
		{name: "ECHO frame with a padded bulk length", build: echoFrame, over: func(n int) bool { return n > maxLineLength }},
		{name: "terminated simple string", build: simpleString(true), over: func(n int) bool { return n > maxLineLength }},
		{name: "unterminated simple string", build: simpleString(false), over: func(n int) bool { return n >= maxLineLength }},
	}
	lengths := []int{maxLineLength - 1, maxLineLength, maxLineLength + 1, maxLineLength + 4096, maxLineLength + 4097}
	readers := []struct {
		name string
		wrap func(io.Reader) io.Reader
	}{
		{name: "whole input", wrap: func(r io.Reader) io.Reader { return r }},
		{name: "one byte at a time", wrap: iotest.OneByteReader},
		{name: "16 byte bufio.Reader", wrap: func(r io.Reader) io.Reader { return bufio.NewReaderSize(r, 16) }},
		{name: "128 KiB bufio.Reader", wrap: func(r io.Reader) io.Reader { return bufio.NewReaderSize(r, 128<<10) }},
	}

	for _, in := range inputs {
		for _, n := range lengths {
			input := in.build(n)
			wantValue, _, wantErr := Decode(input)
			if gotOver := wantErr != nil && !errors.Is(wantErr, ErrIncomplete); gotOver != in.over(n) {
				t.Fatalf("%s of %d bytes: Decode() error = %v, but the line is over the bound: %v", in.name, n, wantErr, in.over(n))
			}

			for _, rd := range readers {
				rd := rd
				t.Run(fmt.Sprintf("%s, %d bytes, %s", in.name, n, rd.name), func(t *testing.T) {
					got, err := NewParser(rd.wrap(bytes.NewReader(input))).Parse()
					switch {
					case errors.Is(wantErr, ErrIncomplete):
						if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
							t.Fatalf("Parse() = %v, %v; want the end of the stream, as Decode() reports ErrIncomplete", got, err)
						}
					case wantErr != nil:
						if err == nil || err.Error() != wantErr.Error() {
							t.Fatalf("Parse() = %v, %v; want error %q", got, err, wantErr)
						}
					case err != nil:
						t.Fatalf("Parse() error = %v, want %v", err, wantValue)
					case !reflect.DeepEqual(got, wantValue):
						t.Fatalf("Parse() = %#v, want %#v", got, wantValue)
					}
				})
			}
		}
	}
}

func TestNewParserReusesBufferedReader(t *testing.T) {
	reader := bufio.NewReader(bytes.NewReader([]byte("+OK\r\n")))
	parser := NewParser(reader)

	if parser.reader != reader {
		t.Fatal("NewParser() did not reuse the provided buffered reader")
	}
}

func stringsReader(value string) io.Reader {
	return &chunkReader{chunks: [][]byte{[]byte(value)}}
}

func assertBulkString(t *testing.T, value Value, want string) {
	t.Helper()

	bulk, ok := value.(BulkString)
	if !ok {
		t.Fatalf("value type = %T, want BulkString", value)
	}
	if bulk.Null {
		t.Fatal("bulk string unexpectedly null")
	}
	if string(bulk.Data) != want {
		t.Fatalf("bulk string = %q, want %q", string(bulk.Data), want)
	}
}

// TestParserAndDecodeShareNestingBound pins the streaming Parser and the
// buffered Decode to the same nesting limit. Parser recurses per level, so
// without this bound a chain of "*1\r\n" headers grows the goroutine stack
// until the runtime aborts the process, which recover cannot intercept.
func TestParserAndDecodeShareNestingBound(t *testing.T) {
	tests := []struct {
		name    string
		depth   int
		wantErr bool
	}{
		{name: "at the limit", depth: maxNestingDepth - 1, wantErr: false},
		{name: "past the limit", depth: maxNestingDepth + 1, wantErr: true},
		{name: "far past the limit", depth: 100_000, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			frame := append(bytes.Repeat([]byte("*1\r\n"), tt.depth), []byte("+ok\r\n")...)

			_, parserErr := NewParser(bytes.NewReader(frame)).Parse()
			if (parserErr != nil) != tt.wantErr {
				t.Fatalf("Parser.Parse() error = %v, wantErr %v", parserErr, tt.wantErr)
			}

			_, _, decodeErr := Decode(frame)
			if (decodeErr != nil) != tt.wantErr {
				t.Fatalf("Decode() error = %v, wantErr %v", decodeErr, tt.wantErr)
			}
		})
	}
}

// readSignal reports each Read on the wrapped reader, so a test can tell when the
// parser has consumed what it was given and is blocked asking for more.
type readSignal struct {
	io.Reader
	reads chan struct{}
}

func (r readSignal) Read(p []byte) (int, error) {
	r.reads <- struct{}{}
	return r.Reader.Read(p)
}

// TestParserAllocatesInProportionToBytesReceived guards against a forged bulk
// length reserving memory up front: the header alone, sent by a client that then
// stalls, must not cost anything close to the declared 512 MiB.
func TestParserAllocatesInProportionToBytesReceived(t *testing.T) {
	pr, pw := io.Pipe()
	defer func() { _ = pr.Close() }()
	reads := make(chan struct{}, 8)

	parsed := make(chan error, 1)
	go func() {
		_, err := NewParser(readSignal{Reader: pr, reads: reads}).Parse()
		parsed <- err
	}()

	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	if _, err := pw.Write([]byte("*1\r\n$536870000\r\n")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	// The first Read delivered the header; the second is the parser asking for the
	// payload, so by then it has sized (or not sized) its payload buffer.
	for i := 0; i < 2; i++ {
		select {
		case <-reads:
		case <-time.After(10 * time.Second):
			t.Fatal("the parser never asked for the bulk payload")
		}
	}

	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	if grown := after.TotalAlloc - before.TotalAlloc; grown > 16<<20 {
		t.Fatalf("parsing only a bulk header allocated %d MiB, want it independent of the declared 512 MiB length", grown>>20)
	}

	pw.CloseWithError(io.ErrUnexpectedEOF)
	if err := <-parsed; err == nil {
		t.Fatal("Parse() error = nil, want an error once the stream ends before the payload")
	}
}

func TestParserBulkPayloadAcrossAllocationBoundaries(t *testing.T) {
	// Payloads larger than the first allocation are read in growing steps; check
	// the sizes around every step keep their content, exact length and terminator.
	for _, size := range []int{0, 1, bulkFirstAllocation - 1, bulkFirstAllocation, bulkFirstAllocation + 1, 3*bulkFirstAllocation + 7} {
		payload := make([]byte, size)
		for i := range payload {
			payload[i] = byte(i*31 + i>>8)
		}
		input := fmt.Sprintf("$%d\r\n%s\r\n+next\r\n", size, payload)

		for name, reader := range map[string]io.Reader{
			"whole":     strings.NewReader(input),
			"half-read": iotest.HalfReader(strings.NewReader(input)),
			"one-byte":  iotest.OneByteReader(strings.NewReader(input)),
		} {
			if name == "one-byte" && size > bulkFirstAllocation {
				continue // millions of one-byte reads add nothing the half-reader does not cover
			}
			parser := NewParser(reader)
			value, err := parser.Parse()
			if err != nil {
				t.Fatalf("size %d (%s): Parse() error = %v", size, name, err)
			}
			bulk, ok := value.(BulkString)
			if !ok || !bytes.Equal(bulk.Data, payload) || len(bulk.Data) != size {
				t.Fatalf("size %d (%s): Parse() = %d bytes (ok=%v), want the exact %d byte payload", size, name, len(bulk.Data), ok, size)
			}
			if next, err := parser.Parse(); err != nil || next != (SimpleString{Value: "next"}) {
				t.Fatalf("size %d (%s): the frame after the payload = (%#v, %v), want +next", size, name, next, err)
			}
		}
	}
}

func TestParserTornBulkPayloadReportsUnexpectedEOF(t *testing.T) {
	// AOF replay treats io.EOF as a clean end of file and io.ErrUnexpectedEOF as a
	// torn trailing record, so a payload cut off at any point, including exactly
	// where the buffer grows, must report the latter.
	const declared = 3*bulkFirstAllocation + 7
	for _, received := range []int{1, bulkFirstAllocation - 1, bulkFirstAllocation, bulkFirstAllocation + 1, 2 * bulkFirstAllocation, declared - 1} {
		input := fmt.Sprintf("$%d\r\n%s", declared, strings.Repeat("x", received))
		_, err := NewParser(strings.NewReader(input)).Parse()
		if !errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			t.Fatalf("payload cut off after %d of %d bytes: error = %v, want io.ErrUnexpectedEOF and not io.EOF", received, declared, err)
		}
	}
}
