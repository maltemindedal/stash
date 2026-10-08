package protocol

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

// The Parser serves default mode, AOF replay and a Replica's link to its Master;
// the Decoder serves --event-loop mode. They implement one grammar twice, and
// nothing but these tests notices when the copies drift apart. A Frame is the
// bytes of one complete RESP value, from its type byte to its final CRLF. Each
// test here runs both decoders on the same bytes, cut into pieces in several
// ways, and compares
//
//   - the values, and where in the stream each one ends;
//   - how the bytes ended: between Frames (clean), inside one (torn), or in a
//     protocol error, and then the error text, which both modes send to the
//     client;
//   - the Decoder against itself, so its answer does not depend on how the
//     stream arrived, and does not change when its buffer is overwritten after
//     each Frame, as the Connection state machine does.
//
// A protocol change belongs in both decoders, and this is what fails when it is
// made in only one. To hunt further than the fixed corpus does (left to minimize
// the inputs it finds, the fuzzer spent a whole minute on them here and ran
// little else, and a failure prints its whole input anyway):
//
//	go test ./internal/protocol -run '^$' -fuzz '^FuzzParserMatchesDecoder$' -fuzztime 60s -fuzzminimizetime 0s
//	STASH_DECODER_FUZZ_CASES=2000000 go test ./internal/protocol -run '^TestParserMatchesDecoderOnMutatedFrames$'

// eventLoopFrameLimit is the read buffer the Connection state machine gives one
// Frame (defaultMaxReadBuffer in internal/server). It is the one place the
// Decoder and the Parser are allowed to disagree; see streamEndFrameBound.
// This package cannot import the machine, so TestConnMachineFeedRejectsHostileFrames
// in internal/server pins the edges of that range against the real one: if the
// machine's limit or check changes, that test fails and this model, and the
// "Protocol errors" section of docs/reference/commands.md, change with it.
const eventLoopFrameLimit = 512 * 1024 * 1024

// differentialCasesEnv names the environment variable that sets how many mutated
// inputs TestParserMatchesDecoderOnMutatedFrames runs.
const differentialCasesEnv = "STASH_DECODER_FUZZ_CASES"

// differentialDefaultCases is the corpus that runs without the variable: about a
// second under -race on a 4-core machine. The first n cases are the same for any
// n, so a larger value extends the corpus rather than replacing it.
const differentialDefaultCases = 2500

// differentialSeed fixes the corpus.
const differentialSeed = 49

// streamEnd is how a run over a stream's bytes stopped.
type streamEnd int

const (
	// streamEndClean: the bytes ran out between two Frames.
	streamEndClean streamEnd = iota
	// streamEndTorn: the bytes ran out inside a Frame.
	streamEndTorn
	// streamEndError: a permanent protocol error.
	streamEndError
	// streamEndFrameBound is the single exception to "the Parser and the Decoder
	// agree", and only the Decoder side can report it.
	//
	// The Connection state machine closes a connection whose read buffer would
	// have to hold more than eventLoopFrameLimit bytes for one Frame, headers and
	// CRLFs included, as soon as the Decoder's byte hint says so. A bulk string
	// header that declares exactly the bulk limit, or a few bytes less, is
	// therefore refused in --event-loop mode: a bare $ header of 536,870,899 to
	// 536,870,912 bytes, and a lower range inside an array, since the elements
	// before the header count too. The Parser has no total bound on a Frame and
	// starts reading the payload, so it ends torn once the test's few bytes run
	// out. That bound is outside the RFC (#49 lists a total Frame bound for
	// default mode as out of scope), so the test accepts the Parser's torn ending
	// for it and nothing else.
	streamEndFrameBound
)

func (e streamEnd) String() string {
	switch e {
	case streamEndClean:
		return "clean"
	case streamEndTorn:
		return "torn"
	case streamEndError:
		return "error"
	case streamEndFrameBound:
		return "frame bound"
	default:
		return "unknown"
	}
}

// streamOutcome is everything a run over a stream's bytes produced.
type streamOutcome struct {
	values []Value
	// offsets[i] is the stream position just after values[i].
	offsets []int
	end     streamEnd
	// err is the text of the protocol error when end is streamEndError.
	err string
}

func (o streamOutcome) String() string {
	text := fmt.Sprintf("%d values, ended %s", len(o.values), o.end)
	if o.end == streamEndError {
		text += fmt.Sprintf(" with %q", o.err)
	}
	return text
}

// split is one way of cutting a stream into the pieces it arrives in.
type split struct {
	name string
	// sizes are the piece sizes in order, repeating when they run out.
	sizes []int
	// bufferSize is the size of the bufio.Reader the Parser reads through. Small
	// ones push lines through bufio's ErrBufferFull path.
	bufferSize int
}

func (s split) String() string {
	return fmt.Sprintf("%s (pieces %v, %d byte bufio.Reader)", s.name, s.sizes, s.bufferSize)
}

// splitReader hands out data in the piece sizes it is given and counts the bytes
// it has handed out.
type splitReader struct {
	data  []byte
	sizes []int
	turn  int
	pos   int
}

func (r *splitReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n := min(r.sizes[r.turn%len(r.sizes)], len(p), len(r.data)-r.pos)
	r.turn++
	copy(p, r.data[r.pos:r.pos+n])
	r.pos += n
	return n, nil
}

// decodeStream feeds input to a Decoder piece by piece, as the Connection state
// machine does: bytes are appended to a buffer, every complete Frame is taken off
// its front, and the buffer a Frame came from is reused. An unfinished Frame is
// decoded again after each piece, so the Decoder's place in it is exercised.
func decodeStream(input []byte, limits Limits, sizes []int) streamOutcome {
	var (
		out      streamOutcome
		decoder  Decoder
		buf      []byte
		fed      int
		consumed int
		turn     int
	)
	decoder.SetLimits(limits)

	for {
		if fed < len(input) {
			n := min(sizes[turn%len(sizes)], len(input)-fed)
			turn++
			buf = append(buf, input[fed:fed+n]...)
			fed += n
		}

		for {
			value, n, err := decoder.Decode(buf)
			if err == nil {
				// The machine compacts its buffer over what it consumed. Poison
				// the bytes so a value that still pointed into them would change.
				for i := range buf[:n] {
					buf[i] = 0xAA
				}
				buf = buf[n:]
				consumed += n
				out.values = append(out.values, value)
				out.offsets = append(out.offsets, consumed)
				continue
			}

			if !errors.Is(err, ErrIncomplete) {
				out.end, out.err = streamEndError, err.Error()
				return out
			}
			var incomplete *IncompleteError
			if errors.As(err, &incomplete) && incomplete.Need > eventLoopFrameLimit {
				out.end = streamEndFrameBound
				return out
			}
			break
		}

		if fed == len(input) {
			if len(buf) == 0 {
				out.end = streamEndClean
			} else {
				out.end = streamEndTorn
			}
			return out
		}
	}
}

// parseStream reads input through a Parser. The Parser reports the end of a
// stream as an error that wraps io.EOF or io.ErrUnexpectedEOF. Here
// streamEndClean means "ended with io.EOF itself" and every other end of stream
// is streamEndTorn; diffParserFromDecoder says when that can be the same end as
// the Decoder's streamEndTorn.
func parseStream(input []byte, limits Limits, s split) streamOutcome {
	source := &splitReader{data: input, sizes: s.sizes}
	buffered := bufio.NewReaderSize(source, s.bufferSize)
	parser := NewParser(buffered)
	parser.SetLimits(limits)

	var out streamOutcome
	for {
		value, err := parser.Parse()
		if err == nil {
			out.values = append(out.values, value)
			// What the Parser has taken from the reader, less what it still
			// holds, is how far into the stream the value ended.
			out.offsets = append(out.offsets, source.pos-buffered.Buffered())
			continue
		}

		switch {
		case errors.Is(err, io.EOF) && errors.Unwrap(err) == nil:
			out.end = streamEndClean
		case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
			out.end = streamEndTorn
		default:
			out.end, out.err = streamEndError, err.Error()
		}
		return out
	}
}

// diffParserFromDecoder describes how the Parser's outcome differs from the
// Decoder's for the same input, or returns "" when they agree.
func diffParserFromDecoder(input []byte, dec, par streamOutcome) string {
	if len(dec.values) != len(par.values) {
		return fmt.Sprintf("the Decoder returned %d values and the Parser %d (%s; %s)", len(dec.values), len(par.values), dec, par)
	}
	for i := range dec.values {
		if !reflect.DeepEqual(dec.values[i], par.values[i]) {
			return fmt.Sprintf("value %d: the Decoder returned %#v and the Parser %#v", i, dec.values[i], par.values[i])
		}
		if dec.offsets[i] != par.offsets[i] {
			return fmt.Sprintf("value %d ends at byte %d for the Decoder and %d for the Parser", i, dec.offsets[i], par.offsets[i])
		}
	}

	switch dec.end {
	case streamEndClean:
		if par.end != streamEndClean {
			return fmt.Sprintf("the Decoder ended between Frames and the Parser did not (%s)", par)
		}
	case streamEndTorn:
		// A torn Frame is a Torn tail to AOF replay and a disconnect to a
		// connection, whichever of its errors the Parser wraps the end in.
		if par.end == streamEndError {
			return fmt.Sprintf("the Decoder ended inside a Frame and the Parser did not (%s)", par)
		}
		// The Parser has one quirk here. A stream that stops inside the first
		// line of a top-level Frame (a bare "*0\r") gives it the same bare io.EOF
		// as one that stops between Frames, because it has read nothing it could
		// name. Once that line is complete, everything it reads is wrapped: a
		// torn payload is io.ErrUnexpectedEOF and a missing element is reported
		// as that element. So a bare io.EOF is accepted for a torn tail only when
		// the tail has no LF after its type byte.
		if par.end == streamEndClean {
			tail := input[lastOffset(dec):]
			if bytes.IndexByte(tail[1:], '\n') >= 0 {
				return fmt.Sprintf("the Decoder ended inside a Frame whose first line is complete, and the Parser ended with a bare io.EOF, as if between Frames (tail %q)", tail)
			}
		}
	case streamEndFrameBound:
		if par.end != streamEndTorn {
			return fmt.Sprintf("the Decoder refused the Frame for its size and the Parser did not end waiting for the payload (%s)", par)
		}
	case streamEndError:
		if par.end != streamEndError || par.err != dec.err {
			return fmt.Sprintf("the Decoder ended with an error and the Parser did not agree (%s; %s)", dec, par)
		}
	}
	return ""
}

// lastOffset is where the last complete Frame of an outcome ends.
func lastOffset(o streamOutcome) int {
	if len(o.offsets) == 0 {
		return 0
	}
	return o.offsets[len(o.offsets)-1]
}

// checkParserMatchesDecoder runs both decoders over input under every split. It
// returns the Decoder's outcome for the whole input and a description of the
// first disagreement, or "" when there is none.
func checkParserMatchesDecoder(input []byte, limits Limits, splits []split) (streamOutcome, string) {
	whole := decodeStream(input, limits, []int{max(len(input), 1)})

	for _, s := range splits {
		if got := decodeStream(input, limits, s.sizes); !reflect.DeepEqual(got, whole) {
			return whole, fmt.Sprintf("the Decoder's outcome depends on how the stream is cut: whole input gives %s, %s gives %s", whole, s, got)
		}
		if diff := diffParserFromDecoder(input, whole, parseStream(input, limits, s)); diff != "" {
			return whole, fmt.Sprintf("%s: %s", s, diff)
		}
	}
	return whole, ""
}

// differentialLimits are the limits a case runs under: none, the ones a client
// that has not authenticated gets, and two small enough to be reached by tiny
// inputs.
var differentialLimits = []Limits{
	{},
	UnauthenticatedLimits,
	{MaxArrayElements: 2, MaxBulkStringLength: 4},
	{MaxArrayElements: 1},
}

// standardSplits cuts an input of n bytes three ways: whole, in the smallest
// pieces that keep the run quick, and in pieces of varying size. Where inputs are
// large the pieces grow with them, since the Decoder looks at an unfinished
// line again after every piece.
func standardSplits(rng *rand.Rand, n int) []split {
	scale := 1 + n/1024
	sizes := make([]int, 8)
	for i := range sizes {
		sizes[i] = (1 + rng.Intn(13)) * scale
	}
	return []split{
		{name: "whole input", sizes: []int{max(n, 1)}, bufferSize: 4096},
		{name: "smallest pieces", sizes: []int{scale}, bufferSize: 16},
		{name: "varying pieces", sizes: sizes, bufferSize: []int{16, 17, 64}[rng.Intn(3)]},
	}
}

// differentialSeeds are Frames, valid and not, that every test here starts from.
func differentialSeeds() []string {
	return []string{
		// Valid, one of each shape.
		"+OK\r\n", "+\r\n", "-ERR boom\r\n", "-\r\n",
		":0\r\n", ":-7\r\n", ":+42\r\n", ":9223372036854775807\r\n", ":-9223372036854775808\r\n",
		"#t\r\n", "#f\r\n", "_\r\n",
		"$5\r\nhello\r\n", "$0\r\n\r\n", "$-1\r\n", "$4\r\n\r\n\r\n\r\n",
		"*0\r\n", "*-1\r\n", "*1\r\n*0\r\n", "*2\r\n*1\r\n:1\r\n+done\r\n",
		"*5\r\n+OK\r\n-ERR\r\n:1\r\n_\r\n#t\r\n",
		"*3\r\n$3\r\nGET\r\n$-1\r\n*-1\r\n",
		string(commandFrame("SET", "key", "value")),
		string(commandFrame("SET", "key", strings.Repeat("v", 5000))),
		"+one\r\n+two\r\n:3\r\n",
		// Malformed.
		"?\r\n", "GET key\r\n", "\r\n", "+OK\n", "+OK\r\r\n",
		":\r\n", ":+\r\n", ":12a\r\n", ":9223372036854775808\r\n", "#x\r\n", "_x\r\n",
		"$abc\r\n", "$-2\r\n", "$5\r\nhell\r\n", "$3\r\nabcde", "$3\r\nabc\n\r\n",
		"*-2\r\n", "*x\r\n", "*2\r\n+a\r\n?\r\n", "*2\r\n:1\r\n*1\r\n:x\r\n",
		// At the limits.
		"*1048576\r\n", "*1048577\r\n", "*99999999999999999999\r\n",
		"$536870898\r\n", "$536870899\r\n", "$536870912\r\n", "$536870913\r\n",
		"$9223372036854775807\r\n", "$99999999999999999999\r\n",
		"*2\r\n$3\r\nSET\r\n$536870912\r\n", "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$9223372036854775807\r\n",
		strings.Repeat("*1\r\n", maxNestingDepth) + ":1\r\n",
		strings.Repeat("*1\r\n", maxNestingDepth+1) + ":1\r\n",
	}
}

func TestParserMatchesDecoderAtEveryCutOfTheSeedFrames(t *testing.T) {
	rng := rand.New(rand.NewSource(differentialSeed))
	for _, seed := range differentialSeeds() {
		if len(seed) > 128 {
			continue
		}
		for cut := 0; cut <= len(seed); cut++ {
			input := []byte(seed[:cut])
			for _, limits := range differentialLimits {
				if _, diff := checkParserMatchesDecoder(input, limits, standardSplits(rng, len(input))); diff != "" {
					t.Fatalf("%s\nlimits %+v\ninput %s", diff, limits, quoteInput(input))
				}
			}
		}
	}
}

// TestParserMatchesDecoderAtTheBounds covers each bound the two decoders have
// to share, on both sides of it: the places they have already disagreed.
func TestParserMatchesDecoderAtTheBounds(t *testing.T) {
	// paddedLine is a line of n bytes after the type byte, CRLF included, whose
	// number is padded with zeros so that the line is as long as the test needs.
	paddedLine := func(prefix string, n int, digit string) string {
		return prefix + strings.Repeat("0", n-2-len(digit)) + digit + "\r\n"
	}

	tests := []struct {
		name  string
		input string
		want  streamEnd
	}{
		// A terminated line may fill maxLineLength bytes, CRLF included. An
		// unterminated one may not: its LF would have to be among them.
		{name: "simple string line at the bound", input: "+" + strings.Repeat("a", maxLineLength-2) + "\r\n", want: streamEndClean},
		{name: "simple string line one byte over", input: "+" + strings.Repeat("a", maxLineLength-1) + "\r\n", want: streamEndError},
		{name: "simple string line without its LF, one byte short", input: "+" + strings.Repeat("a", maxLineLength-1), want: streamEndTorn},
		{name: "simple string line without its LF, at the bound", input: "+" + strings.Repeat("a", maxLineLength), want: streamEndError},
		{name: "integer line at the bound", input: paddedLine(":", maxLineLength, "1"), want: streamEndClean},
		{name: "integer line one byte over", input: paddedLine(":", maxLineLength+1, "1"), want: streamEndError},
		{name: "array header line at the bound", input: paddedLine("*", maxLineLength, "0"), want: streamEndClean},
		{name: "array header line one byte over", input: paddedLine("*", maxLineLength+1, "0"), want: streamEndError},
		{name: "bulk header line at the bound", input: paddedLine("*1\r\n$", maxLineLength, "0") + "\r\n", want: streamEndClean},
		{name: "bulk header line one byte over", input: paddedLine("*1\r\n$", maxLineLength+1, "0") + "\r\n", want: streamEndError},

		// Nesting.
		{name: "nesting at the bound", input: strings.Repeat("*1\r\n", maxNestingDepth) + ":1\r\n", want: streamEndClean},
		{name: "nesting one level over the bound", input: strings.Repeat("*1\r\n", maxNestingDepth+1) + ":1\r\n", want: streamEndError},

		// Array lengths: the header is judged before any element arrives.
		{name: "array of the most elements", input: "*1048576\r\n", want: streamEndTorn},
		{name: "array of one element too many", input: "*1048577\r\n", want: streamEndError},
		{name: "array length that does not fit an int", input: "*99999999999999999999\r\n", want: streamEndError},

		// Bulk lengths. Both decoders refuse a header over the limit as soon as
		// it arrives, including one that would overflow an int when added up.
		{name: "bulk string of the most bytes, without its payload", input: "$536870898\r\n", want: streamEndTorn},
		{name: "bulk string of one byte too many", input: "$536870913\r\n", want: streamEndError},
		{name: "bulk string of one byte too many in a command", input: "*2\r\n$3\r\nSET\r\n$536870913\r\n", want: streamEndError},
		{name: "bulk string of MaxInt bytes", input: "$9223372036854775807\r\n", want: streamEndError},

		// The exception. The Decoder's byte hint for a bare $ header is the bytes
		// before the payload (the type byte, the digits and a CRLF) plus the
		// payload and its CRLF, and the event loop's read buffer holds 512 MiB.
		// For $536870898 that is exactly the 512 MiB, for $536870899 one byte
		// more, and the Parser accepts both.
		{name: "bulk header whose Frame just fits the event loop's buffer", input: "$536870898\r\n", want: streamEndTorn},
		{name: "bulk header whose Frame is one byte too large for it", input: "$536870899\r\n", want: streamEndFrameBound},
		{name: "bulk header declaring the bulk limit", input: "$536870912\r\n", want: streamEndFrameBound},
		{name: "bulk header in a command whose Frame is one byte too large for it", input: "*2\r\n$3\r\nSET\r\n$536870886\r\n", want: streamEndFrameBound},
		{name: "bulk header in a command whose Frame just fits", input: "*2\r\n$3\r\nSET\r\n$536870885\r\n", want: streamEndTorn},
	}

	rng := rand.New(rand.NewSource(differentialSeed))
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := []byte(tt.input)
			whole, diff := checkParserMatchesDecoder(input, Limits{}, standardSplits(rng, len(input)))
			if diff != "" {
				t.Fatal(diff)
			}
			if whole.end != tt.want {
				t.Fatalf("the Decoder's outcome was %s, want it to end %s", whole, tt.want)
			}
		})
	}
}

// TestParserMatchesDecoderOnMutatedFrames runs the fixed corpus of inputs built
// from the seed Frames and from random ones, then damaged: bytes changed,
// inserted, removed and moved, numbers swapped for the ones that sit on a limit,
// and the end cut off. -short skips it.
func TestParserMatchesDecoderOnMutatedFrames(t *testing.T) {
	if testing.Short() {
		t.Skip("-short skips the mutated corpus; FuzzParserMatchesDecoder still runs the seed Frames")
	}
	cases := differentialDefaultCases
	if raw := os.Getenv(differentialCasesEnv); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			t.Fatalf("%s = %q, want a positive number of cases", differentialCasesEnv, raw)
		}
		cases = n
	}

	generator := &inputGenerator{rng: rand.New(rand.NewSource(differentialSeed)), seeds: differentialSeeds()}
	ends := map[streamEnd]int{}
	var withValues int
	for i := 0; i < cases; i++ {
		input := generator.next()
		limits := differentialLimits[generator.rng.Intn(len(differentialLimits))]

		whole, diff := checkParserMatchesDecoder(input, limits, standardSplits(generator.rng, len(input)))
		if diff != "" {
			t.Fatalf("case %d of the corpus: %s\nlimits %+v\ninput %s\nadd the input to differentialSeeds once it is fixed", i, diff, limits, quoteInput(input))
		}
		ends[whole.end]++
		if len(whole.values) > 0 {
			withValues++
		}
	}

	t.Logf("%d cases; ended clean %d, torn %d, error %d, frame bound %d; %d returned a value",
		cases, ends[streamEndClean], ends[streamEndTorn], ends[streamEndError], ends[streamEndFrameBound], withValues)
	if cases >= differentialDefaultCases {
		// A corpus that stopped producing one kind of input would pass without
		// testing it.
		for _, end := range []streamEnd{streamEndClean, streamEndTorn, streamEndError} {
			if ends[end] < cases/50 {
				t.Errorf("only %d of %d cases ended %s; the generator no longer covers it", ends[end], cases, end)
			}
		}
		if withValues < cases/4 {
			t.Errorf("only %d of %d cases returned a value", withValues, cases)
		}
	}
}

// FuzzParserMatchesDecoder is the open-ended version of the corpus test. Without
// -fuzz, go test runs its seeds: every seed Frame under each set of limits.
func FuzzParserMatchesDecoder(f *testing.F) {
	for i, seed := range differentialSeeds() {
		f.Add([]byte(seed), uint8(i), uint8(i*7))
	}

	f.Fuzz(func(t *testing.T, input []byte, limitsChoice uint8, pieceSize uint8) {
		limits := differentialLimits[int(limitsChoice)%len(differentialLimits)]
		piece := 1 + int(pieceSize)%64
		splits := []split{
			{name: "whole input", sizes: []int{max(len(input), 1)}, bufferSize: 4096},
			{name: "fixed pieces", sizes: []int{piece}, bufferSize: 16},
			{name: "fixed pieces, large buffer", sizes: []int{piece}, bufferSize: 4096},
		}
		if _, diff := checkParserMatchesDecoder(input, limits, splits); diff != "" {
			t.Fatalf("%s\nlimits %+v\ninput %s", diff, limits, quoteInput(input))
		}
	})
}

// quoteInput prints an input in full up to a size that is still readable.
func quoteInput(input []byte) string {
	const shown = 2000
	if len(input) <= shown {
		return strconv.Quote(string(input))
	}
	return fmt.Sprintf("%s... (%d bytes in all)", strconv.Quote(string(input[:shown])), len(input))
}

// inputGenerator produces the corpus: a stream of one to four Frames, taken from
// the seeds or made up, then damaged. Its output depends only on the order of the
// calls.
type inputGenerator struct {
	rng   *rand.Rand
	seeds []string
}

// interestingBytes are the bytes a mutation prefers: the ones the grammar is
// made of.
var interestingBytes = []byte("\r\n$*+-:#_0123456789 tf\x00\xff")

// interestingNumbers replace a number in the input: the ones just inside and
// outside each bound, and the ones a parser is likely to mishandle.
var interestingNumbers = []string{
	"-1", "-2", "0", "1", "2", "3", "+5", "-0", "007", "127", "128", "4096", "65535", "65536",
	"1048575", "1048576", "1048577", "2147483647", "2147483648", "4294967296",
	"536870897", "536870898", "536870899", "536870911", "536870912", "536870913",
	"9223372036854775806", "9223372036854775807", "9223372036854775808",
	"-9223372036854775808", "-9223372036854775809", "99999999999999999999",
}

func (g *inputGenerator) next() []byte {
	var input []byte
	for frames := 1 + g.rng.Intn(4); frames > 0; frames-- {
		if g.rng.Intn(4) == 0 {
			input = append(input, g.seeds[g.rng.Intn(len(g.seeds))]...)
		} else {
			input = g.appendFrame(input, 0)
		}
	}
	for mutations := g.rng.Intn(3); mutations > 0 && len(input) > 0; mutations-- {
		input = g.mutate(input)
	}
	return input
}

// appendFrame appends one valid Frame of a random shape.
func (g *inputGenerator) appendFrame(dst []byte, depth int) []byte {
	kinds := 9
	if depth >= 3 {
		kinds = 8
	}
	switch g.rng.Intn(kinds) {
	case 0:
		return append(append(append(dst, '+'), g.text(12)...), "\r\n"...)
	case 1:
		return append(append(append(dst, '-'), g.text(12)...), "\r\n"...)
	case 2:
		return append(append(append(dst, ':'), strconv.FormatInt(g.integer(), 10)...), "\r\n"...)
	case 3:
		payload := g.payload()
		dst = append(append(append(dst, '$'), strconv.Itoa(len(payload))...), "\r\n"...)
		return append(append(dst, payload...), "\r\n"...)
	case 4:
		return append(dst, "$-1\r\n"...)
	case 5:
		return append(dst, []string{"#t\r\n", "#f\r\n"}[g.rng.Intn(2)]...)
	case 6:
		return append(dst, "_\r\n"...)
	case 7:
		return append(dst, []string{"*-1\r\n", "*0\r\n"}[g.rng.Intn(2)]...)
	default:
		count := 1 + g.rng.Intn(4)
		dst = append(append(append(dst, '*'), strconv.Itoa(count)...), "\r\n"...)
		for i := 0; i < count; i++ {
			dst = g.appendFrame(dst, depth+1)
		}
		return dst
	}
}

// text is a line's worth of bytes with no CR or LF in it.
func (g *inputGenerator) text(most int) []byte {
	text := make([]byte, g.rng.Intn(most+1))
	for i := range text {
		text[i] = "abcXYZ 019:-+_$*#"[g.rng.Intn(17)]
	}
	return text
}

func (g *inputGenerator) integer() int64 {
	switch g.rng.Intn(4) {
	case 0:
		return int64(g.rng.Intn(10))
	case 1:
		return -int64(g.rng.Intn(1000))
	case 2:
		return g.rng.Int63()
	default:
		return -g.rng.Int63()
	}
}

// payload is a bulk string's bytes, CR and LF and the type bytes among them.
func (g *inputGenerator) payload() []byte {
	payload := make([]byte, []int{0, 1, 2, 3, 5, 10, 40, 300}[g.rng.Intn(8)])
	for i := range payload {
		if g.rng.Intn(3) == 0 {
			payload[i] = interestingBytes[g.rng.Intn(len(interestingBytes))]
		} else {
			payload[i] = byte('a' + g.rng.Intn(26))
		}
	}
	return payload
}

// mutate damages input in one random way. input is not empty.
func (g *inputGenerator) mutate(input []byte) []byte {
	pick := func() byte { return interestingBytes[g.rng.Intn(len(interestingBytes))] }

	switch g.rng.Intn(8) {
	case 0: // change a byte
		input[g.rng.Intn(len(input))] = pick()
	case 1: // insert bytes
		at := g.rng.Intn(len(input) + 1)
		mutated := append([]byte(nil), input[:at]...)
		for n := 1 + g.rng.Intn(2); n > 0; n-- {
			mutated = append(mutated, pick())
		}
		input = append(mutated, input[at:]...)
	case 2: // remove a span
		at := g.rng.Intn(len(input))
		end := min(len(input), at+1+g.rng.Intn(3))
		input = append(input[:at:at], input[end:]...)
	case 3: // cut the end off
		input = input[:g.rng.Intn(len(input))]
	case 4: // replace a number
		input = g.replaceNumber(input)
	case 5: // repeat a span
		from := g.rng.Intn(len(input))
		to := min(len(input), from+1+g.rng.Intn(16))
		span := input[from:to]
		input = append(input[:to:to], append(bytes.Clone(span), input[to:]...)...)
	case 6: // swap two bytes
		i, j := g.rng.Intn(len(input)), g.rng.Intn(len(input))
		input[i], input[j] = input[j], input[i]
	default: // trail off into something else
		tail := []string{"\r\n", "\r", "\n", "$", "*1\r\n", "garbage\r\n", "$3\r\nab"}[g.rng.Intn(7)]
		input = append(input, tail...)
	}
	return input
}

// replaceNumber swaps one run of digits for a number from interestingNumbers.
func (g *inputGenerator) replaceNumber(input []byte) []byte {
	var runs [][2]int
	for i := 0; i < len(input); {
		if input[i] < '0' || input[i] > '9' {
			i++
			continue
		}
		end := i
		for end < len(input) && input[end] >= '0' && input[end] <= '9' {
			end++
		}
		runs = append(runs, [2]int{i, end})
		i = end
	}
	if len(runs) == 0 {
		return input
	}

	run := runs[g.rng.Intn(len(runs))]
	number := interestingNumbers[g.rng.Intn(len(interestingNumbers))]
	return append(input[:run[0]:run[0]], append([]byte(number), input[run[1]:]...)...)
}
