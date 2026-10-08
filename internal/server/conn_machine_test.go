package server

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/maltemindedal/stash/internal/protocol"
)

func echoRunner(t *testing.T) (ConnCommandRunner, *[]protocol.Value) {
	t.Helper()

	executed := &[]protocol.Value{}
	run := func(_ context.Context, request protocol.Value) ([]protocol.Value, error) {
		*executed = append(*executed, request)
		return []protocol.Value{protocol.SimpleString{Value: "OK"}}, nil
	}
	return run, executed
}

func flushAll(t *testing.T, machine *ConnMachine) []byte {
	t.Helper()

	var out bytes.Buffer
	for machine.HasPendingOutput() {
		if err := machine.Flush(&out); err != nil {
			t.Fatalf("Flush() error = %v", err)
		}
	}
	return out.Bytes()
}

// shortWriter accepts at most limit bytes per Write call to model a transport
// that applies backpressure.
type shortWriter struct {
	buf   bytes.Buffer
	limit int
}

func (w *shortWriter) Write(p []byte) (int, error) {
	if len(p) > w.limit {
		p = p[:w.limit]
	}
	return w.buf.Write(p)
}

func TestConnMachinePartialReads(t *testing.T) {
	run, executed := echoRunner(t)
	machine := NewConnMachine(nil)

	request := []byte("*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
	for i, b := range request {
		if err := machine.Feed([]byte{b}); err != nil {
			t.Fatalf("Feed() error = %v", err)
		}
		if i < len(request)-1 && machine.PendingRequests() != 0 {
			t.Fatalf("PendingRequests() = %d after %d bytes, want 0", machine.PendingRequests(), i+1)
		}
	}

	if machine.PendingRequests() != 1 {
		t.Fatalf("PendingRequests() = %d, want 1", machine.PendingRequests())
	}
	if err := machine.ProcessPending(context.Background(), run); err != nil {
		t.Fatalf("ProcessPending() error = %v", err)
	}
	if len(*executed) != 1 {
		t.Fatalf("executed %d requests, want 1", len(*executed))
	}

	if got, want := flushAll(t, machine), "+OK\r\n"; string(got) != want {
		t.Fatalf("flushed output = %q, want %q", got, want)
	}
	if machine.State() != ConnStateActive {
		t.Fatalf("State() = %d, want ConnStateActive", machine.State())
	}
}

func TestConnMachineCommandBoundaries(t *testing.T) {
	run, executed := echoRunner(t)
	machine := NewConnMachine(nil)

	// Two complete requests plus the beginning of a third in one readable chunk.
	if err := machine.Feed([]byte("*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n*2\r\n$4\r\nECHO\r\n$3\r\nhe")); err != nil {
		t.Fatalf("Feed() error = %v", err)
	}
	if machine.PendingRequests() != 2 {
		t.Fatalf("PendingRequests() = %d, want 2", machine.PendingRequests())
	}

	if err := machine.Feed([]byte("y\r\n")); err != nil {
		t.Fatalf("Feed() error = %v", err)
	}
	if machine.PendingRequests() != 3 {
		t.Fatalf("PendingRequests() = %d, want 3", machine.PendingRequests())
	}

	if err := machine.ProcessPending(context.Background(), run); err != nil {
		t.Fatalf("ProcessPending() error = %v", err)
	}
	if len(*executed) != 3 {
		t.Fatalf("executed %d requests, want 3", len(*executed))
	}

	third, ok := (*executed)[2].(protocol.Array)
	if !ok || len(third.Elements) != 2 {
		t.Fatalf("third request = %#v, want two-element array", (*executed)[2])
	}
	payload, ok := third.Elements[1].(protocol.BulkString)
	if !ok || string(payload.Data) != "hey" {
		t.Fatalf("third request payload = %#v, want %q", third.Elements[1], "hey")
	}

	if got, want := flushAll(t, machine), "+OK\r\n+OK\r\n+OK\r\n"; string(got) != want {
		t.Fatalf("flushed output = %q, want %q", got, want)
	}
}

func TestConnMachinePartialWrites(t *testing.T) {
	machine := NewConnMachine(nil)
	run := func(context.Context, protocol.Value) ([]protocol.Value, error) {
		return []protocol.Value{protocol.BulkString{Data: []byte("a longer payload that needs several flushes")}}, nil
	}

	if err := machine.Feed([]byte("*1\r\n$4\r\nPING\r\n")); err != nil {
		t.Fatalf("Feed() error = %v", err)
	}
	if err := machine.ProcessPending(context.Background(), run); err != nil {
		t.Fatalf("ProcessPending() error = %v", err)
	}

	writer := &shortWriter{limit: 7}
	flushes := 0
	for machine.HasPendingOutput() {
		if err := machine.Flush(writer); err != nil {
			t.Fatalf("Flush() error = %v", err)
		}
		flushes++
	}

	want := "$43\r\na longer payload that needs several flushes\r\n"
	if got := writer.buf.String(); got != want {
		t.Fatalf("flushed output = %q, want %q", got, want)
	}
	if flushes < 2 {
		t.Fatalf("flushes = %d, want multiple partial writes", flushes)
	}
	if machine.State() != ConnStateActive {
		t.Fatalf("State() = %d, want ConnStateActive", machine.State())
	}
}

func TestConnMachineProtocolErrorClosesAfterOrderedReplies(t *testing.T) {
	run, executed := echoRunner(t)
	machine := NewConnMachine(nil)

	// One valid request followed by an unsupported frame prefix.
	if err := machine.Feed([]byte("*1\r\n$4\r\nPING\r\nX\r\n")); err != nil {
		t.Fatalf("Feed() error = %v", err)
	}
	if machine.State() != ConnStateClosing {
		t.Fatalf("State() = %d, want ConnStateClosing", machine.State())
	}
	if machine.Err() == nil {
		t.Fatal("Err() = nil, want recorded protocol error")
	}

	if err := machine.Feed([]byte("+more\r\n")); !errors.Is(err, ErrConnMachineClosed) {
		t.Fatalf("Feed() after protocol error = %v, want ErrConnMachineClosed", err)
	}

	if err := machine.ProcessPending(context.Background(), run); err != nil {
		t.Fatalf("ProcessPending() error = %v", err)
	}
	if len(*executed) != 1 {
		t.Fatalf("executed %d requests, want 1", len(*executed))
	}

	var out bytes.Buffer
	for machine.State() != ConnStateClosed {
		if err := machine.Flush(&out); err != nil {
			t.Fatalf("Flush() error = %v", err)
		}
	}

	reader := protocol.NewParser(bufio.NewReader(bytes.NewReader(out.Bytes())))
	first, err := reader.Parse()
	if err != nil {
		t.Fatalf("parse first reply: %v", err)
	}
	if want := (protocol.SimpleString{Value: "OK"}); first != want {
		t.Fatalf("first reply = %#v, want %#v", first, want)
	}
	second, err := reader.Parse()
	if err != nil {
		t.Fatalf("parse second reply: %v", err)
	}
	if _, ok := second.(protocol.ErrorValue); !ok {
		t.Fatalf("second reply = %#v, want protocol error reply", second)
	}
	if _, err := reader.Parse(); !errors.Is(err, io.EOF) {
		t.Fatalf("trailing parse error = %v, want io.EOF", err)
	}
}

// TestConnMachineFeedRejectsHostileFrames covers the frames a malicious peer can
// send to make the machine allocate or recurse without bound. Feed itself always
// succeeds. A hostile frame is reported by moving the machine to ConnStateClosing
// with a recorded error, never by panicking or by buffering without limit.
func TestConnMachineFeedRejectsHostileFrames(t *testing.T) {
	deeplyNested := func() []byte {
		var frame []byte
		for i := 0; i < 4096; i++ {
			frame = append(frame, "*1\r\n"...)
		}
		return append(frame, ":1\r\n"...)
	}

	tests := []struct {
		name          string
		maxReadBuffer int
		frame         []byte
		wantState     ConnMachineState
		reason        string
	}{
		{
			name:      "near-MaxInt bulk length buffers as incomplete",
			frame:     []byte("$9223372036854775807\r\n"),
			wantState: ConnStateActive,
			reason:    "a huge declared length must not index out of range in the decoder",
		},
		{
			name:      "deeply nested array",
			frame:     deeplyNested(),
			wantState: ConnStateClosing,
			reason:    "deep nesting must return a protocol error, not cause a stack overflow",
		},
		{
			name:          "buffered bytes exceed the read limit",
			maxReadBuffer: 16,
			frame:         []byte("$100\r\nabcdefghijklmnop"),
			wantState:     ConnStateClosing,
			reason:        "an incomplete frame must not be buffered without bound",
		},
		{
			name:          "declared length far past the read limit",
			maxReadBuffer: 1024,
			frame:         []byte("$1000000\r\n"),
			wantState:     ConnStateClosing,
			reason:        "the decoder's byte hint must reject before the payload is buffered",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			machine := NewConnMachine(nil)
			if tt.maxReadBuffer > 0 {
				machine.maxReadBuffer = tt.maxReadBuffer
			}

			if err := machine.Feed(tt.frame); err != nil {
				t.Fatalf("Feed() error = %v", err)
			}
			if machine.State() != tt.wantState {
				t.Fatalf("State() = %d, want %d (%s)", machine.State(), tt.wantState, tt.reason)
			}

			if tt.wantState == ConnStateClosing {
				if machine.Err() == nil {
					t.Fatalf("Err() = nil, want recorded protocol error (%s)", tt.reason)
				}
				return
			}
			if machine.PendingRequests() != 0 {
				t.Fatalf("PendingRequests() = %d, want 0 for incomplete frame", machine.PendingRequests())
			}
		})
	}
}

func TestConnMachineDefersDecodeUntilFrameCanComplete(t *testing.T) {
	// While a bulk payload is still arriving, the machine must not re-decode on
	// every append; it waits until the buffer reaches the declared frame size.
	run, executed := echoRunner(t)
	machine := NewConnMachine(nil)

	if err := machine.Feed([]byte("*1\r\n$5\r\nhe")); err != nil {
		t.Fatalf("Feed() error = %v", err)
	}
	if machine.resumeAt == 0 {
		t.Fatal("resumeAt = 0, want a pending-frame byte hint")
	}

	// Feeding fewer bytes than the hint requires keeps the frame pending.
	if err := machine.Feed([]byte("ll")); err != nil {
		t.Fatalf("Feed() error = %v", err)
	}
	if machine.PendingRequests() != 0 {
		t.Fatalf("PendingRequests() = %d, want 0 before the frame completes", machine.PendingRequests())
	}

	// The final byte plus terminator completes the frame.
	if err := machine.Feed([]byte("o\r\n")); err != nil {
		t.Fatalf("Feed() error = %v", err)
	}
	if machine.PendingRequests() != 1 {
		t.Fatalf("PendingRequests() = %d, want 1 once the frame completes", machine.PendingRequests())
	}
	if err := machine.ProcessPending(context.Background(), run); err != nil {
		t.Fatalf("ProcessPending() error = %v", err)
	}
	request, ok := (*executed)[0].(protocol.Array)
	if !ok || len(request.Elements) != 1 {
		t.Fatalf("request = %#v, want single-element array", (*executed)[0])
	}
	payload, ok := request.Elements[0].(protocol.BulkString)
	if !ok || string(payload.Data) != "hello" {
		t.Fatalf("payload = %#v, want %q", request.Elements[0], "hello")
	}
}

func TestConnMachineRunnerErrorClosesImmediately(t *testing.T) {
	machine := NewConnMachine(nil)
	fatal := fmt.Errorf("execution pipeline failed")
	run := func(context.Context, protocol.Value) ([]protocol.Value, error) {
		return nil, fatal
	}

	if err := machine.Feed([]byte("*1\r\n$4\r\nPING\r\n")); err != nil {
		t.Fatalf("Feed() error = %v", err)
	}
	if err := machine.ProcessPending(context.Background(), run); !errors.Is(err, fatal) {
		t.Fatalf("ProcessPending() error = %v, want %v", err, fatal)
	}

	if machine.State() != ConnStateClosed {
		t.Fatalf("State() = %d, want ConnStateClosed", machine.State())
	}
	if !errors.Is(machine.Err(), fatal) {
		t.Fatalf("Err() = %v, want %v", machine.Err(), fatal)
	}
	if machine.HasPendingOutput() {
		t.Fatal("HasPendingOutput() = true, want discarded output after fatal close")
	}
}

func TestConnMachineCloseCleansUpClientState(t *testing.T) {
	state := &ClientState{ID: 7}
	state.BindResponseWriter(bufio.NewWriter(io.Discard))
	if !state.BeginTransaction() {
		t.Fatal("BeginTransaction() = false, want true")
	}
	state.EnqueueCommand("SET", [][]byte{[]byte("foo"), []byte("bar")})

	machine := NewConnMachine(state)
	if err := machine.Feed([]byte("*1\r\n$4\r\nPING\r\n")); err != nil {
		t.Fatalf("Feed() error = %v", err)
	}

	machine.Close(nil)

	if machine.State() != ConnStateClosed {
		t.Fatalf("State() = %d, want ConnStateClosed", machine.State())
	}
	if state.InTransactionActive() {
		t.Fatal("InTransactionActive() = true, want transaction reset on close")
	}
	if state.HasActiveResponseWriter() {
		t.Fatal("HasActiveResponseWriter() = true, want response writer detached on close")
	}

	if err := machine.Feed([]byte("+ping\r\n")); !errors.Is(err, ErrConnMachineClosed) {
		t.Fatalf("Feed() after close = %v, want ErrConnMachineClosed", err)
	}
	run, _ := echoRunner(t)
	if err := machine.ProcessPending(context.Background(), run); !errors.Is(err, ErrConnMachineClosed) {
		t.Fatalf("ProcessPending() after close = %v, want ErrConnMachineClosed", err)
	}
	if err := machine.Flush(io.Discard); !errors.Is(err, ErrConnMachineClosed) {
		t.Fatalf("Flush() after close = %v, want ErrConnMachineClosed", err)
	}
}

func TestConnMachineCloseIsIdempotent(t *testing.T) {
	machine := NewConnMachine(nil)
	first := fmt.Errorf("peer reset")

	machine.Close(first)
	machine.Close(fmt.Errorf("later error"))

	if !errors.Is(machine.Err(), first) {
		t.Fatalf("Err() = %v, want first close error %v", machine.Err(), first)
	}
}

func TestConnMachineBufferEncodedOrdersPushFramesWithResponses(t *testing.T) {
	run, _ := echoRunner(t)
	machine := NewConnMachine(nil)

	if err := machine.Feed([]byte("*1\r\n$4\r\nPING\r\n")); err != nil {
		t.Fatalf("Feed() error = %v", err)
	}
	if err := machine.ProcessPending(context.Background(), run); err != nil {
		t.Fatalf("ProcessPending() error = %v", err)
	}

	// Drain part of the buffered response so the pending output has a non-zero
	// write offset, then append a push frame behind it.
	short := &shortWriter{limit: 2}
	if err := machine.Flush(short); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	if err := machine.BufferEncoded([]byte("+push\r\n")); err != nil {
		t.Fatalf("BufferEncoded() error = %v", err)
	}

	rest := flushAll(t, machine)
	if got, want := short.buf.String()+string(rest), "+OK\r\n+push\r\n"; got != want {
		t.Fatalf("flushed output = %q, want %q", got, want)
	}
}

func TestConnMachineBufferEncodedRejectsClosedMachine(t *testing.T) {
	machine := NewConnMachine(nil)
	machine.Close(nil)

	if err := machine.BufferEncoded([]byte("+push\r\n")); !errors.Is(err, ErrConnMachineClosed) {
		t.Fatalf("BufferEncoded() after close = %v, want ErrConnMachineClosed", err)
	}
}

func TestConnMachineProcessNextExecutesOneRequestAtATime(t *testing.T) {
	run, executed := echoRunner(t)
	machine := NewConnMachine(nil)

	if err := machine.Feed([]byte("*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n")); err != nil {
		t.Fatalf("Feed() error = %v", err)
	}

	more, err := machine.ProcessNext(context.Background(), run)
	if err != nil {
		t.Fatalf("ProcessNext() error = %v", err)
	}
	if !more {
		t.Fatal("ProcessNext() more = false after first request, want true")
	}
	if len(*executed) != 1 || machine.PendingRequests() != 1 {
		t.Fatalf("executed %d requests with %d pending, want 1 and 1", len(*executed), machine.PendingRequests())
	}

	more, err = machine.ProcessNext(context.Background(), run)
	if err != nil {
		t.Fatalf("ProcessNext() second call error = %v", err)
	}
	if more {
		t.Fatal("ProcessNext() more = true after final request, want false")
	}
	if len(*executed) != 2 {
		t.Fatalf("executed %d requests, want 2", len(*executed))
	}

	if got, want := flushAll(t, machine), "+OK\r\n+OK\r\n"; string(got) != want {
		t.Fatalf("flushed output = %q, want %q", got, want)
	}
}

func TestConnMachineWriteBufferLimitClosesOnOversizedResponses(t *testing.T) {
	run := func(_ context.Context, _ protocol.Value) ([]protocol.Value, error) {
		return []protocol.Value{protocol.BulkString{Data: bytes.Repeat([]byte("x"), 64)}}, nil
	}
	machine := NewConnMachine(nil)
	machine.maxWriteBuffer = 32

	if err := machine.Feed([]byte("*1\r\n$4\r\nPING\r\n")); err != nil {
		t.Fatalf("Feed() error = %v", err)
	}
	if err := machine.ProcessPending(context.Background(), run); err == nil {
		t.Fatal("ProcessPending() error = nil, want write-buffer limit error")
	}
	if machine.State() != ConnStateClosed {
		t.Fatalf("State() = %d, want ConnStateClosed", machine.State())
	}
}

func TestConnMachineWriteBufferLimitRejectsOversizedPushFrames(t *testing.T) {
	machine := NewConnMachine(nil)
	machine.maxWriteBuffer = 8

	if err := machine.BufferEncoded([]byte("+ok\r\n")); err != nil {
		t.Fatalf("BufferEncoded() within limit error = %v", err)
	}
	if err := machine.BufferEncoded([]byte("+more\r\n")); err == nil {
		t.Fatal("BufferEncoded() error = nil, want write-buffer limit error")
	}
	if machine.State() == ConnStateClosed {
		t.Fatal("State() = ConnStateClosed, want machine left open for the caller to close")
	}
}

// feedInChunks feeds data to the machine in chunk-sized pieces, the way socket
// reads deliver a large pipelined request, and stops early once the machine
// leaves the active state.
func feedInChunks(t *testing.T, machine *ConnMachine, data []byte, chunk int) {
	t.Helper()

	for len(data) > 0 && machine.State() == ConnStateActive {
		n := min(chunk, len(data))
		if err := machine.Feed(data[:n]); err != nil {
			t.Fatalf("Feed() error = %v", err)
		}
		data = data[n:]
	}
}

func TestConnMachineFeedIsLinearInArrayFrameSize(t *testing.T) {
	// One RPUSH-shaped request with hundreds of thousands of small arguments
	// arrives across many reads. Re-decoding the whole prefix on every read made
	// this cost seconds of event-loop CPU for a few megabytes of input; each byte
	// must be examined once. Absolute times depend on the machine and on the race
	// detector, so the test compares a frame with one four times its size: linear
	// work takes about four times as long, quadratic work about sixteen times.
	feed := func(args int) (time.Duration, int) {
		var frame bytes.Buffer
		fmt.Fprintf(&frame, "*%d\r\n$5\r\nRPUSH\r\n$1\r\nk\r\n", args+2)
		for i := 0; i < args; i++ {
			frame.WriteString("$1\r\nx\r\n")
		}

		best := time.Duration(1<<63 - 1)
		for run := 0; run < 3; run++ {
			machine := NewConnMachine(nil)
			start := time.Now()
			feedInChunks(t, machine, frame.Bytes(), 64*1024)
			if elapsed := time.Since(start); elapsed < best {
				best = elapsed
			}
			if machine.State() != ConnStateActive || machine.PendingRequests() != 1 {
				t.Fatalf("state = %d, pending = %d, want active with 1 request (err = %v)", machine.State(), machine.PendingRequests(), machine.Err())
			}
		}
		return best, frame.Len()
	}

	small, _ := feed(100_000)
	large, size := feed(400_000)

	if ratio := float64(large) / float64(small); ratio > 9 {
		t.Fatalf("feeding a %d byte array frame took %v, %.1fx the time of a frame a quarter of the size (%v): want roughly 4x (linear), not 16x (quadratic)", size, large, ratio, small)
	}
}

func TestConnMachineFeedRejectsUnboundedHeaderLines(t *testing.T) {
	tests := []struct {
		name  string
		frame []byte
	}{
		{name: "line that never terminates", frame: append([]byte("+"), bytes.Repeat([]byte("A"), 1<<20)...)},
		{name: "array length beyond the element limit", frame: []byte("*2000000\r\n")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			machine := NewConnMachine(nil)
			start := time.Now()
			feedInChunks(t, machine, tt.frame, 64*1024)

			if machine.State() != ConnStateClosing || machine.Err() == nil {
				t.Fatalf("State() = %d, Err() = %v, want a protocol error and the closing state", machine.State(), machine.Err())
			}
			if elapsed := time.Since(start); elapsed > time.Second {
				t.Fatalf("rejecting the frame took %v, want it to be immediate", elapsed)
			}
		})
	}
}

func machineFrame(args ...string) []byte {
	var frame bytes.Buffer
	fmt.Fprintf(&frame, "*%d\r\n", len(args))
	for _, arg := range args {
		fmt.Fprintf(&frame, "$%d\r\n%s\r\n", len(arg), arg)
	}
	return frame.Bytes()
}

func commandNames(requests []protocol.Value) []string {
	names := make([]string, 0, len(requests))
	for _, request := range requests {
		array, ok := request.(protocol.Array)
		if !ok || len(array.Elements) == 0 {
			names = append(names, "?")
			continue
		}
		bulk, _ := array.Elements[0].(protocol.BulkString)
		names = append(names, string(bulk.Data))
	}
	return names
}

func TestConnMachineDecodesOneRequestAtATimeWhileLimitsApply(t *testing.T) {
	// Requests behind an AUTH must be decoded under the limits that hold after the
	// AUTH has run, not the ones that held when they arrived. So while limits
	// apply the machine decodes one request, waits for it to run, and decodes the
	// next.
	limited := true
	newMachine := func() *ConnMachine {
		limited = true
		machine := NewConnMachine(nil)
		machine.SetRequestLimits(func() protocol.Limits {
			if limited {
				return protocol.UnauthenticatedLimits
			}
			return protocol.Limits{}
		})
		return machine
	}
	bigValue := string(bytes.Repeat([]byte("v"), 100*1024))

	t.Run("a large request behind a successful AUTH is decoded under the lifted limits", func(t *testing.T) {
		machine := newMachine()
		var executed []protocol.Value
		run := func(_ context.Context, request protocol.Value) ([]protocol.Value, error) {
			executed = append(executed, request)
			if names := commandNames(executed); names[len(names)-1] == "AUTH" {
				limited = false // the AUTH succeeded
			}
			return []protocol.Value{protocol.SimpleString{Value: "OK"}}, nil
		}

		input := append(append(machineFrame("AUTH", "secret"), machineFrame("SET", "key", bigValue)...), machineFrame("PING")...)
		if err := machine.Feed(input); err != nil {
			t.Fatalf("Feed() error = %v", err)
		}
		if got := machine.PendingRequests(); got != 1 {
			t.Fatalf("PendingRequests() after Feed = %d, want only the AUTH decoded", got)
		}

		if err := machine.ProcessPending(context.Background(), run); err != nil {
			t.Fatalf("ProcessPending() error = %v", err)
		}
		if got := fmt.Sprint(commandNames(executed)); got != "[AUTH SET PING]" {
			t.Fatalf("executed = %s, want [AUTH SET PING]", got)
		}
		if machine.State() != ConnStateActive {
			t.Fatalf("State() = %d, want active (err = %v)", machine.State(), machine.Err())
		}
	})

	t.Run("bytes that arrive while the request is waiting are kept for later", func(t *testing.T) {
		machine := newMachine()
		run, executed := echoRunner(t)

		if err := machine.Feed(machineFrame("PING")); err != nil {
			t.Fatalf("Feed() error = %v", err)
		}
		// The PING has not run yet, so nothing more is decoded, but the bytes stay.
		if err := machine.Feed(machineFrame("PING")); err != nil {
			t.Fatalf("Feed() error = %v", err)
		}
		if got := machine.PendingRequests(); got != 1 {
			t.Fatalf("PendingRequests() = %d, want 1 while limits apply", got)
		}
		if err := machine.ProcessPending(context.Background(), run); err != nil {
			t.Fatalf("ProcessPending() error = %v", err)
		}
		if len(*executed) != 2 {
			t.Fatalf("executed %d requests, want both PINGs", len(*executed))
		}
	})

	t.Run("a large request behind a failed AUTH is refused when it is reached", func(t *testing.T) {
		machine := newMachine()
		run, executed := echoRunner(t) // the AUTH does not lift the limits

		input := append(machineFrame("AUTH", "wrong"), machineFrame("SET", "key", bigValue)...)
		if err := machine.Feed(input); err != nil {
			t.Fatalf("Feed() error = %v", err)
		}
		if err := machine.ProcessPending(context.Background(), run); err != nil {
			t.Fatalf("ProcessPending() error = %v", err)
		}
		if len(*executed) != 1 {
			t.Fatalf("executed %d requests, want only the AUTH", len(*executed))
		}
		if machine.State() != ConnStateClosing {
			t.Fatalf("State() = %d, want closing after the oversized request", machine.State())
		}
		out := string(flushAll(t, machine))
		if want := "+OK\r\n-ERR protocol: parse array element 2: protocol: bulk string length 102400 exceeds 16384 byte limit for a client that has not authenticated\r\n"; out != want {
			t.Fatalf("output = %q, want %q", out, want)
		}
	})

	t.Run("without limits every complete request is decoded at once", func(t *testing.T) {
		machine := NewConnMachine(nil)
		machine.SetRequestLimits(func() protocol.Limits { return protocol.Limits{} })

		input := append(append(machineFrame("PING"), machineFrame("PING")...), machineFrame("PING")...)
		if err := machine.Feed(input); err != nil {
			t.Fatalf("Feed() error = %v", err)
		}
		if got := machine.PendingRequests(); got != 3 {
			t.Fatalf("PendingRequests() = %d, want 3", got)
		}
	})
}

func TestConnMachinePushLimitDoesNotApplyToReplies(t *testing.T) {
	// A reply can be as large as a stored value; only pushed frames are held to
	// the tighter limit.
	run := func(_ context.Context, _ protocol.Value) ([]protocol.Value, error) {
		return []protocol.Value{protocol.BulkString{Data: bytes.Repeat([]byte("x"), 1024)}}, nil
	}
	machine := NewConnMachine(nil)
	machine.SetPushLimit(64)

	if err := machine.Feed([]byte("*1\r\n$4\r\nPING\r\n")); err != nil {
		t.Fatalf("Feed() error = %v", err)
	}
	if err := machine.ProcessPending(context.Background(), run); err != nil {
		t.Fatalf("ProcessPending() error = %v, want a reply above the push limit accepted", err)
	}
	if got := machine.PendingOutputBytes(); got <= 64 {
		t.Fatalf("PendingOutputBytes() = %d, want the 1 KiB reply buffered", got)
	}

	if err := machine.BufferEncoded([]byte("+push\r\n")); err == nil {
		t.Fatal("BufferEncoded() error = nil, want a push refused while output above the push limit is pending")
	}

	// Once the reply has drained, a small push fits again.
	flushAll(t, machine)
	if err := machine.BufferEncoded([]byte("+push\r\n")); err != nil {
		t.Fatalf("BufferEncoded() after draining error = %v", err)
	}
}

// idleBufferCapBound is the capacity above which an idle connection must not
// keep a read or write buffer.
const idleBufferCapBound = 64 << 10

// largeSetFrame returns a SET of a value of the given size as one RESP frame.
func largeSetFrame(size int) []byte {
	frame := make([]byte, 0, size+64)
	frame = fmt.Appendf(frame, "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$%d\r\n", size)
	frame = append(frame, bytes.Repeat([]byte("x"), size)...)
	return append(frame, "\r\n"...)
}

func TestConnMachineReleasesItsReadBufferAfterALargeRequest(t *testing.T) {
	// A connection that once received a large value must not keep the buffer it
	// grew to receive it: with many idle connections that adds up to gigabytes.
	const readChunk = 64 << 10 // what the event loop reads from a socket at a time
	ping := machineFrame("PING")

	tests := []struct {
		name string
		// tail follows the large request in the read that completes it, and rest
		// arrives afterwards.
		tail, rest []byte
	}{
		{name: "the connection idles and a small request arrives later", rest: ping},
		{name: "half of the next request arrives with the end of the large one", tail: ping[:7], rest: ping[7:]},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			run, executed := echoRunner(t)
			machine := NewConnMachine(nil)

			feedInChunks(t, machine, append(largeSetFrame(32<<20), tt.tail...), readChunk)
			if err := machine.ProcessPending(context.Background(), run); err != nil {
				t.Fatalf("ProcessPending() error = %v", err)
			}
			if len(*executed) != 1 || machine.State() != ConnStateActive {
				t.Fatalf("executed %d requests, state = %d (err = %v), want the large request executed and the machine active", len(*executed), machine.State(), machine.Err())
			}
			if got := cap(machine.readBuf); got > idleBufferCapBound {
				t.Fatalf("read buffer capacity after the large request = %d bytes, want at most %d", got, idleBufferCapBound)
			}

			feedInChunks(t, machine, tt.rest, readChunk)
			if err := machine.ProcessPending(context.Background(), run); err != nil {
				t.Fatalf("ProcessPending() error = %v", err)
			}
			if got := fmt.Sprint(commandNames(*executed)); got != "[SET PING]" {
				t.Fatalf("executed = %s, want [SET PING]", got)
			}
			if got := cap(machine.readBuf); got > idleBufferCapBound {
				t.Fatalf("read buffer capacity after the later PING = %d bytes, want at most %d", got, idleBufferCapBound)
			}
		})
	}
}

func TestConnMachineKeepsALargeRemainderOfTheNextRequest(t *testing.T) {
	// Bytes of the next request that are still waiting after a large one is
	// decoded are the start of another large frame, so they stay where they are.
	// Dropping them would lose the request.
	run, executed := echoRunner(t)
	machine := NewConnMachine(nil)

	first, second := largeSetFrame(1<<20), largeSetFrame(1<<20)
	if err := machine.Feed(append(append([]byte(nil), first...), second[:300<<10]...)); err != nil {
		t.Fatalf("Feed() error = %v", err)
	}
	if got := machine.PendingRequests(); got != 1 {
		t.Fatalf("PendingRequests() = %d, want only the first request decoded", got)
	}
	if err := machine.Feed(second[300<<10:]); err != nil {
		t.Fatalf("Feed() error = %v", err)
	}
	if err := machine.ProcessPending(context.Background(), run); err != nil {
		t.Fatalf("ProcessPending() error = %v", err)
	}
	if got := fmt.Sprint(commandNames(*executed)); got != "[SET SET]" {
		t.Fatalf("executed = %s, want [SET SET]", got)
	}
	if got := cap(machine.readBuf); got > idleBufferCapBound {
		t.Fatalf("read buffer capacity once both requests were decoded = %d bytes, want at most %d", got, idleBufferCapBound)
	}
}

// bufferIdentity returns the address of the first byte of b's backing array. It
// changes whenever the machine replaces a buffer with a new one.
func bufferIdentity(b []byte) *byte {
	if cap(b) == 0 {
		return nil
	}
	return &b[:1][0]
}

func TestConnMachineKeepsItsReadBufferWhileOrdinaryRequestsKeepArriving(t *testing.T) {
	// A client that streams requests leaves part of one behind each 64 KiB read,
	// and the next read lands behind it. That buffer is working memory, not
	// something a large request left behind, so it must be reused: replacing it on
	// every read costs an allocation and a copy per read.
	const readChunk = 64 << 10
	tests := []struct {
		name      string
		valueSize int
	}{
		{name: "values of 100 bytes", valueSize: 100},
		{name: "values of 4 KiB", valueSize: 4 << 10},
		{name: "values that nearly fill a read", valueSize: 60 << 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			frame := machineFrame("SET", "key", string(bytes.Repeat([]byte("v"), tt.valueSize)))
			stream := bytes.Repeat(frame, (2<<20)/len(frame))
			run := func(context.Context, protocol.Value) ([]protocol.Value, error) {
				return []protocol.Value{protocol.SimpleString{Value: "OK"}}, nil
			}
			machine := NewConnMachine(nil)

			var identities []*byte
			var caps []int
			for off := 0; off < len(stream); off += readChunk {
				if err := machine.Feed(stream[off:min(off+readChunk, len(stream))]); err != nil {
					t.Fatalf("Feed() error = %v", err)
				}
				if err := machine.ProcessPending(context.Background(), run); err != nil {
					t.Fatalf("ProcessPending() error = %v", err)
				}
				if err := machine.Flush(io.Discard); err != nil {
					t.Fatalf("Flush() error = %v", err)
				}
				identities = append(identities, bufferIdentity(machine.readBuf))
				caps = append(caps, cap(machine.readBuf))
			}

			// The buffer grows a few times while the reads settle on a size, and is
			// replaced at most that often, not on every read.
			replaced := 0
			for i := 1; i < len(identities); i++ {
				if identities[i] != identities[i-1] {
					replaced++
				}
			}
			if limit := len(identities) / 4; replaced > limit {
				t.Fatalf("the read buffer was replaced %d times in %d reads (capacities %v), want at most %d", replaced, len(identities), caps, limit)
			}
		})
	}
}

// countingDiscard drops what it is written and counts it. It accepts at most
// limit bytes per Write, like a transport that applies backpressure.
type countingDiscard struct {
	limit int
	total int
}

func (w *countingDiscard) Write(p []byte) (int, error) {
	n := min(len(p), w.limit)
	w.total += n
	return n, nil
}

func TestConnMachineReleasesItsWriteBufferAfterALargeReply(t *testing.T) {
	// A connection that once sent a large value must not keep the buffer it grew
	// to hold the reply once the reply has drained. A reply the machine would
	// keep working memory for is kept, so the next one needs no new buffer.
	tests := []struct {
		name         string
		replySize    int
		writeLimit   int
		wantReleased bool
	}{
		{name: "a large reply the peer accepts in one write", replySize: 32 << 20, writeLimit: 1 << 30, wantReleased: true},
		{name: "a large reply the peer accepts a megabyte at a time", replySize: 32 << 20, writeLimit: 1 << 20, wantReleased: true},
		{name: "a reply just over what the machine keeps", replySize: retainedWriteBufferCap + 1, writeLimit: 1 << 30, wantReleased: true},
		{name: "a reply of a megabyte keeps its buffer for the next one", replySize: 1 << 20, writeLimit: 1 << 30, wantReleased: false},
		{name: "a small reply keeps its buffer for the next one", replySize: 1 << 10, writeLimit: 1 << 30, wantReleased: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reply := bytes.Repeat([]byte("x"), tt.replySize)
			run := func(context.Context, protocol.Value) ([]protocol.Value, error) {
				return []protocol.Value{protocol.BulkString{Data: reply}}, nil
			}
			machine := NewConnMachine(nil)
			peer := &countingDiscard{limit: tt.writeLimit}

			if err := machine.Feed(machineFrame("GET", "k")); err != nil {
				t.Fatalf("Feed() error = %v", err)
			}
			if err := machine.ProcessPending(context.Background(), run); err != nil {
				t.Fatalf("ProcessPending() error = %v", err)
			}
			if got := machine.PendingOutputBytes(); got < tt.replySize {
				t.Fatalf("PendingOutputBytes() = %d, want the %d byte reply buffered", got, tt.replySize)
			}

			for machine.HasPendingOutput() {
				if err := machine.Flush(peer); err != nil {
					t.Fatalf("Flush() error = %v", err)
				}
			}
			wantTotal := len(fmt.Sprintf("$%d\r\n", tt.replySize)) + tt.replySize + len("\r\n")
			if peer.total != wantTotal {
				t.Fatalf("peer received %d bytes, want %d", peer.total, wantTotal)
			}

			got := cap(machine.writeBuf)
			if tt.wantReleased && got > idleBufferCapBound {
				t.Fatalf("write buffer capacity after the reply drained = %d bytes, want at most %d", got, idleBufferCapBound)
			}
			if !tt.wantReleased && (got == 0 || got > retainedWriteBufferCap) {
				t.Fatalf("write buffer capacity after the reply drained = %d bytes, want it kept for reuse and at most %d", got, retainedWriteBufferCap)
			}

			// The released buffer is still good for the next output.
			if err := machine.BufferEncoded([]byte("+later\r\n")); err != nil {
				t.Fatalf("BufferEncoded() error = %v", err)
			}
			for machine.HasPendingOutput() {
				if err := machine.Flush(peer); err != nil {
					t.Fatalf("Flush() error = %v", err)
				}
			}
			if peer.total != wantTotal+len("+later\r\n") {
				t.Fatalf("peer received %d bytes after the later push, want %d", peer.total, wantTotal+len("+later\r\n"))
			}
		})
	}
}

func TestConnMachineKeepsItsWriteBufferWhilePipelinedRepliesKeepDraining(t *testing.T) {
	// A client that sends a pipeline of requests and reads all the replies before
	// the next pipeline drains the buffer completely each time. That buffer is
	// working memory, so replacing it after every pipeline would cost an
	// allocation and a copy per pipeline.
	tests := []struct {
		name      string
		count     int
		replySize int
	}{
		{name: "100 replies of 1 KiB", count: 100, replySize: 1 << 10},
		{name: "16 replies of 4 KiB", count: 16, replySize: 4 << 10},
		{name: "16 replies of 64 KiB", count: 16, replySize: 64 << 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reply := bytes.Repeat([]byte("x"), tt.replySize)
			run := func(context.Context, protocol.Value) ([]protocol.Value, error) {
				return []protocol.Value{protocol.BulkString{Data: reply}}, nil
			}
			pipeline := bytes.Repeat(machineFrame("GET", "k"), tt.count)
			machine := NewConnMachine(nil)

			var identities []*byte
			var caps []int
			for round := 0; round < 20; round++ {
				if err := machine.Feed(pipeline); err != nil {
					t.Fatalf("Feed() error = %v", err)
				}
				if err := machine.ProcessPending(context.Background(), run); err != nil {
					t.Fatalf("ProcessPending() error = %v", err)
				}
				// The identity is taken while the replies are buffered, since a buffer
				// that has drained is empty.
				identities = append(identities, bufferIdentity(machine.writeBuf))
				caps = append(caps, cap(machine.writeBuf))
				for machine.HasPendingOutput() {
					if err := machine.Flush(io.Discard); err != nil {
						t.Fatalf("Flush() error = %v", err)
					}
				}
			}

			replaced := 0
			for i := 1; i < len(identities); i++ {
				if identities[i] != identities[i-1] {
					replaced++
				}
			}
			if limit := len(identities) / 4; replaced > limit {
				t.Fatalf("the write buffer was replaced %d times in %d pipelines (capacities %v), want at most %d", replaced, len(identities), caps, limit)
			}
		})
	}
}
