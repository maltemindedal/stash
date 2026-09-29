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
