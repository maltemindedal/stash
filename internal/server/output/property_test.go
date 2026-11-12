package output

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"runtime"
	"sync"
	"testing"

	"github.com/maltemindedal/stash/internal/protocol"
)

// fitfulSocket is the Event loop's socket, in memory: each write takes a random
// part of what it is given and reports that it would block, or reports
// nothing, as a socket whose buffer filled does.
type fitfulSocket struct {
	rng      *rand.Rand
	received bytes.Buffer
	// open makes it take everything, to write an Output out.
	open bool
}

func (s *fitfulSocket) Write(p []byte) (int, error) {
	if s.open {
		return s.received.Write(p)
	}
	n := s.rng.Intn(len(p) + 1)
	s.received.Write(p[:n])
	switch {
	case n == len(p):
		return n, nil
	case s.rng.Intn(2) == 0:
		return n, io.ErrShortWrite
	default:
		return n, ErrWouldBlock
	}
}

// yieldingWire is the socket of a connection served by its own goroutine, in
// memory. It gives up the processor in some writes, so that the connection's
// goroutine and the pump meet in every order.
type yieldingWire struct {
	mu       sync.Mutex
	received bytes.Buffer
	writes   int
}

func (w *yieldingWire) Write(p []byte) (int, error) {
	w.mu.Lock()
	w.writes++
	yield := w.writes%3 == 0
	w.mu.Unlock()
	if yield {
		runtime.Gosched()
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.received.Write(p)
}

func (w *yieldingWire) sent() []byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]byte(nil), w.received.Bytes()...)
}

// TestAnOutputsBytesAreTheOnesItsLedgerCountsAndReachTheWireInOrder drives an
// Output with random Replies, Pushed messages, holds and writes, in each
// networking mode. After every operation the bytes it holds must be as many as
// its Ledger counts: a decision made from a count that the bytes do not match
// is a wrong decision. At the end the wire must carry every Reply and Pushed
// message in the order they were produced, a held message behind the Reply
// that released it.
func TestAnOutputsBytesAreTheOnesItsLedgerCountsAndReachTheWireInOrder(t *testing.T) {
	const operations = 300

	type driver struct {
		out *Output
		// write gives the socket a chance to take output, as the mode does
		// between requests, and writeOut writes everything that waits.
		write, writeOut func()
		sent            func() []byte
	}
	modes := []struct {
		name string
		make func(rng *rand.Rand) driver
	}{
		{name: "a goroutine for each connection", make: func(*rand.Rand) driver {
			wire := &yieldingWire{}
			out := NewBlocking(NewShared(), wire, discardLogger)
			flush := func() {
				if err := out.Flush(); err != nil {
					panic(fmt.Sprintf("Flush() error = %v", err))
				}
			}
			return driver{out: out, write: flush, sent: wire.sent, writeOut: func() {
				flush()
				// What was pushed is the pump's to write.
				for out.Backlog() > 0 {
					runtime.Gosched()
				}
			}}
		}},
		{name: "the event loop", make: func(rng *rand.Rand) driver {
			socket := &fitfulSocket{rng: rng}
			out := NewPolled(NewShared(), nil, discardLogger)
			return driver{out: out, sent: socket.received.Bytes,
				write: func() { out.Service(socket) },
				writeOut: func() {
					socket.open = true
					out.Service(socket)
				}}
		}},
	}

	for _, mode := range modes {
		mode := mode
		t.Run(mode.name, func(t *testing.T) {
			for seed := int64(1); seed <= 8; seed++ {
				rng := rand.New(rand.NewSource(seed))
				d := mode.make(rng)
				out := d.out

				// stream is what the wire must carry, and held what waits for the
				// next Reply.
				var stream, held []byte
				holding := false
				large := 0
				payload := func(tag byte) []byte {
					size := rng.Intn(200)
					switch roll := rng.Intn(100); {
					case roll < 2 && large < 3:
						// As large as the buffers an Output takes whole.
						large++
						size = outputRetainedBuffer + rng.Intn(1024)
					case roll < 10:
						size = rng.Intn(3 * outputWriteThrough)
					}
					return bytes.Repeat([]byte{tag}, size)
				}
				check := func(operation int, what string) {
					t.Helper()
					out.mu.Lock()
					holds, counts := out.bytes.waiting(), out.ledger.Waiting()
					out.mu.Unlock()
					if int64(holds) != counts {
						t.Fatalf("seed %d, after operation %d (%s): the Output holds %d bytes and its Ledger counts %d", seed, operation, what, holds, counts)
					}
				}

				for i := 0; i < operations; i++ {
					tag := byte('a' + i%26)
					var what string
					switch roll := rng.Intn(10); {
					case roll < 3:
						what = "a reply"
						values := make([]protocol.Value, rng.Intn(3))
						for j := range values {
							values[j] = protocol.BulkString{Data: payload(tag)}
						}
						encoded, err := protocol.EncodeValues(values)
						if err != nil {
							t.Fatalf("EncodeValues() error = %v", err)
						}
						if err := out.Reply(values...); err != nil {
							t.Fatalf("seed %d: Reply() error = %v", seed, err)
						}
						stream = append(append(stream, encoded...), held...)
						held, holding = held[:0], false
					case roll < 7:
						what = "a push"
						frame := append(payload(tag), '\n')
						if !out.Push(frame) {
							t.Fatalf("seed %d: Push() = false, error %v", seed, out.Err())
						}
						if holding {
							held = append(held, frame...)
						} else {
							stream = append(stream, frame...)
						}
					case roll < 8:
						what = "a hold"
						out.HoldPushes()
						holding = true
					default:
						what = "a write"
						d.write()
					}
					check(i, what)
				}

				if err := out.Reply(); err != nil {
					t.Fatalf("seed %d: Reply() error = %v", seed, err)
				}
				stream = append(stream, held...)
				d.writeOut()
				check(operations, "everything was written")
				if got := d.sent(); !bytes.Equal(got, stream) {
					at := firstDifference(string(got), string(stream))
					t.Fatalf("seed %d: the wire carries %d bytes and %d were produced, and they differ from byte %d: the stream is out of order or incomplete", seed, len(got), len(stream), at)
				}

				out.Close(nil)
				check(operations, "the close")
			}
		})
	}
}
