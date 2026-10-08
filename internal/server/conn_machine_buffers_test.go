package server

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/maltemindedal/stash/internal/protocol"
)

// releasedBufferCapacity is the capacity above which an idle connection must
// not keep a buffer. It is also the size of one socket read, so a buffer at or
// below it is what a connection holds between requests anyway.
const releasedBufferCapacity = 64 << 10

// largeRequestSize is far past releasedBufferCapacity, so receiving or answering
// it grows a buffer to a size an idle connection must not keep.
const largeRequestSize = 32 << 20

// setFrame encodes SET key value as a RESP request.
func setFrame(key string, value []byte) []byte {
	header := fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n", len(key), key, len(value))
	frame := make([]byte, 0, len(header)+len(value)+2)
	frame = append(frame, header...)
	frame = append(frame, value...)
	return append(frame, "\r\n"...)
}

// decodeWhole decodes frame, which must hold exactly one RESP value.
func decodeWhole(t *testing.T, frame []byte) protocol.Value {
	t.Helper()

	value, n, err := protocol.Decode(frame)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if n != len(frame) {
		t.Fatalf("Decode() consumed %d of %d bytes", n, len(frame))
	}
	return value
}

// feedInReads feeds frame to the machine in reads of releasedBufferCapacity
// bytes, the size of the reads the event loop takes from a socket. The last
// trailer bytes of frame arrive in the final read together with follow, as when
// the next request is already on the wire behind the one that just finished.
func feedInReads(t *testing.T, machine *ConnMachine, frame []byte, trailer int, follow []byte) {
	t.Helper()

	body := frame[:len(frame)-trailer]
	for off := 0; off < len(body); off += releasedBufferCapacity {
		end := min(off+releasedBufferCapacity, len(body))
		if err := machine.Feed(body[off:end]); err != nil {
			t.Fatalf("Feed() error = %v", err)
		}
	}
	last := append(append([]byte(nil), frame[len(frame)-trailer:]...), follow...)
	if err := machine.Feed(last); err != nil {
		t.Fatalf("Feed() error = %v", err)
	}
}

func TestConnMachineReleasesItsReadBufferAfterALargeRequest(t *testing.T) {
	nextSet := setFrame("next", bytes.Repeat([]byte("y"), 256<<10))

	tests := []struct {
		name string
		// next is the request after the large one. The first split bytes of it
		// arrive in the same read as the end of the large request, the rest in a
		// later read.
		next  []byte
		split int
		// releasedAtOnce reports whether the read buffer must be small as soon as
		// the large request has been decoded. A large remainder is the start of
		// the next large request, so it stays until that request is decoded too.
		releasedAtOnce bool
		wantCommands   []string
	}{
		{
			name:           "nothing follows the large request",
			next:           []byte(pingFrame),
			releasedAtOnce: true,
			wantCommands:   []string{"SET", "PING"},
		},
		{
			name:           "half of a small request follows in the last read",
			next:           []byte(pingFrame),
			split:          len(pingFrame) / 2,
			releasedAtOnce: true,
			wantCommands:   []string{"SET", "PING"},
		},
		{
			name:         "a large part of the next request follows in the last read",
			next:         nextSet,
			split:        128 << 10,
			wantCommands: []string{"SET", "SET"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			run, executed := echoRunner(t)
			machine := NewConnMachine(nil)

			feedInReads(t, machine, setFrame("k", bytes.Repeat([]byte("x"), largeRequestSize)), 16, tt.next[:tt.split])

			if machine.State() != ConnStateActive {
				t.Fatalf("State() = %d, want ConnStateActive", machine.State())
			}
			if machine.PendingRequests() != 1 {
				t.Fatalf("PendingRequests() = %d, want the large request", machine.PendingRequests())
			}
			if got := len(machine.readBuf); got != tt.split {
				t.Fatalf("len(readBuf) = %d, want the %d bytes of the next request that arrived with it", got, tt.split)
			}
			if tt.releasedAtOnce && cap(machine.readBuf) > releasedBufferCapacity {
				t.Fatalf("cap(readBuf) = %d after the large request was decoded, want at most %d", cap(machine.readBuf), releasedBufferCapacity)
			}
			if err := machine.ProcessPending(context.Background(), run); err != nil {
				t.Fatalf("ProcessPending() error = %v", err)
			}

			if err := machine.Feed(tt.next[tt.split:]); err != nil {
				t.Fatalf("Feed() error = %v", err)
			}
			if err := machine.ProcessPending(context.Background(), run); err != nil {
				t.Fatalf("ProcessPending() error = %v", err)
			}

			if got := commandNames(*executed); !reflect.DeepEqual(got, tt.wantCommands) {
				t.Fatalf("executed commands = %v, want %v", got, tt.wantCommands)
			}
			if want := decodeWhole(t, tt.next); !reflect.DeepEqual((*executed)[1], want) {
				t.Fatalf("second request = %#v, want %#v", (*executed)[1], want)
			}
			if got := len(machine.readBuf); got != 0 {
				t.Fatalf("len(readBuf) = %d after every request ran, want 0", got)
			}
			if cap(machine.readBuf) > releasedBufferCapacity {
				t.Fatalf("cap(readBuf) = %d after every request ran, want at most %d", cap(machine.readBuf), releasedBufferCapacity)
			}
		})
	}
}

func TestConnMachineReleasesItsWriteBufferAfterALargeReply(t *testing.T) {
	value := bytes.Repeat([]byte("x"), largeRequestSize)
	wantReply := append(append([]byte(fmt.Sprintf("$%d\r\n", len(value))), value...), "\r\n"...)

	tests := []struct {
		name string
		// writeLimit is how many bytes the socket takes per write.
		writeLimit int
		wantWrites int
	}{
		{name: "the socket takes the whole reply at once", writeLimit: len(wantReply), wantWrites: 1},
		{name: "the socket takes the reply a mebibyte at a time", writeLimit: 1 << 20, wantWrites: len(wantReply)>>20 + 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			replies := 0
			run := func(context.Context, protocol.Value) ([]protocol.Value, error) {
				replies++
				if replies == 1 {
					return []protocol.Value{protocol.BulkString{Data: value}}, nil
				}
				return []protocol.Value{protocol.SimpleString{Value: "OK"}}, nil
			}
			machine := NewConnMachine(nil)

			if err := machine.Feed(machineFrame("GET", "k")); err != nil {
				t.Fatalf("Feed() error = %v", err)
			}
			if err := machine.ProcessPending(context.Background(), run); err != nil {
				t.Fatalf("ProcessPending() error = %v", err)
			}
			if got := machine.PendingOutputBytes(); got != len(wantReply) {
				t.Fatalf("PendingOutputBytes() = %d, want the %d byte reply", got, len(wantReply))
			}

			socket := &shortWriter{limit: tt.writeLimit}
			writes := 0
			for machine.HasPendingOutput() {
				if err := machine.Flush(socket); err != nil {
					t.Fatalf("Flush() error = %v", err)
				}
				writes++
			}

			if writes != tt.wantWrites {
				t.Fatalf("Flush() wrote the reply in %d writes, want %d", writes, tt.wantWrites)
			}
			if !bytes.Equal(socket.buf.Bytes(), wantReply) {
				t.Fatalf("flushed %d bytes that differ from the %d byte reply", socket.buf.Len(), len(wantReply))
			}
			if cap(machine.writeBuf) > releasedBufferCapacity {
				t.Fatalf("cap(writeBuf) = %d after the reply drained, want at most %d", cap(machine.writeBuf), releasedBufferCapacity)
			}

			if err := machine.Feed([]byte(pingFrame)); err != nil {
				t.Fatalf("Feed() error = %v", err)
			}
			if err := machine.ProcessPending(context.Background(), run); err != nil {
				t.Fatalf("ProcessPending() error = %v", err)
			}
			if got, want := flushAll(t, machine), "+OK\r\n"; string(got) != want {
				t.Fatalf("flushed output = %q, want %q", got, want)
			}
			if cap(machine.writeBuf) > releasedBufferCapacity {
				t.Fatalf("cap(writeBuf) = %d after a small reply drained, want at most %d", cap(machine.writeBuf), releasedBufferCapacity)
			}
		})
	}
}
