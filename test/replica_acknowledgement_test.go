package test

import (
	"bufio"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/maltemindedal/stash/internal/protocol"
)

// TestTheMasterReadsAReplicasAcknowledgementsWhileItsStreamWaits attaches a
// replica to a master whose snapshot is 32 MiB and has clients write 32 MiB
// more while the replica is still reading the snapshot. The replica then reads
// its stream as far as the first of those writes, acknowledges it, and reads no
// further: a replica that applies its stream more slowly than the master writes
// it. More of the stream waits for the replica than its socket takes, so
// whoever writes it is waiting on the socket. That must not be the goroutine
// that reads the replica's acknowledgements: WAIT 1 for the write the replica
// acknowledged returns 1.
//
// The default networking mode only: a WAIT that has to wait is an error under
// --event-loop.
func TestTheMasterReadsAReplicasAcknowledgementsWhileItsStreamWaits(t *testing.T) {
	tests := []struct {
		name string
	}{
		// The connection's own goroutine writes the full resync reply, and what
		// is written meanwhile is queued behind it.
		{name: "nothing is written while the full resync reply is produced"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			acknowledgeBehindAWaitingStream(t)
		})
	}
}

func acknowledgeBehindAWaitingStream(t *testing.T) {
	t.Helper()

	const megabyte = 1 << 20
	const deadline = 20 * time.Second

	addr, cancel, errCh := startTestServer(t, defaultTestConfig())
	defer func() {
		cancel()
		waitForServerStop(t, errCh)
	}()

	filler, fillerParser := dialClient(t, addr)
	assertCommandResponse(t, filler, fillerParser, protocol.SimpleString{Value: "OK"}, "SET", "pad", strings.Repeat("s", 32*megabyte))

	replica, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) replica error = %v", addr, err)
	}
	// Closed before the master is stopped (deferred after it), so that shutdown
	// does not wait for a replica that is not reading.
	defer func() { _ = replica.Close() }()
	// Little of what the master writes may wait unread on the replica's side.
	if err := replica.(*net.TCPConn).SetReadBuffer(256 << 10); err != nil {
		t.Fatalf("SetReadBuffer() error = %v", err)
	}
	if err := replica.SetDeadline(time.Now().Add(deadline)); err != nil {
		t.Fatalf("SetDeadline() error = %v", err)
	}

	if err := protocol.WriteValue(replica, request("PSYNC", "?", "-1")); err != nil {
		t.Fatalf("WriteValue(PSYNC) error = %v", err)
	}
	reader := bufio.NewReaderSize(replica, 64<<10)
	line, err := reader.ReadString('\n')
	if err != nil || !strings.HasPrefix(line, "+FULLRESYNC ") {
		t.Fatalf("first line after PSYNC = %q, %v, want +FULLRESYNC", line, err)
	}
	header, err := reader.ReadString('\n')
	if err != nil || !strings.HasPrefix(header, "$") {
		t.Fatalf("snapshot header = %q, %v, want a bulk string's", header, err)
	}
	snapshot, err := strconv.ParseInt(strings.TrimSpace(header[1:]), 10, 64)
	if err != nil {
		t.Fatalf("snapshot length %q: %v", header, err)
	}
	// A quarter of the snapshot: the master is in the middle of sending it.
	if _, err := io.CopyN(io.Discard, reader, snapshot/4); err != nil {
		t.Fatalf("reading the start of the snapshot: %v", err)
	}

	writer, writerParser := dialClient(t, addr)
	acknowledged := request("SET", "acknowledged", "1")
	upTo, err := protocol.EncodedLen(acknowledged)
	if err != nil {
		t.Fatalf("EncodedLen() error = %v", err)
	}
	assertCommandResponse(t, writer, writerParser, protocol.SimpleString{Value: "OK"}, "SET", "acknowledged", "1")
	megabyteValue := strings.Repeat("f", megabyte)
	for i := 0; i < 32; i++ {
		assertCommandResponse(t, filler, fillerParser, protocol.SimpleString{Value: "OK"}, "SET", "fill", megabyteValue)
	}

	// The rest of the snapshot with its line ending, then the stream as far as
	// the write to acknowledge, which is its first frame.
	if _, err := io.CopyN(io.Discard, reader, snapshot-snapshot/4+2+int64(upTo)); err != nil {
		t.Fatalf("reading the rest of the snapshot and the first write of the stream: %v", err)
	}
	if err := protocol.WriteValue(replica, request("REPLCONF", "ACK", strconv.Itoa(upTo))); err != nil {
		t.Fatalf("WriteValue(REPLCONF ACK) error = %v", err)
	}

	if err := writer.SetReadDeadline(time.Now().Add(deadline)); err != nil {
		t.Fatalf("SetReadDeadline() error = %v", err)
	}
	if err := protocol.WriteValue(writer, request("WAIT", "1", "5000")); err != nil {
		t.Fatalf("WriteValue(WAIT) error = %v", err)
	}
	reply, err := writerParser.Parse()
	if err != nil {
		t.Fatalf("Parse() WAIT reply error = %v", err)
	}
	if got, ok := reply.(protocol.Integer); !ok || got.Value != 1 {
		t.Fatalf("WAIT 1 5000 = %#v for a write the replica acknowledged, want 1: the master has not read the acknowledgement of the first %d bytes of the stream, sent while 32 MiB more of it waits for the replica", reply, upTo)
	}
}
