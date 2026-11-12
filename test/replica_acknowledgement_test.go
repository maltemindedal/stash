package test

import (
	"bufio"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maltemindedal/stash/internal/config"
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
		// writeDuringAttach has a client write from before PSYNC is sent until
		// the replica is reading its snapshot.
		writeDuringAttach bool
	}{
		// The connection's own goroutine writes the full resync reply, and what
		// is written meanwhile is queued behind it.
		{name: "nothing is written while the full resync reply is produced"},
		// A write is held behind the full resync reply, which then hands the
		// socket to the pump, and the connection's goroutine waits for the pump
		// to write the reply.
		{name: "writes are made while the full resync reply is produced", writeDuringAttach: true},
	}

	forEachNetworkingMode(t, defaultTestConfig(), func(t *testing.T, cfg config.Config) {
		for _, tt := range tests {
			tt := tt
			t.Run(tt.name, func(t *testing.T) {
				acknowledgeBehindAWaitingStream(t, cfg, tt.writeDuringAttach)
			})
		}
	})
}

func acknowledgeBehindAWaitingStream(t *testing.T, cfg config.Config, writeDuringAttach bool) {
	t.Helper()

	const megabyte = 1 << 20
	const deadline = 20 * time.Second

	addr, cancel, errCh := startTestServer(t, cfg)
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

	// The master copies the snapshot while writers wait, and encodes it after
	// they go on. A client that writes all the time therefore has a write
	// propagated between the two, which is held behind the reply.
	early := request("SET", "early", "e")
	earlyLen, err := protocol.EncodedLen(early)
	if err != nil {
		t.Fatalf("EncodedLen() error = %v", err)
	}
	var earlyWrites atomic.Int64
	stopEarlyWrites := func() {}
	if writeDuringAttach {
		earlyWriter, earlyParser := dialClient(t, addr)
		stop, done := make(chan struct{}), make(chan struct{})
		stopEarlyWrites = sync.OnceFunc(func() {
			close(stop)
			<-done
		})
		defer stopEarlyWrites()
		go func() {
			defer close(done)
			for {
				select {
				case <-stop:
					return
				default:
				}
				if err := protocol.WriteValue(earlyWriter, early); err != nil {
					t.Errorf("WriteValue(SET early) error = %v", err)
					return
				}
				if _, err := earlyParser.Parse(); err != nil {
					t.Errorf("Parse() SET early reply error = %v", err)
					return
				}
				earlyWrites.Add(1)
			}
		}()
		for earlyWrites.Load() == 0 && !t.Failed() {
			time.Sleep(time.Millisecond)
		}
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

	stopEarlyWrites()

	writer, writerParser := dialClient(t, addr)
	acknowledged, err := protocol.EncodedLen(request("SET", "acknowledged", "1"))
	if err != nil {
		t.Fatalf("EncodedLen() error = %v", err)
	}
	// The write to acknowledge ends no further into the stream than this: the
	// early writes that were made after the snapshot was copied are ahead of it.
	upTo := int(earlyWrites.Load())*earlyLen + acknowledged
	assertCommandResponse(t, writer, writerParser, protocol.SimpleString{Value: "OK"}, "SET", "acknowledged", "1")
	megabyteValue := strings.Repeat("f", megabyte)
	for i := 0; i < 32; i++ {
		assertCommandResponse(t, filler, fillerParser, protocol.SimpleString{Value: "OK"}, "SET", "fill", megabyteValue)
	}

	// The rest of the snapshot with its line ending, then the stream past the
	// write to acknowledge.
	if _, err := io.CopyN(io.Discard, reader, snapshot-snapshot/4+2+int64(upTo)); err != nil {
		t.Fatalf("reading the rest of the snapshot and the start of the stream: %v", err)
	}
	if err := protocol.WriteValue(replica, request("REPLCONF", "ACK", strconv.Itoa(upTo))); err != nil {
		t.Fatalf("WriteValue(REPLCONF ACK) error = %v", err)
	}

	if cfg.EventLoop {
		// WAIT does not block on the Event loop, so read the replica's offset
		// as the master counts it: it stays 0 until the acknowledgement is read.
		limit := time.Now().Add(deadline)
		for {
			fields := infoFields(t, writer, writerParser, "replication")
			if !strings.HasSuffix(fields["slave0"], ",offset=0") && fields["slave0"] != "" {
				return
			}
			if time.Now().After(limit) {
				t.Fatalf("INFO replication slave0 = %q %v after the replica acknowledged the first %d bytes of the stream: the master has not read the acknowledgement, sent while 32 MiB more of the stream waits for the replica", fields["slave0"], deadline, upTo)
			}
			time.Sleep(10 * time.Millisecond)
		}
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
