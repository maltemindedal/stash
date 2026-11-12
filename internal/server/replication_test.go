package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maltemindedal/stash/internal/config"
	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/record"
	"github.com/maltemindedal/stash/internal/replication"
	"github.com/maltemindedal/stash/internal/storage"
)

// TestAReplicaWhoseSocketFailsIsDetachedWhenTheNextFrameIsPushed has two
// Replicas, one on a socket whose writes fail. Propagating only queues a frame
// on each Replica's Output. The Output whose socket fails closes itself when it
// writes, and the Master detaches its Replica at the next frame, which that
// Output refuses. The other Replica receives every frame.
func TestAReplicaWhoseSocketFailsIsDetachedWhenTheNextFrameIsPushed(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := newTestServer(t, config.Config{}, logger, storage.NewStore(), nil)

	healthy := &recordingConn{}
	srv.master.Add(replication.Candidate{ID: 1, ListeningPort: 6380, Out: NewOutput(healthy)})
	failing := NewOutput(&stubConn{writeErr: errors.New("write boom")})
	srv.master.Add(replication.Candidate{ID: 2, ListeningPort: 6381, Out: failing})

	srv.master.Propagate([]byte("+one\r\n"), true)
	waitForOutput(t, "the failed write to close the Replica's Output", func() bool { return failing.Err() != nil })
	if got := srv.master.Count(); got != 2 {
		t.Fatalf("master.Count() = %d before another frame is pushed, want 2", got)
	}

	srv.master.Propagate([]byte("+two\r\n"), true)
	if got := srv.master.Count(); got != 1 {
		t.Fatalf("master.Count() = %d, want the Replica whose socket failed detached", got)
	}
	want := "+one\r\n+two\r\n"
	waitForOutput(t, "the healthy Replica to be written both frames", func() bool { return len(healthy.Bytes()) == len(want) })
	if got := string(healthy.Bytes()); got != want {
		t.Fatalf("the healthy Replica received %q, want %q", got, want)
	}
}

// TestAReplicaDetachedForRefusingAFrameIsLoggedByWhyItsOutputClosed closes a
// Replica's Output for each cause it can close with and propagates a frame,
// which the Output refuses, so the Master detaches the Replica and tells the
// server. Only a Replica that fell behind (its stream past the Push limit, or
// nothing accepted for the stall timeout) is logged as dropped. One whose
// connection ended left, or is being torn down, and is logged as disconnected,
// as the connection's teardown would have logged it: a Replica that closed its
// socket just as a frame was written to it used to be logged as dropped. The
// teardown that follows finds the Replica detached and logs nothing more.
func TestAReplicaDetachedForRefusingAFrameIsLoggedByWhyItsOutputClosed(t *testing.T) {
	tests := []struct {
		name    string
		cause   error
		wantLog string
	}{
		{name: "the connection was closed", cause: net.ErrClosed, wantLog: "replica disconnected"},
		{name: "the replica reset the connection", cause: &net.OpError{Op: "write", Net: "tcp", Err: io.ErrClosedPipe}, wantLog: "replica disconnected"},
		{name: "the connection was torn down", cause: nil, wantLog: "replica disconnected"},
		{name: "its stream passed the push limit", cause: fmt.Errorf("%w: 268435457 bytes against a limit of 268435456", errPushLimitExceeded), wantLog: "dropping a replica"},
		{name: "it accepted nothing for the stall timeout", cause: fmt.Errorf("%w of 30s", errReplicaStalled), wantLog: "dropping a replica"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			var logs bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&logs, nil))
			srv := newTestServer(t, config.Config{}, logger, storage.NewStore(), nil)
			out := NewOutput(&recordingConn{})
			srv.master.Add(replication.Candidate{ID: 1, ListeningPort: 6380, Out: out})
			out.Close(tt.cause)

			logged := func(when string) {
				t.Helper()
				for _, line := range []string{"replica disconnected", "dropping a replica"} {
					if got, want := strings.Count(logs.String(), line), map[bool]int{true: 1}[line == tt.wantLog]; got != want {
						t.Fatalf("%s: logged %q %d times, want %d\nlogs:\n%s", when, line, got, want, logs.String())
					}
				}
			}
			srv.master.Propagate([]byte("+OK\r\n"), true)
			logged("after the frame the Output refused")
			if got := srv.master.Count(); got != 0 {
				t.Fatalf("%d replicas registered after the refusal, want 0", got)
			}

			srv.teardownClient(1, logger)
			logged("after the connection's teardown")
		})
	}
}

// TestAWritePropagatedBeforeTheFullResyncReplyIsQueuedFollowsIt attaches a
// Replica on a connection served by its own goroutine and propagates a write
// before the reply is queued, as a client writes while the snapshot is still
// being encoded. The attach cut told the connection's Output to hold what is
// pushed to it, so the write waits there and reaches the socket after the
// FULLRESYNC reply and the snapshot. The Event loop's connection is covered by
// TestEventLoopSendsAWritePropagatedDuringAFullResyncAfterItsReply.
func TestAWritePropagatedBeforeTheFullResyncReplyIsQueuedFollowsIt(t *testing.T) {
	srv := newTestServer(t, config.Config{}, slog.New(slog.NewTextHandler(io.Discard, nil)), storage.NewStore(), nil)
	wire := newOutputWire()
	out := newGoroutineOutput(newOutputs(), wire)
	defer out.Close(nil)

	attachment := attachWithoutCut(srv.master, replication.Candidate{ID: 1, ListeningPort: 6380, Out: out})
	srv.master.Propagate([]byte(fullResyncStreamFrame), true)
	if got := out.Backlog(); got != len(fullResyncStreamFrame) {
		t.Fatalf("Backlog() = %d before the reply, want the %d bytes of the write waiting", got, len(fullResyncStreamFrame))
	}

	reply := attachment.Reply()
	if err := out.Reply(reply...); err != nil {
		t.Fatalf("Reply() error = %v", err)
	}
	if err := out.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	want := encodedReply(t, reply) + fullResyncStreamFrame
	waitForOutput(t, "the reply and the write to be written", func() bool { return len(wire.sent()) == len(want) })
	if got := wire.sent(); got != want {
		t.Fatalf("the replica was sent %q, want the full resync reply and then the write: %q", got, want)
	}
}

// TestAReplicaPastItsPushLimitDuringItsFullResyncIsClosedAtOnce propagates more
// than a Replica may have waiting while its full resync reply is still being
// produced. The stream held behind that reply counts against the Replica Push
// limit, so the Output closes the connection on the write that passes it,
// without waiting for the reply, and the Master detaches the Replica and
// reports it dropped. The reply is then refused, so nothing is written. The
// connection used to stay open until the whole snapshot had been sent.
func TestAReplicaPastItsPushLimitDuringItsFullResyncIsClosedAtOnce(t *testing.T) {
	var logs bytes.Buffer
	srv := newTestServer(t, config.Config{}, slog.New(slog.NewTextHandler(&logs, nil)), storage.NewStore(), nil)
	wire := newOutputWire()
	out := newGoroutineOutput(newOutputs(withReplicaPushLimit(64)), wire)

	attachment := attachWithoutCut(srv.master, replication.Candidate{ID: 1, ListeningPort: 6380, Out: out})
	srv.master.Propagate(make([]byte, 40), true)
	if wire.isClosed() || srv.master.Count() != 1 {
		t.Fatalf("with 40 bytes waiting against a limit of 64: socket closed = %v, %d replicas registered; want it open and 1", wire.isClosed(), srv.master.Count())
	}

	srv.master.Propagate(make([]byte, 40), true)
	if !wire.isClosed() {
		t.Fatal("the socket is open with 80 bytes of stream waiting against a limit of 64, want it closed at once")
	}
	if err := out.Err(); !errors.Is(err, errPushLimitExceeded) {
		t.Fatalf("Err() = %v, want errPushLimitExceeded", err)
	}
	if got := srv.master.Count(); got != 0 {
		t.Fatalf("%d replicas registered after the limit was passed, want 0", got)
	}
	if dropped, disconnected := strings.Count(logs.String(), "dropping a replica"), strings.Count(logs.String(), "replica disconnected"); dropped != 1 || disconnected != 0 {
		t.Fatalf("logged \"dropping a replica\" %d times and \"replica disconnected\" %d times, want 1 and 0\nlogs:\n%s", dropped, disconnected, logs.String())
	}

	if err := out.Reply(attachment.Reply()...); !errors.Is(err, errPushLimitExceeded) {
		t.Fatalf("Reply() of the full resync error = %v, want it refused for the limit", err)
	}
	if got := wire.sent(); got != "" {
		t.Fatalf("sent %q to a replica dropped before its reply, want nothing", got)
	}
}

// fullResyncStreamFrame is a command frame propagated while a Replica's full
// resync reply is being produced.
const fullResyncStreamFrame = "*1\r\n$4\r\nPING\r\n"

// encodedReply is the bytes values are sent as.
func encodedReply(t *testing.T, values []protocol.Value) string {
	t.Helper()

	encoded, err := protocol.EncodeValues(values)
	if err != nil {
		t.Fatalf("EncodeValues() error = %v", err)
	}
	return string(encoded)
}

// attachWithoutCut attaches c as PSYNC does, but with no sequencer to cut
// against, for tests that propagate nothing while it runs.
func attachWithoutCut(master *replication.Master, c replication.Candidate) *replication.Attachment {
	return master.Attach(c, func(startRecording func()) (release func()) {
		startRecording()
		return func() {}
	})
}

type stubConn struct {
	writeErr error
	closeErr error
	closes   atomic.Int32
}

type recordingConn struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (c *stubConn) Read(_ []byte) (int, error) { return 0, io.EOF }
func (c *stubConn) Write(p []byte) (int, error) {
	if c.writeErr != nil {
		return 0, c.writeErr
	}
	return len(p), nil
}
func (c *stubConn) Close() error {
	c.closes.Add(1)
	return c.closeErr
}
func (c *stubConn) LocalAddr() net.Addr                { return stubAddr("local") }
func (c *stubConn) RemoteAddr() net.Addr               { return stubAddr("remote") }
func (c *stubConn) SetDeadline(_ time.Time) error      { return nil }
func (c *stubConn) SetReadDeadline(_ time.Time) error  { return nil }
func (c *stubConn) SetWriteDeadline(_ time.Time) error { return nil }

func (c *recordingConn) Read(_ []byte) (int, error) { return 0, io.EOF }
func (c *recordingConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.Write(p)
}
func (c *recordingConn) Close() error                       { return nil }
func (c *recordingConn) LocalAddr() net.Addr                { return stubAddr("local") }
func (c *recordingConn) RemoteAddr() net.Addr               { return stubAddr("remote") }
func (c *recordingConn) SetDeadline(_ time.Time) error      { return nil }
func (c *recordingConn) SetReadDeadline(_ time.Time) error  { return nil }
func (c *recordingConn) SetWriteDeadline(_ time.Time) error { return nil }

func (c *recordingConn) Bytes() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return bytes.Clone(c.buf.Bytes())
}

type stubAddr string

func (a stubAddr) Network() string { return "tcp" }
func (a stubAddr) String() string  { return string(a) }

// psyncLikeExecutor answers PSYNC as the command executor does, making the
// client a replica and answering with FULLRESYNC and a snapshot, and answers
// every other request with +OK.
type psyncLikeExecutor struct{ unorderedBackground }

func (psyncLikeExecutor) Handle(_ context.Context, call Call, request protocol.Value) (ExecuteResult, error) {
	if commandName(request) != "PSYNC" {
		return SingleResponse(protocol.SimpleString{Value: "OK"}), nil
	}
	call.Client.PromoteToReplica()
	return MultiResponse(protocol.SimpleString{Value: "FULLRESYNC test-replid 0"}, protocol.BulkString{Data: []byte("snapshot")}), nil
}

// TestRequestsOnAReplicasLinkGetNoReplies runs requests from one connection
// through the pipeline both networking modes share. The PSYNC that makes it a
// replica is answered, and nothing it sends after is: as in Redis, the link
// carries the replication stream alone. Its requests used to be answered into
// the stream.
func TestRequestsOnAReplicasLinkGetNoReplies(t *testing.T) {
	srv := newTestServer(t, config.Config{}, slog.New(slog.NewTextHandler(io.Discard, nil)), storage.NewStore(), psyncLikeExecutor{})
	state := &ClientState{ID: 1, Authenticated: true}
	call := Call{Client: state, Origin: record.OriginClient}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	steps := []struct {
		request     []string
		wantReplies int
	}{
		{request: []string{"PING"}, wantReplies: 1},
		{request: []string{"PSYNC", "?", "-1"}, wantReplies: 2},
		{request: []string{"REPLCONF", "ACK", "0", "FACK", "0"}, wantReplies: 0},
		{request: []string{"PING"}, wantReplies: 0},
	}
	for _, step := range steps {
		responses, err := srv.executeClientRequest(context.Background(), call, state.ID, &stubConn{}, logger, requestFrame(step.request...))
		if err != nil {
			t.Fatalf("%v error = %v", step.request, err)
		}
		if len(responses) != step.wantReplies {
			t.Fatalf("%v got %d replies %v, want %d", step.request, len(responses), responses, step.wantReplies)
		}
	}
}

// requestFrame is the RESP array a client sends for parts.
func requestFrame(parts ...string) protocol.Array {
	elements := make([]protocol.Value, len(parts))
	for i, part := range parts {
		elements[i] = protocol.BulkString{Data: []byte(part)}
	}
	return protocol.Array{Elements: elements}
}

// TestServerStatsConcurrentWithAckUpdates has INFO's data source read replica
// offsets while acknowledgements arrive. ServerStats used to read AckOffset from
// the shared peer without the Master's lock that Ack writes it under; the
// race detector reports that, so this test is meaningful under -race.
func TestServerStatsConcurrentWithAckUpdates(t *testing.T) {
	srv := newTestServer(t, config.Default(), slog.New(slog.NewTextHandler(io.Discard, nil)), storage.NewStore(), stubExecutor{})
	srv.master.Add(replication.Candidate{ID: 1, ListeningPort: 6380, Out: NewOutput(io.Discard)})

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for offset := int64(1); ; offset++ {
			select {
			case <-stop:
				return
			default:
				srv.master.Ack(1, offset)
			}
		}
	}()

	var last int64
	for i := 0; i < 2000; i++ {
		stats := srv.ServerStats()
		if len(stats.Replicas) != 1 {
			t.Fatalf("len(Replicas) = %d, want 1", len(stats.Replicas))
		}
		if got := stats.Replicas[0].AckedMasterOffset; got < last {
			t.Fatalf("AckedMasterOffset went backwards from %d to %d", last, got)
		} else {
			last = got
		}
	}
	close(stop)
	wg.Wait()
}

// TestAClientWithNoOutputIsAClosedConnectionToTheMaster hands the Master the
// Output of a Client state that was never bound to a connection, which is nil,
// as PSYNC would. Attaching it does nothing to it, it refuses the first frame
// with the cause of a closed connection, and so the Master detaches the
// Replica as one whose connection ended.
func TestAClientWithNoOutputIsAClosedConnectionToTheMaster(t *testing.T) {
	out := (&ClientState{ID: 1}).Output()

	out.MarkReplica()
	out.HoldPushes()
	out.EndStream()
	if out.Push([]byte("+message\r\n")) {
		t.Fatal("Push() = true for a client with no connection, want it refused")
	}
	if err := out.Err(); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("Err() = %v, want a closed connection's", err)
	}
}
