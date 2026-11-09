package server

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maltemindedal/stash/internal/config"
	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/record"
	"github.com/maltemindedal/stash/internal/storage"
)

func TestServerPropagateToReplicasRemovesFailingReplica(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := newTestServer(t, config.Config{}, logger, storage.NewStore(), nil)

	serverConn := &recordingConn{}
	srv.master.Add(Candidate{ID: 1, ListeningPort: 6380, Out: newReplicaPeerStateForTest(1, serverConn)}, serverConn)
	failingConn := &stubConn{writeErr: errors.New("write boom")}
	srv.master.Add(Candidate{ID: 2, ListeningPort: 6381, Out: newReplicaPeerStateForTest(2, failingConn)}, failingConn)

	// Propagating only queues the command for each replica; the replica whose
	// socket fails is found out, and dropped, by its feed.
	srv.master.Propagate([]byte("+OK\r\n"), true)
	deadline := time.Now().Add(2 * time.Second)
	for srv.master.Count() != 1 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if srv.master.Count() != 1 {
		t.Fatalf("master.Count() = %d, want the failing replica dropped", srv.master.Count())
	}
	for len(serverConn.Bytes()) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	parser := protocol.NewParser(bytes.NewReader(serverConn.Bytes()))
	value, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if got, ok := value.(protocol.SimpleString); !ok || got.Value != "OK" {
		t.Fatalf("parsed value = %#v, want simple string OK", value)
	}
}

// TestADroppedReplicaIsLoggedByWhyItsFeedStopped drops a replica for each cause
// its feed can stop with. Only a replica that fell behind (its queue past the
// limit, or a write to it stalled) is logged as dropped. One whose connection
// ended left, or is being torn down, and is logged as disconnected, as the
// connection's teardown would have logged it: a replica that closed its socket
// just as its feed wrote used to be logged as dropped.
func TestADroppedReplicaIsLoggedByWhyItsFeedStopped(t *testing.T) {
	tests := []struct {
		name    string
		cause   error
		wantLog string
	}{
		{name: "the connection was closed", cause: net.ErrClosed, wantLog: "replica disconnected"},
		{name: "the replica reset the connection", cause: &net.OpError{Op: "write", Net: "tcp", Err: io.ErrClosedPipe}, wantLog: "replica disconnected"},
		{name: "the client state was disconnected", cause: errors.New("client response writer unavailable"), wantLog: "replica disconnected"},
		{name: "its queue passed the limit", cause: ErrReplicaBacklog, wantLog: "dropping a replica"},
		{name: "the event loop's push limit", cause: fmt.Errorf("%w: more than its 268435456 byte limit waiting", errPushLimitExceeded), wantLog: "dropping a replica"},
		{name: "a write stalled", cause: &net.OpError{Op: "write", Net: "tcp", Err: os.ErrDeadlineExceeded}, wantLog: "dropping a replica"},
		{name: "the writer stayed busy", cause: fmt.Errorf("%w for 30s", errResponseWriterBusy), wantLog: "dropping a replica"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			var logs bytes.Buffer
			srv := newTestServer(t, config.Config{}, slog.New(slog.NewTextHandler(&logs, nil)), storage.NewStore(), nil)
			conn := &stubConn{}
			srv.master.Add(Candidate{ID: 1, ListeningPort: 6380, Out: newReplicaPeerStateForTest(1, &recordingConn{})}, conn)

			srv.dropReplica(1, tt.cause)

			for _, line := range []string{"replica disconnected", "dropping a replica"} {
				if got, want := strings.Count(logs.String(), line), map[bool]int{true: 1}[line == tt.wantLog]; got != want {
					t.Fatalf("logged %q %d times, want %d\nlogs:\n%s", line, got, want, logs.String())
				}
			}
			if srv.master.Count() != 0 || conn.closes.Load() != 1 {
				t.Fatalf("after the drop: %d replicas registered, connection closed %d times; want 0 and once", srv.master.Count(), conn.closes.Load())
			}
			srv.master.StopFeeds(time.Second)
		})
	}
}

// TestAConnectionIsMarkedAReplicaBeforeItsFeedStarts starts the feed of a
// replica whose connection, like the event loop's, holds waiting output to a
// limit that depends on being a replica. The connection must be marked before
// the feed starts: the feed may push what it queued during the full resync at
// once, and a connection not yet marked would hold it to a subscriber's 32 MiB
// instead of a replica's 256 MiB. The feed holds a frame from the attach, so a
// feed started first would write it while the connection waits to be marked.
func TestAConnectionIsMarkedAReplicaBeforeItsFeedStarts(t *testing.T) {
	srv := newTestServer(t, config.Config{}, slog.New(slog.NewTextHandler(io.Discard, nil)), storage.NewStore(), nil)
	out := &markOrderOutput{written: make(chan struct{})}
	attachWithoutCut(srv.master, Candidate{ID: 1, ListeningPort: 6380, Out: out})
	defer srv.master.StopFeeds(time.Second)
	srv.master.Propagate([]byte("*1\r\n$4\r\nPING\r\n"), true)

	conn := &markedConn{}
	conn.onMark = func() {
		select {
		case <-out.written:
			conn.feedWroteBeforeMark = true
		case <-time.After(50 * time.Millisecond):
		}
	}
	srv.registerReplicaPeer(1, conn)

	if conn.marks != 1 {
		t.Fatalf("the connection was marked a replica %d times, want once", conn.marks)
	}
	if conn.feedWroteBeforeMark {
		t.Fatal("the connection was marked a replica after its feed started writing")
	}
	select {
	case <-out.written:
	case <-time.After(5 * time.Second):
		t.Fatal("the feed did not start: it wrote nothing within 5s of the connection being registered")
	}
}

// TestAReplicaDroppedWhileItsFullResyncIsSentHasItsConnectionClosed detaches a
// replica between its attach cut and the start of its feed, as a disconnect or
// a backlog past the limit during the transfer does. The Master has no
// connection to close for it yet, so the server closes it once the reply is
// sent, when the feed will not start.
func TestAReplicaDroppedWhileItsFullResyncIsSentHasItsConnectionClosed(t *testing.T) {
	srv := newTestServer(t, config.Config{}, slog.New(slog.NewTextHandler(io.Discard, nil)), storage.NewStore(), nil)
	attachWithoutCut(srv.master, Candidate{ID: 1, ListeningPort: 6380, Out: newReplicaPeerStateForTest(1, &recordingConn{})})
	srv.master.Detach(1)

	conn := &stubConn{}
	srv.registerReplicaPeer(1, conn)
	if got := conn.closes.Load(); got != 1 {
		t.Fatalf("the connection was closed %d times, want once: a dropped replica's reply is followed by a close", got)
	}
}

// attachWithoutCut attaches c as PSYNC does, but with no sequencer to cut
// against, for tests that propagate nothing while it runs.
func attachWithoutCut(master *Master, c Candidate) *Attachment {
	return master.Attach(c, func(startRecording func()) (release func()) {
		startRecording()
		return func() {}
	})
}

// markOrderOutput is a replica's output that reports its first write.
type markOrderOutput struct {
	once    sync.Once
	written chan struct{}
}

func (o *markOrderOutput) WriteEncodedWithDeadline([]byte, time.Duration) error {
	o.once.Do(func() { close(o.written) })
	return nil
}

// markedConn is a connection that, like the event loop's, is told when it
// becomes a replica.
type markedConn struct {
	stubConn
	marks               int
	onMark              func()
	feedWroteBeforeMark bool
}

func (c *markedConn) markReplica() {
	c.marks++
	c.onMark()
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
type psyncLikeExecutor struct{}

func (psyncLikeExecutor) Handle(_ context.Context, call Call, request protocol.Value) (ExecuteResult, error) {
	if commandName(request) != "PSYNC" {
		return SingleResponse(protocol.SimpleString{Value: "OK"}), nil
	}
	call.Client.PromoteToReplica()
	result := MultiResponse(protocol.SimpleString{Value: "FULLRESYNC test-replid 0"}, protocol.BulkString{Data: []byte("snapshot")})
	result.RegisterReplica = true
	return result, nil
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
		responses, _, err := srv.executeClientRequest(context.Background(), call, state.ID, &stubConn{}, logger, requestFrame(step.request...))
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

func newReplicaPeerStateForTest(id uint64, conn net.Conn) *ClientState {
	state := &ClientState{ID: id, Authenticated: true}
	state.PromoteToReplica()
	state.BindResponseWriter(bufio.NewWriter(conn))
	return state
}

// TestServerStatsConcurrentWithAckUpdates has INFO's data source read replica
// offsets while acknowledgements arrive. ServerStats used to read AckOffset from
// the shared peer without the Master's lock that Ack writes it under; the
// race detector reports that, so this test is meaningful under -race.
func TestServerStatsConcurrentWithAckUpdates(t *testing.T) {
	srv := newTestServer(t, config.Default(), slog.New(slog.NewTextHandler(io.Discard, nil)), storage.NewStore(), stubExecutor{})
	srv.master.Add(Candidate{ID: 1, ListeningPort: 6380, Out: nil}, &stubConn{})

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
