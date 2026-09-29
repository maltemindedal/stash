package server

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/maltemindedal/stash/internal/config"
	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/storage"
)

func TestReplicaRegistryCountReplicasAtOrAboveWithNotify(t *testing.T) {
	registry := NewReplicaRegistry()
	serverConn, replicaConn := net.Pipe()
	defer func() { _ = serverConn.Close() }()
	defer func() { _ = replicaConn.Close() }()

	state := newReplicaPeerStateForTest(1, serverConn)
	registry.Add(1, serverConn, 6380, state)

	count, changed := registry.CountReplicasAtOrAboveWithNotify(10)
	if count != 0 {
		t.Fatalf("initial count = %d, want 0", count)
	}

	if ok := registry.UpdateAck(1, 10); !ok {
		t.Fatal("UpdateAck() = false, want true")
	}

	select {
	case <-changed:
	case <-time.After(time.Second):
		t.Fatal("CountReplicasAtOrAboveWithNotify() channel was not notified")
	}

	count, _ = registry.CountReplicasAtOrAboveWithNotify(10)
	if count != 1 {
		t.Fatalf("updated count = %d, want 1", count)
	}
}

func TestReplicaRegistryRemoveAndCloseReturnsCloseError(t *testing.T) {
	registry := NewReplicaRegistry()
	conn := &stubConn{closeErr: errors.New("close boom")}
	registry.Add(1, conn, 6380, newReplicaPeerStateForTest(1, conn))

	err := registry.RemoveAndClose(1)
	if err == nil || err.Error() != "close boom" {
		t.Fatalf("RemoveAndClose() error = %v, want close boom", err)
	}
}

func TestServerPropagateToReplicasRemovesFailingReplica(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := New(config.Config{}, logger, storage.NewStore(), nil)

	serverConn := &recordingConn{}
	srv.replicaPeers.Add(1, serverConn, 6380, newReplicaPeerStateForTest(1, serverConn))
	failingConn := &stubConn{writeErr: errors.New("write boom")}
	srv.replicaPeers.Add(2, failingConn, 6381, newReplicaPeerStateForTest(2, failingConn))

	// Propagating only queues the command for each replica; the replica whose
	// socket fails is found out, and dropped, by its feed.
	report := srv.propagateToReplicas([]protocol.Value{protocol.SimpleString{Value: "OK"}})
	if report.attempted != 2 {
		t.Fatalf("report.attempted = %d, want 2", report.attempted)
	}
	if report.succeeded != 2 || report.failed != 0 {
		t.Fatalf("report = %+v, want both queued", report)
	}
	deadline := time.Now().Add(2 * time.Second)
	for srv.replicaPeers.Count() != 1 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if srv.replicaPeers.Count() != 1 {
		t.Fatalf("replicaPeers.Count() = %d, want the failing replica dropped", srv.replicaPeers.Count())
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

type stubConn struct {
	writeErr error
	closeErr error
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
func (c *stubConn) Close() error                       { return c.closeErr }
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

func newReplicaPeerStateForTest(id uint64, conn net.Conn) *ClientState {
	state := &ClientState{ID: id, Authenticated: true}
	state.PromoteToReplica()
	state.BindResponseWriter(bufio.NewWriter(conn))
	return state
}

// TestServerStatsConcurrentWithAckUpdates has INFO's data source read replica
// offsets while acknowledgements arrive. ServerStats used to read AckOffset from
// the shared peer without the registry lock that UpdateAck writes it under; the
// race detector reports that, so this test is meaningful under -race.
func TestServerStatsConcurrentWithAckUpdates(t *testing.T) {
	srv := New(config.Default(), slog.New(slog.NewTextHandler(io.Discard, nil)), storage.NewStore(), stubExecutor{})
	srv.replicaPeers.Add(1, &stubConn{}, 6380, nil)

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
				srv.replicaPeers.UpdateAck(1, offset)
			}
		}
	}()

	var last int64
	for i := 0; i < 2000; i++ {
		stats := srv.ServerStats()
		if len(stats.Replicas) != 1 {
			t.Fatalf("len(Replicas) = %d, want 1", len(stats.Replicas))
		}
		if got := stats.Replicas[0].AckOffset; got < last {
			t.Fatalf("AckOffset went backwards from %d to %d", last, got)
		} else {
			last = got
		}
	}
	close(stop)
	wg.Wait()
}
