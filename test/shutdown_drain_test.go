package test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/maltemindedal/stash/internal/config"
	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/server"
	"github.com/maltemindedal/stash/internal/storage"
)

// TestAReplicaReceivesEveryAcknowledgedWriteWhenItsMasterShutsDown stops a master
// gracefully, by cancelling its context, while a replica that has stopped reading
// has more of the stream queued than the sockets between them hold. The master
// gives replicas up to a second to take what is queued, so once the replica
// reads again it must receive every write acknowledged before the shutdown, and
// then the end of the stream, not a frame cut short. Both networking modes used
// to close the replica's socket at once: the default mode's handler closes every
// connection when the context is cancelled, and the event loop closes every
// connection when it stops, so the replica received about a quarter of the
// writes, the last of them cut off.
//
// A replica that acknowledges while it reads, as a Redis replica does every
// second, must receive everything too. The master closed a replica's socket as
// soon as its output had reached the kernel, with several MiB still in the send
// queue, and the next acknowledgement drew a reset that discarded them.
func TestAReplicaReceivesEveryAcknowledgedWriteWhenItsMasterShutsDown(t *testing.T) {
	tests := []struct {
		name      string
		eventLoop bool
		acks      bool
	}{
		{name: "default networking"},
		{name: "default networking, with a replica acknowledging while it reads", acks: true},
		{name: "event loop", eventLoop: true},
		{name: "event loop, with a replica acknowledging while it reads", eventLoop: true, acks: true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			cfg := defaultTestConfig()
			cfg.EventLoop = tt.eventLoop
			replicaDrainsAtShutdown(t, cfg, func(drain *shutdownDrain) {
				drain.cancel()
				if tt.acks {
					drain.acknowledgeWhileReading(t)
				}
			}, nil)
		})
	}
}

// drainedWrites is how many writes of drainedValueSize bytes the replica in
// replicaDrainsAtShutdown has queued: 16 MiB, more than the socket buffers
// between master and replica hold, so most of it is still in the master's queue
// for the replica when the master stops.
const (
	drainedWrites    = 256
	drainedValueSize = 64 << 10
)

// shutdownDrain is a master during replicaDrainsAtShutdown, with a client
// connection and the logs it writes, for the step that stops it.
type shutdownDrain struct {
	master       *server.Server
	addr         string
	cancel       context.CancelFunc
	client       net.Conn
	clientParser *protocol.Parser
	logs         *synchronizedBuffer

	replica  net.Conn
	stopAcks chan struct{}
	ackers   sync.WaitGroup
}

// acknowledgeWhileReading has the replica send REPLCONF ACK every few
// milliseconds until it has read the end of the stream, as a replica that
// acknowledges on its own does.
func (d *shutdownDrain) acknowledgeWhileReading(t *testing.T) {
	t.Helper()

	ack, err := protocol.EncodeValues([]protocol.Value{request("REPLCONF", "ACK", "0")})
	if err != nil {
		t.Fatalf("EncodeValues() error = %v", err)
	}
	d.ackers.Add(1)
	go func() {
		defer d.ackers.Done()
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-d.stopAcks:
				return
			case <-ticker.C:
				if _, err := d.replica.Write(ack); err != nil {
					return
				}
			}
		}
	}()
}

// replicaDrainsAtShutdown starts a master on cfg and attaches a replica that does
// not read, makes drainedWrites acknowledged writes, and has stop end the master.
// The replica then reads, and must receive those writes in order and then the
// end of the stream. ListenAndServe must return wantErr.
func replicaDrainsAtShutdown(t *testing.T, cfg config.Config, stop func(*shutdownDrain), wantErr error) {
	t.Helper()

	logs := &synchronizedBuffer{}
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo}))
	master := newServer(t, cfg, logger, storage.NewStore())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- master.ListenAndServe(ctx) }()
	addr := waitForAddr(t, master)

	replica, replicaParser := attachTestReplica(t, master, addr)
	client, clientParser := dialClient(t, addr)
	value := string(bytes.Repeat([]byte("v"), drainedValueSize))
	for i := 0; i < drainedWrites; i++ {
		assertCommandResponse(t, client, clientParser, protocol.SimpleString{Value: "OK"}, "SET", fmt.Sprintf("key-%03d", i), value)
	}

	drain := &shutdownDrain{master: master, addr: addr, cancel: cancel, client: client, clientParser: clientParser, logs: logs, replica: replica, stopAcks: make(chan struct{})}
	var stopAcksOnce sync.Once
	stopAcks := func() {
		stopAcksOnce.Do(func() { close(drain.stopAcks) })
		drain.ackers.Wait()
	}
	defer stopAcks()
	stop(drain)

	if err := replica.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline() error = %v", err)
	}
	for i := 0; i < drainedWrites; i++ {
		if err := assertReplicaRequest(replicaParser, "SET", fmt.Sprintf("key-%03d", i), value); err != nil {
			t.Fatalf("write %d of the %d acknowledged before shutdown: %v", i+1, drainedWrites, err)
		}
	}
	if extra, err := replicaParser.Parse(); !errors.Is(err, io.EOF) {
		t.Fatalf("after the acknowledged writes the replica read %.80q with error %v, want the end of the stream", fmt.Sprint(extra), err)
	}
	// As a replica does at the end of its master's stream.
	stopAcks()
	if tcp, ok := replica.(*net.TCPConn); ok {
		_ = tcp.CloseWrite()
	}

	select {
	case err := <-errCh:
		if !errors.Is(err, wantErr) {
			t.Fatalf("ListenAndServe() error = %v, want %v", err, wantErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the master did not stop within 5s")
	}
}
