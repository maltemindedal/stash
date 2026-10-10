package server

import (
	"context"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maltemindedal/stash/internal/config"
	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/rdb"
	"github.com/maltemindedal/stash/internal/storage"
)

// recordingExecutor answers every request with +OK, and records the Call each
// one came with and whether the caller released its result.
type recordingExecutor struct {
	mu    sync.Mutex
	calls []recordedCall
}

type recordedCall struct {
	command  string
	call     Call
	released *atomic.Bool
}

func (r *recordingExecutor) Handle(_ context.Context, call Call, request protocol.Value) (ExecuteResult, error) {
	released := &atomic.Bool{}
	r.mu.Lock()
	r.calls = append(r.calls, recordedCall{command: replicationCommandName(request), call: call, released: released})
	r.mu.Unlock()

	result := SingleResponse(protocol.SimpleString{Value: "OK"})
	result.Release = func() { released.Store(true) }
	return result, nil
}

// released waits for the first request named command to be executed and its
// result released, and returns its Call.
func (r *recordingExecutor) released(t *testing.T, command string) Call {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		r.mu.Lock()
		for _, recorded := range r.calls {
			if recorded.command == command && recorded.released.Load() {
				r.mu.Unlock()
				return recorded.call
			}
		}
		r.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("no %s was executed and released within 5s", command)
	return Call{}
}

// fakeMaster accepts one Replica, answers its handshake with a full
// resynchronisation to an empty dataset, and then sends it stream.
func fakeMaster(t *testing.T, stream protocol.Value) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()

		parser := protocol.NewParser(conn)
		// PING, then REPLCONF listening-port.
		for _, reply := range []string{"+PONG\r\n", "+OK\r\n"} {
			if _, err := parser.Parse(); err != nil {
				return
			}
			if _, err := conn.Write([]byte(reply)); err != nil {
				return
			}
		}
		// PSYNC ? -1.
		if _, err := parser.Parse(); err != nil {
			return
		}
		snapshot, _ := rdb.BuildSnapshot(nil)
		if _, err := conn.Write([]byte("+FULLRESYNC " + strings.Repeat("a", 40) + " 0\r\n")); err != nil {
			return
		}
		if err := protocol.WriteValue(conn, protocol.BulkString{Data: snapshot}); err != nil {
			return
		}
		if err := protocol.WriteValue(conn, stream); err != nil {
			return
		}
		_, _ = io.Copy(io.Discard, conn)
	}()

	return listener.Addr().String()
}

func setFrame() protocol.Array {
	return protocol.Array{Elements: []protocol.Value{
		protocol.BulkString{Data: []byte("SET")},
		protocol.BulkString{Data: []byte("key")},
		protocol.BulkString{Data: []byte("value")},
	}}
}

func TestEveryExecutionPathPassesItsOrigin(t *testing.T) {
	// Only TCP tests covered these paths before: each builds its Call, and a
	// path that passed the wrong one would run with another Origin's rights.
	tests := []struct {
		name       string
		eventLoop  bool
		configure  func(t *testing.T, cfg *config.Config)
		drive      func(t *testing.T, addr string)
		command    string
		wantOrigin Origin
		wantClient bool
		wantInline bool
	}{
		{
			name:       "a client connection",
			drive:      sendPing,
			command:    "PING",
			wantOrigin: OriginClient,
			wantClient: true,
		},
		{
			name:       "a client connection on the event loop",
			eventLoop:  true,
			drive:      sendPing,
			command:    "PING",
			wantOrigin: OriginClient,
			wantClient: true,
			wantInline: true,
		},
		{
			name: "AOF replay",
			configure: func(t *testing.T, cfg *config.Config) {
				payload, err := protocol.Encode(setFrame())
				if err != nil {
					t.Fatalf("Encode() error = %v", err)
				}
				cfg.AOFPath = filepath.Join(t.TempDir(), "appendonly.aof")
				if err := os.WriteFile(cfg.AOFPath, payload, 0o600); err != nil {
					t.Fatalf("WriteFile() error = %v", err)
				}
			},
			command:    "SET",
			wantOrigin: OriginReplay,
		},
		{
			name: "the Master's replication stream",
			configure: func(t *testing.T, cfg *config.Config) {
				cfg.ReplicaOf = fakeMaster(t, setFrame())
			},
			command:    "SET",
			wantOrigin: OriginMaster,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.eventLoop && runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
				t.Skip("the event loop runs on Linux and macOS")
			}
			cfg := config.Default()
			cfg.Port = 0
			cfg.DumpPath = ""
			cfg.EventLoop = tt.eventLoop
			if tt.configure != nil {
				tt.configure(t, &cfg)
			}
			executor := &recordingExecutor{}
			srv := newTestServer(t, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), storage.NewStore(), executor)

			ctx, cancel := context.WithCancel(context.Background())
			errCh := make(chan error, 1)
			go func() { errCh <- srv.ListenAndServe(ctx) }()
			defer func() {
				cancel()
				select {
				case err := <-errCh:
					if err != nil {
						t.Errorf("ListenAndServe() error = %v", err)
					}
				case <-time.After(5 * time.Second):
					t.Error("the server did not stop within 5s")
				}
			}()

			addr := listeningAddr(t, srv)
			if tt.drive != nil {
				tt.drive(t, addr)
			}

			call := executor.released(t, tt.command)
			if call.Origin != tt.wantOrigin || (call.Client != nil) != tt.wantClient || call.Inline != tt.wantInline {
				t.Fatalf("%s ran with Call{Origin: %d, Client set: %v, Inline: %v}, want Origin %d, Client set %v, Inline %v",
					tt.command, call.Origin, call.Client != nil, call.Inline, tt.wantOrigin, tt.wantClient, tt.wantInline)
			}
		})
	}
}

// listeningAddr waits for srv to listen and returns its address.
func listeningAddr(t *testing.T, srv *Server) string {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if addr := srv.Addr(); addr != "" {
			return addr
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("the server did not listen within 5s")
	return ""
}

// sendPing sends PING on a new connection and reads the reply.
func sendPing(t *testing.T, addr string) {
	t.Helper()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) error = %v", addr, err)
	}
	defer func() { _ = conn.Close() }()
	if err := protocol.WriteValue(conn, protocol.Array{Elements: []protocol.Value{protocol.BulkString{Data: []byte("PING")}}}); err != nil {
		t.Fatalf("WriteValue(PING) error = %v", err)
	}
	if _, err := protocol.NewParser(conn).Parse(); err != nil {
		t.Fatalf("Parse(PING reply) error = %v", err)
	}
}
