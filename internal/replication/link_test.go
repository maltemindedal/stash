package replication

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maltemindedal/stash/internal/protocol"
)

// linkStore is a Replica's Store that records what a Link does to it.
type linkStore struct {
	mu        sync.Mutex
	snapshots [][]byte
	applied   []string
	// applyErr, when set, is what Apply returns for the command named failOn.
	failOn   string
	applyErr error
	// replaceErr, when set, is what Replace returns.
	replaceErr error
}

func (s *linkStore) Replace(snapshot []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.replaceErr != nil {
		return s.replaceErr
	}
	s.snapshots = append(s.snapshots, bytes.Clone(snapshot))
	s.applied = nil
	return nil
}

func (s *linkStore) Apply(_ context.Context, command protocol.Value) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	words := linkWords(command)
	if s.applyErr != nil && len(words) > 0 && words[0] == s.failOn {
		return s.applyErr
	}
	s.applied = append(s.applied, strings.Join(words, " "))
	return nil
}

func (s *linkStore) state() (snapshots [][]byte, applied []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([][]byte(nil), s.snapshots...), append([]string(nil), s.applied...)
}

func linkWords(command protocol.Value) []string {
	array, ok := command.(protocol.Array)
	if !ok {
		return nil
	}
	words := make([]string, 0, len(array.Elements))
	for _, element := range array.Elements {
		raw, err := protocol.Bytes(element)
		if err != nil {
			return nil
		}
		words = append(words, string(raw))
	}
	return words
}

func linkCommand(parts ...string) protocol.Array {
	elements := make([]protocol.Value, len(parts))
	for i, part := range parts {
		elements[i] = protocol.BulkString{Data: []byte(part)}
	}
	return protocol.Array{Elements: elements}
}

// pipeMaster is the Master's end of a net.Pipe.
type pipeMaster struct {
	t      *testing.T
	conn   net.Conn
	parser *protocol.Parser
}

// expect reads the Replica's next request and requires it to be want.
func (m *pipeMaster) expect(want ...string) {
	m.t.Helper()
	value, err := m.parser.Parse()
	if err != nil {
		m.t.Fatalf("reading the Replica's %v: %v", want, err)
	}
	if got := linkWords(value); strings.Join(got, " ") != strings.Join(want, " ") {
		m.t.Fatalf("the Replica sent %q, want %q", got, want)
	}
}

// send writes values to the Replica and returns how many bytes they encode to.
func (m *pipeMaster) send(values ...protocol.Value) int {
	m.t.Helper()
	payload, err := protocol.EncodeValues(values)
	if err != nil {
		m.t.Fatalf("encoding %v: %v", values, err)
	}
	if _, err := m.conn.Write(payload); err != nil {
		m.t.Fatalf("writing to the Replica: %v", err)
	}
	return len(payload)
}

type linkResult struct {
	synchronised bool
	err          error
}

// runLink runs link against a fake Master over a net.Pipe, and returns the
// Master's end and the channel Run's result arrives on.
func runLink(t *testing.T, ctx context.Context, link *Link) (*pipeMaster, <-chan linkResult) {
	t.Helper()
	upstream, masterConn := net.Pipe()
	t.Cleanup(func() {
		_ = upstream.Close()
		_ = masterConn.Close()
	})
	// A step that never arrives fails the test instead of hanging it.
	if err := masterConn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatalf("SetDeadline() error = %v", err)
	}
	done := make(chan linkResult, 1)
	go func() {
		synchronised, err := link.Run(ctx, upstream)
		done <- linkResult{synchronised, err}
	}()
	return &pipeMaster{t: t, conn: masterConn, parser: protocol.NewParser(masterConn)}, done
}

func awaitLink(t *testing.T, done <-chan linkResult) linkResult {
	t.Helper()
	select {
	case result := <-done:
		return result
	case <-time.After(10 * time.Second):
		t.Fatal("Run() had not returned after 10s")
		return linkResult{}
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestReplicaLinkInstallsTheFullResyncThenAppliesTheStreamAndAcknowledgesItsOffset
// walks a Link through a whole attachment over a net.Pipe: the handshake in
// order, the snapshot replacing the Store, each command of the stream applied,
// and REPLCONF GETACK answered by the Link itself with the bytes of the stream
// it has processed, the GETACK included, counting from zero after the full
// resync.
func TestReplicaLinkInstallsTheFullResyncThenAppliesTheStreamAndAcknowledgesItsOffset(t *testing.T) {
	store := &linkStore{}
	link := &Link{ListeningPort: 6380, MasterAuth: "secret", Store: store, Logger: discardLogger()}
	master, done := runLink(t, context.Background(), link)

	master.expect("PING")
	master.send(protocol.SimpleString{Value: "PONG"})
	master.expect("AUTH", "secret")
	master.send(protocol.SimpleString{Value: "OK"})
	master.expect("REPLCONF", "listening-port", "6380")
	master.send(protocol.SimpleString{Value: "OK"})
	master.expect("PSYNC", "?", "-1")
	master.send(protocol.SimpleString{Value: "FULLRESYNC test-replid 0"}, protocol.BulkString{Data: []byte("the snapshot")})

	sent := master.send(linkCommand("SET", "key", "value"), linkCommand("DEL", "other"))
	sent += master.send(linkCommand("REPLCONF", "GETACK", "*"))
	master.expect("REPLCONF", "ACK", strconv.Itoa(sent))

	// A second GETACK counts the first one and itself.
	sent += master.send(linkCommand("replconf", "getack", "*"))
	master.expect("REPLCONF", "ACK", strconv.Itoa(sent))

	snapshots, applied := store.state()
	if len(snapshots) != 1 || string(snapshots[0]) != "the snapshot" {
		t.Fatalf("the Store was replaced with %q, want the snapshot once", snapshots)
	}
	if want := []string{"SET key value", "DEL other"}; fmt.Sprint(applied) != fmt.Sprint(want) {
		t.Fatalf("applied %q, want %q: a GETACK is answered, not applied", applied, want)
	}
	if got := link.Offset(); got != int64(sent) {
		t.Fatalf("Offset() = %d, want the %d bytes of the stream", got, sent)
	}

	// The Master going away after the handshake is a link that ended, not one
	// that failed.
	_ = master.conn.Close()
	if result := awaitLink(t, done); !result.synchronised || result.err != nil {
		t.Fatalf("Run() = (%v, %v) after the Master closed the link, want (true, nil)", result.synchronised, result.err)
	}
}

// TestReplicaLinkReportsEachHandshakeRefusal has the Master refuse each step of
// the handshake in turn. Run reports that the handshake did not complete and
// which step failed, and the Store is left alone.
func TestReplicaLinkReportsEachHandshakeRefusal(t *testing.T) {
	pong := protocol.SimpleString{Value: "PONG"}
	ok := protocol.SimpleString{Value: "OK"}
	tests := []struct {
		name       string
		masterAuth string
		// replies are the Master's answers to the Replica's requests, in order.
		replies    [][]protocol.Value
		replaceErr error
		wantErr    []string
	}{
		{
			name:    "PING is answered with an error",
			replies: [][]protocol.Value{{protocol.ErrorValue{Message: "ERR busy"}}},
			wantErr: []string{"invalid PING response from master", "ERR busy"},
		},
		{
			name:       "the password is wrong",
			masterAuth: "wrong",
			replies:    [][]protocol.Value{{pong}, {protocol.ErrorValue{Message: "WRONGPASS invalid username-password pair or user is disabled."}}},
			wantErr:    []string{"replica AUTH failed", "replica AUTH rejected by master", "WRONGPASS"},
		},
		{
			name:    "the Master needs a password the Replica does not have",
			replies: [][]protocol.Value{{pong}, {protocol.ErrorValue{Message: "NOAUTH Authentication required."}}},
			wantErr: []string{"replica REPLCONF failed", "protected master requires replica authentication (--masterauth)", "NOAUTH Authentication required."},
		},
		{
			name:    "REPLCONF is answered with an error",
			replies: [][]protocol.Value{{pong}, {protocol.ErrorValue{Message: "ERR syntax error"}}},
			wantErr: []string{"replica REPLCONF failed", "ERR syntax error"},
		},
		{
			name:    "PSYNC is answered with an error",
			replies: [][]protocol.Value{{pong}, {ok}, {protocol.ErrorValue{Message: "ERR syntax error"}}},
			wantErr: []string{"failed to consume FULLRESYNC from master", "unexpected FULLRESYNC response type"},
		},
		{
			name:    "FULLRESYNC names an offset other than 0",
			replies: [][]protocol.Value{{pong}, {ok}, {protocol.SimpleString{Value: "FULLRESYNC test-replid 42"}}},
			wantErr: []string{"failed to consume FULLRESYNC from master", "invalid FULLRESYNC response"},
		},
		{
			name:    "the snapshot is not a bulk string",
			replies: [][]protocol.Value{{pong}, {ok}, {protocol.SimpleString{Value: "FULLRESYNC test-replid 0"}, protocol.Integer{Value: 1}}},
			wantErr: []string{"failed to consume FULLRESYNC from master", "unexpected replication snapshot type"},
		},
		{
			name:       "the snapshot does not load",
			replies:    [][]protocol.Value{{pong}, {ok}, {protocol.SimpleString{Value: "FULLRESYNC test-replid 0"}, protocol.BulkString{Data: []byte("garbage")}}},
			replaceErr: errors.New("bad magic"),
			wantErr:    []string{"failed to consume FULLRESYNC from master", "load replication snapshot", "bad magic"},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			store := &linkStore{replaceErr: tt.replaceErr}
			link := &Link{ListeningPort: 6380, MasterAuth: tt.masterAuth, Store: store, Logger: discardLogger()}
			master, done := runLink(t, context.Background(), link)

			for _, reply := range tt.replies {
				if _, err := master.parser.Parse(); err != nil {
					t.Fatalf("reading the Replica's request: %v", err)
				}
				master.send(reply...)
			}

			result := awaitLink(t, done)
			if result.synchronised || result.err == nil {
				t.Fatalf("Run() = (%v, %v), want (false, an error)", result.synchronised, result.err)
			}
			for _, fragment := range tt.wantErr {
				if !strings.Contains(result.err.Error(), fragment) {
					t.Fatalf("Run() error = %q, want it to contain %q", result.err, fragment)
				}
			}
			if snapshots, applied := store.state(); len(snapshots) != 0 || len(applied) != 0 {
				t.Fatalf("the Store was replaced %d times and applied %q, want it untouched", len(snapshots), applied)
			}
		})
	}
}

// TestReplicaLinkEndsWhenACommandOfTheStreamFails has the Store refuse a
// command, as it does when the command cannot be appended to the Replica's AOF.
// The link ends with that error after a completed handshake, so the Replica
// synchronises again; a cancelled context ends it without an error.
func TestReplicaLinkEndsWhenACommandOfTheStreamFails(t *testing.T) {
	errDiskFull := errors.New("disk full")
	tests := []struct {
		name     string
		applyErr error
		wantErr  error
	}{
		{name: "the Store refuses the command", applyErr: errDiskFull, wantErr: errDiskFull},
		{name: "the context was cancelled", applyErr: context.Canceled},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			store := &linkStore{failOn: "SET", applyErr: tt.applyErr}
			link := &Link{ListeningPort: 6380, Store: store, Logger: discardLogger()}
			master, done := runLink(t, context.Background(), link)

			master.expect("PING")
			master.send(protocol.SimpleString{Value: "PONG"})
			master.expect("REPLCONF", "listening-port", "6380")
			master.send(protocol.SimpleString{Value: "OK"})
			master.expect("PSYNC", "?", "-1")
			master.send(protocol.SimpleString{Value: "FULLRESYNC test-replid 0"}, protocol.BulkString{})
			master.send(linkCommand("SET", "key", "value"))

			result := awaitLink(t, done)
			if !result.synchronised {
				t.Fatal("Run() reported no completed handshake, want one")
			}
			if tt.wantErr == nil && result.err != nil {
				t.Fatalf("Run() error = %v, want nil", result.err)
			}
			if tt.wantErr != nil && !errors.Is(result.err, tt.wantErr) {
				t.Fatalf("Run() error = %v, want it to wrap %v", result.err, tt.wantErr)
			}
		})
	}
}
