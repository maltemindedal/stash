package test

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/maltemindedal/stash/internal/command"
	"github.com/maltemindedal/stash/internal/config"
	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/rdb"
	"github.com/maltemindedal/stash/internal/storage"
)

// TestAWriteMadeWhileAFullResyncIsSentReachesTheReplica has a client write while
// the master is still sending a replica its full resync, and checks that the
// write reaches the replica exactly once, in the snapshot or in the stream that
// follows it. The default networking mode used to register the replica only once
// the whole snapshot was on the socket, so a write in between was in neither.
// With WAIT's base offset that also made WAIT count the replica for the write.
// The event-loop row is TestAWriteMadeWhileAFullResyncIsSentReachesTheReplicaUnderTheEventLoop.
func TestAWriteMadeWhileAFullResyncIsSentReachesTheReplica(t *testing.T) {
	tests := []struct {
		name string
		aof  bool
		// wait has the writer run WAIT 1 after its write, which must return 1,
		// and only once the replica has acknowledged a stream that holds it.
		wait bool
	}{
		{name: "default networking"},
		{name: "default networking with an append-only file", aof: true},
		{name: "WAIT counts the replica once it holds the write", wait: true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			cfg := defaultTestConfig()
			if tt.aof {
				cfg.AOFPath = filepath.Join(t.TempDir(), "master.aof")
			}
			writeDuringFullResync(t, cfg, tt.wait)
		})
	}
}

// fullResyncKeys fills the master with a snapshot of about 21 MB, more than the
// socket buffers between it and a replica that is not reading hold, so the
// master is still sending it when the test writes.
const fullResyncKeys = 100000

// writeDuringFullResync starts a master on cfg, attaches a replica that stops
// reading after PSYNC, writes from another client once the master has copied
// the snapshot, and then lets the replica read. With wait, the writer then runs
// WAIT 1 and the replica acknowledges what it read when the master asks.
func writeDuringFullResync(t *testing.T, cfg config.Config, wait bool) {
	t.Helper()

	store := storage.NewStore()
	value := bytes.Repeat([]byte("v"), 200)
	for i := 0; i < fullResyncKeys; i++ {
		if _, err := store.Set(fmt.Sprintf("bulk-%06d", i), value, 0); err != nil {
			t.Fatalf("Set() error = %v", err)
		}
	}

	var logs synchronizedBuffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo}))
	srv := newServer(t, cfg, logger, store)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe(ctx) }()
	addr := waitForAddr(t, srv)

	replica, replicaParser := dialClient(t, addr)
	assertCommandResponse(t, replica, replicaParser, protocol.SimpleString{Value: "OK"}, "REPLCONF", "listening-port", "6380")
	if err := protocol.WriteValue(replica, request("PSYNC", "?", "-1")); err != nil {
		t.Fatalf("WriteValue(PSYNC) error = %v", err)
	}
	// The master logs this once it has copied the snapshot and before it sends it.
	waitForLogFragments(t, &logs, 10*time.Second, "serving full resync to replica")

	client, clientParser := dialClient(t, addr)
	assertCommandResponse(t, client, clientParser, protocol.SimpleString{Value: "OK"}, "SET", "written-during-resync", "x")

	if err := replica.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline() error = %v", err)
	}
	if _, err := replicaParser.Parse(); err != nil { // +FULLRESYNC <replid> 0
		t.Fatalf("Parse() FULLRESYNC error = %v", err)
	}
	snapshotValue, err := replicaParser.Parse()
	if err != nil {
		t.Fatalf("Parse() snapshot error = %v", err)
	}
	snapshot, ok := snapshotValue.(protocol.BulkString)
	if !ok {
		t.Fatalf("snapshot type = %T, want protocol.BulkString", snapshotValue)
	}
	loaded := storage.NewStore()
	if _, err := rdb.LoadReader(bytes.NewReader(snapshot.Data), loaded); err != nil {
		t.Fatalf("LoadReader(snapshot) error = %v", err)
	}
	_, inSnapshot, err := loaded.Get("written-during-resync")
	if err != nil {
		t.Fatalf("Get() from the snapshot error = %v", err)
	}

	// Once the master logs this, it sends the replica everything written after,
	// so the frames that follow are in the stream however the write before was
	// handled.
	waitForLogFragments(t, &logs, 10*time.Second, "replica registered")
	type waitReply struct {
		value protocol.Value
		err   error
	}
	waitReplies := make(chan waitReply, 1)
	if wait {
		if err := protocol.WriteValue(client, request("WAIT", "1", "5000")); err != nil {
			t.Fatalf("WriteValue(WAIT) error = %v", err)
		}
		go func() {
			value, err := clientParser.Parse()
			waitReplies <- waitReply{value, err}
		}()
	} else {
		// A later write, to know where to stop reading.
		assertCommandResponse(t, client, clientParser, protocol.SimpleString{Value: "OK"}, "SET", "later", "x")
	}

	// Read the stream as a replica applies it, counting the bytes it processed.
	inStream := 0
	processed := 0
	for {
		frame, err := replicaParser.Parse()
		if err != nil {
			t.Fatalf("Parse() stream error = %v (the write was in the snapshot: %v, in the stream %d times)", err, inSnapshot, inStream)
		}
		size, err := protocol.EncodedLen(frame)
		if err != nil {
			t.Fatalf("EncodedLen() error = %v", err)
		}
		processed += size
		propagated, err := command.DecodeRequest(frame)
		if err != nil {
			t.Fatalf("DecodeRequest() error = %v", err)
		}
		if propagated.Name == "SET" && len(propagated.Args) > 0 && string(propagated.Args[0]) == "written-during-resync" {
			inStream++
		}
		if !wait && propagated.Name == "SET" && len(propagated.Args) > 0 && string(propagated.Args[0]) == "later" {
			break
		}
		if wait && propagated.Name == "REPLCONF" && len(propagated.Args) > 0 && strings.EqualFold(string(propagated.Args[0]), "GETACK") {
			select {
			case reply := <-waitReplies:
				t.Fatalf("WAIT answered %#v (error %v) before the replica acknowledged anything", reply.value, reply.err)
			default:
			}
			// Acknowledge everything read since the snapshot, the GETACK
			// included, as a Stash replica does.
			if err := protocol.WriteValue(replica, request("REPLCONF", "ACK", strconv.Itoa(processed))); err != nil {
				t.Fatalf("WriteValue(REPLCONF ACK) error = %v", err)
			}
			break
		}
	}

	held := inSnapshot || inStream > 0
	if wait {
		select {
		case reply := <-waitReplies:
			if reply.err != nil {
				t.Fatalf("Parse() WAIT reply error = %v", reply.err)
			}
			assertValuesEqual(t, reply.value, protocol.Integer{Value: 1})
			if !held {
				t.Fatal("WAIT counted the replica for a write that is in neither its snapshot nor its stream")
			}
		case <-time.After(10 * time.Second):
			t.Fatal("WAIT did not answer after the replica acknowledged")
		}
	}
	if count := inStream + boolCount(inSnapshot); count != 1 {
		t.Fatalf("the write reached the replica %d times (in the snapshot: %v, in the stream: %d times), want exactly once", count, inSnapshot, inStream)
	}

	cancel()
	waitForServerStop(t, errCh)
}

func boolCount(b bool) int {
	if b {
		return 1
	}
	return 0
}
