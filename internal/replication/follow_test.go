package replication

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/maltemindedal/stash/internal/protocol"
)

// TestFollowDoublesItsWaitToThirtySecondsAndStartsOverAfterASynchronisedLink
// drives Follow with a dial and a sleep the test controls, so no time passes.
// Links that fail before the handshake completes are retried after waits that
// double from one second to thirty and stay there. A link that completed its
// handshake and then ended is retried after one second again. Cancelling the
// context during a wait ends Follow.
func TestFollowDoublesItsWaitToThirtySecondsAndStartsOverAfterASynchronisedLink(t *testing.T) {
	// The seventh and the tenth link synchronise before they end; every other
	// dial fails.
	synchronises := map[int]bool{7: true, 10: true}
	want := []time.Duration{
		time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 30 * time.Second,
		time.Second, // after the seventh link, which synchronised
		2 * time.Second, 4 * time.Second,
		time.Second, // after the tenth
		2 * time.Second,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dials := 0
	dial := func(context.Context) (net.Conn, error) {
		dials++
		if !synchronises[dials] {
			return nil, errors.New("connection refused")
		}
		upstream, masterConn := net.Pipe()
		go func() {
			defer func() { _ = masterConn.Close() }()
			parser := protocol.NewParser(masterConn)
			replies := [][]protocol.Value{
				{protocol.SimpleString{Value: "PONG"}},
				{protocol.SimpleString{Value: "OK"}},
				{protocol.SimpleString{Value: "FULLRESYNC test-replid 0"}, protocol.BulkString{}},
			}
			for _, reply := range replies {
				if _, err := parser.Parse(); err != nil {
					return
				}
				payload, err := protocol.EncodeValues(reply)
				if err != nil {
					return
				}
				if _, err := masterConn.Write(payload); err != nil {
					return
				}
			}
			// Closing ends the link after its handshake.
		}()
		return upstream, nil
	}

	var waits []time.Duration
	sleep := func(_ context.Context, d time.Duration) bool {
		waits = append(waits, d)
		if len(waits) == len(want) {
			cancel()
			return false
		}
		return true
	}

	link := &Link{ListeningPort: 6380, Store: &linkStore{}, Logger: discardLogger()}
	done := make(chan struct{})
	go func() {
		defer close(done)
		Follow(ctx, dial, link, sleep)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Follow() had not returned 10s after its context was cancelled")
	}

	if fmt.Sprint(waits) != fmt.Sprint(want) {
		t.Fatalf("Follow() waited %v between links, want %v", waits, want)
	}
	if dials != len(want) {
		t.Fatalf("Follow() dialled %d times, want %d", dials, len(want))
	}
}

// TestFollowEndsARunningLinkWhenItsContextIsCancelled cancels Follow while its
// link is applying the stream of a Master that never closes the connection.
// Follow closes the connection, which ends the link, and returns without
// dialling again: a server whose accept loop failed used to keep its link.
func TestFollowEndsARunningLinkWhenItsContextIsCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	synchronised := make(chan struct{})
	masterDone := make(chan error, 1)
	dials := 0
	dial := func(context.Context) (net.Conn, error) {
		dials++
		upstream, masterConn := net.Pipe()
		go func() {
			parser := protocol.NewParser(masterConn)
			for _, reply := range []string{"+PONG\r\n", "+OK\r\n", "+FULLRESYNC test-replid 0\r\n$0\r\n\r\n"} {
				if _, err := parser.Parse(); err != nil {
					masterDone <- err
					return
				}
				if _, err := masterConn.Write([]byte(reply)); err != nil {
					masterDone <- err
					return
				}
			}
			close(synchronised)
			// The Master stays: only the Replica's side closing ends the read.
			_, err := parser.Parse()
			masterDone <- err
		}()
		return upstream, nil
	}
	sleep := func(context.Context, time.Duration) bool {
		t.Error("Follow() waited to retry after its context was cancelled")
		return false
	}

	link := &Link{ListeningPort: 6380, Store: &linkStore{}, Logger: discardLogger()}
	done := make(chan struct{})
	go func() {
		defer close(done)
		Follow(ctx, dial, link, sleep)
	}()

	select {
	case <-synchronised:
	case <-time.After(10 * time.Second):
		t.Fatal("the link had not synchronised after 10s")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Follow() had not returned 10s after its context was cancelled")
	}
	select {
	case err := <-masterDone:
		if err == nil {
			t.Fatal("the Master read a request after the handshake, want its connection closed")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the Master's connection was not closed")
	}
	if dials != 1 {
		t.Fatalf("Follow() dialled %d times, want once", dials)
	}
}
