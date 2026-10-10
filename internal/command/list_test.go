package command

import (
	"bufio"
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/server"
)

func TestAWokenBLPopDoesNotPopForAClientThatLeftWhileItWaitedForThePusher(t *testing.T) {
	// With an AOF or a Replica, a push wakes a blocked BLPOP while it still holds
	// the key's stripe, and holds it until its own frame has been logged (and
	// fsynced, under appendfsync always) and replicated. The woken BLPOP waits for
	// the stripe before it pops. It used to look at its client only before that
	// wait, so a client that left during it still had the element popped into
	// its closed connection, logged and replicated as LPOP, and the client waiting
	// behind it was never woken.
	executor := newTestExecutor()
	client, conn := tcpPair(t)
	state := newTestClientState(executor, 1)
	state.BindResponseWriter(bufio.NewWriter(conn))
	state.BindResponseConn(conn)
	ctx, cancel := context.WithCancel(server.WithClientState(context.Background(), state))
	defer cancel()

	// BLPOP flushes the replies queued ahead of it only once it is waiting.
	if err := state.QueueResponses([]protocol.Value{protocol.SimpleString{Value: "PONG"}}); err != nil {
		t.Fatalf("QueueResponses() error = %v", err)
	}
	type outcome struct {
		result server.ExecuteResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := handle(ctx, executor, requestValue("BLPOP", "jobs"), true)
		done <- outcome{result, err}
	}()
	if err := client.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline() error = %v", err)
	}
	if reply, err := protocol.NewParser(client).Parse(); err != nil {
		t.Fatalf("no PONG ahead of the blocked BLPOP: %v", err)
	} else {
		assertValueEqual(t, reply, protocol.SimpleString{Value: "PONG"})
	}
	next := executor.store.SubscribeListPush("jobs")

	// Push as a client does with an AOF: the key's stripe stays held after the
	// push, until the push has been logged.
	stripe := &executor.seq.stripes[stripeOf([]byte("jobs"))]
	stripe.Lock()
	stripeHeld := true
	defer func() {
		if stripeHeld {
			stripe.Unlock()
		}
	}()
	if _, _, err := executor.store.RightPush("jobs", [][]byte{[]byte("job-1")}); err != nil {
		t.Fatalf("RightPush() error = %v", err)
	}

	// The woken BLPOP takes the gate shared on its way to the stripe, so once the
	// gate cannot be taken exclusively it is past its wake-up and waiting.
	waitFor(t, "the woken BLPOP to wait for the stripe", func() bool {
		if executor.seq.gate.TryLock() {
			executor.seq.gate.Unlock()
			return false
		}
		return true
	})
	if err := client.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	waitFor(t, "the server side to see the client leave", func() bool { return server.ClientDisconnected(ctx) })
	stripe.Unlock()
	stripeHeld = false

	var got outcome
	select {
	case got = <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("BLPOP did not return within 2s of the stripe's release")
	}
	if got.result.Release != nil {
		got.result.Release()
	}
	if !errors.Is(got.err, server.ErrClientDisconnected) {
		t.Fatalf("BLPOP = (%v, durability %v), error %v; want error %v", got.result.Responses, got.result.Durability, got.err, server.ErrClientDisconnected)
	}
	if len(got.result.Durability) != 0 || len(got.result.Propagation) != 0 {
		t.Fatalf("BLPOP durability %v, propagation %v; want none", got.result.Durability, got.result.Propagation)
	}
	values, err := executor.store.ListRange("jobs", 0, -1)
	if err != nil || len(values) != 1 || string(values[0]) != "job-1" {
		t.Fatalf("ListRange(jobs) = (%q, %v), want ([job-1], nil)", values, err)
	}
	select {
	case <-next:
	default:
		t.Fatal("the client waiting behind the one that left was not woken")
	}
}

// tcpPair returns the two ends of a loopback TCP connection: the client's, and
// the server's.
func tcpPair(t *testing.T) (clientEnd, serverEnd net.Conn) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	defer func() { _ = listener.Close() }()
	clientEnd, err = net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	t.Cleanup(func() { _ = clientEnd.Close() })
	serverEnd, err = listener.Accept()
	if err != nil {
		t.Fatalf("Accept() error = %v", err)
	}
	t.Cleanup(func() { _ = serverEnd.Close() })
	return clientEnd, serverEnd
}

// waitFor polls until ready reports true, failing if it does not within 2s.
func waitFor(t *testing.T, what string, ready func() bool) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for !ready() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}
