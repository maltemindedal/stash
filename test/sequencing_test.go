package test

import (
	"fmt"
	"net"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/maltemindedal/stash/internal/protocol"
)

// roundTrip sends one command on conn and returns its reply.
func roundTrip(t *testing.T, conn net.Conn, parser *protocol.Parser, parts ...string) protocol.Value {
	t.Helper()

	if err := conn.SetDeadline(time.Now().Add(20 * time.Second)); err != nil {
		t.Fatalf("SetDeadline() error = %v", err)
	}
	if err := protocol.WriteValue(conn, request(parts...)); err != nil {
		t.Fatalf("WriteValue(%v) error = %v", parts, err)
	}
	reply, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse(%v reply) error = %v", parts, err)
	}
	return reply
}

func dialClient(t *testing.T, addr string) (net.Conn, *protocol.Parser) {
	t.Helper()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) error = %v", addr, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn, protocol.NewParser(conn)
}

func bulkText(t *testing.T, value protocol.Value) string {
	t.Helper()

	text, isNull, ok := integrationBulkStringContent(value)
	if !ok || isNull {
		t.Fatalf("reply = %#v, want a bulk string", value)
	}
	return text
}

func TestWatchedTransactionsLoseNoUpdates(t *testing.T) {
	// The optimistic-locking pattern: WATCH, read, MULTI, write back, EXEC, and
	// try again if EXEC returns a null reply. Every EXEC that succeeds must have
	// seen the value it read, so the counter ends at exactly the number of
	// successes. Before EXEC was atomic with its check, other clients' writes
	// slipped in between the check and the queued commands.
	addr, stop, errCh := startTestServer(t, defaultTestConfig())
	defer func() {
		stop()
		waitForServerStop(t, errCh)
	}()

	const workers, perWorker = 8, 250
	admin, adminParser := dialClient(t, addr)
	roundTrip(t, admin, adminParser, "SET", "counter", "0")

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, parser := dialClient(t, addr)
			for done := 0; done < perWorker; {
				roundTrip(t, conn, parser, "WATCH", "counter")
				current, err := strconv.Atoi(bulkText(t, roundTrip(t, conn, parser, "GET", "counter")))
				if err != nil {
					t.Errorf("counter is not an integer: %v", err)
					return
				}
				roundTrip(t, conn, parser, "MULTI")
				roundTrip(t, conn, parser, "SET", "counter", strconv.Itoa(current+1))
				reply := roundTrip(t, conn, parser, "EXEC")
				if array, ok := reply.(protocol.Array); ok && !array.Null {
					done++
				}
			}
		}()
	}
	wg.Wait()

	got := bulkText(t, roundTrip(t, admin, adminParser, "GET", "counter"))
	if want := strconv.Itoa(workers * perWorker); got != want {
		t.Fatalf("counter = %s after %d successful optimistic increments, want %s: %d updates were lost", got, workers*perWorker, want, workers*perWorker-atoiOrZero(got))
	}
}

func atoiOrZero(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}

func TestTransactionsAreNotObservedHalfWayThrough(t *testing.T) {
	// EXEC runs its queued commands as one unit: a reader may see the counter
	// before the transaction or after it, never between the two INCRs.
	addr, stop, errCh := startTestServer(t, defaultTestConfig())
	defer func() {
		stop()
		waitForServerStop(t, errCh)
	}()

	var writers sync.WaitGroup
	quit := make(chan struct{})
	for w := 0; w < 3; w++ {
		writers.Add(1)
		go func() {
			defer writers.Done()
			conn, parser := dialClient(t, addr)
			for {
				select {
				case <-quit:
					return
				default:
				}
				roundTrip(t, conn, parser, "MULTI")
				roundTrip(t, conn, parser, "INCR", "pair")
				roundTrip(t, conn, parser, "INCR", "pair")
				roundTrip(t, conn, parser, "EXEC")
			}
		}()
	}

	reader, readerParser := dialClient(t, addr)
	deadline := time.Now().Add(1500 * time.Millisecond)
	odd, reads := 0, 0
	for time.Now().Before(deadline) {
		reply := roundTrip(t, reader, readerParser, "GET", "pair")
		if text, isNull, ok := integrationBulkStringContent(reply); ok && !isNull {
			reads++
			if atoiOrZero(text)%2 != 0 {
				odd++
			}
		}
	}
	close(quit)
	writers.Wait()

	if reads == 0 {
		t.Fatal("the reader never saw the counter")
	}
	if odd > 0 {
		t.Fatalf("the reader saw the counter half way through a transaction in %d of %d reads", odd, reads)
	}
}

func TestAOFRecordsConcurrentWritesInExecutionOrder(t *testing.T) {
	// Two clients pushing to one list must leave the same list behind after a
	// restart as they did in memory. The AOF used to be appended after the store
	// was updated, outside anything that ordered the two, so the file could hold
	// the pushes in a different order than they ran.
	for _, policy := range []string{"no", "everysec", "always"} {
		t.Run("appendfsync "+policy, func(t *testing.T) {
			if policy == "always" && testing.Short() {
				t.Skip("fsync per write")
			}
			aofPath := filepath.Join(t.TempDir(), "appendonly.aof")
			cfg := testAOFConfig(aofPath)
			cfg.AppendFsync = policy

			addr, stop, errCh := startTestServer(t, cfg)
			const workers, perWorker = 6, 120
			var wg sync.WaitGroup
			for w := 0; w < workers; w++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					conn, parser := dialClient(t, addr)
					for i := 0; i < perWorker; i++ {
						roundTrip(t, conn, parser, "RPUSH", "shared", fmt.Sprintf("w%d-%d", w, i))
					}
				}(w)
			}
			wg.Wait()

			admin, adminParser := dialClient(t, addr)
			live := roundTrip(t, admin, adminParser, "LRANGE", "shared", "0", "-1")
			stop()
			waitForServerStop(t, errCh)

			restartAddr, restartStop, restartErrCh := startTestServer(t, cfg)
			defer func() {
				restartStop()
				waitForServerStop(t, restartErrCh)
			}()
			restarted, restartedParser := dialClient(t, restartAddr)
			replayed := roundTrip(t, restarted, restartedParser, "LRANGE", "shared", "0", "-1")

			if got, want := len(live.(protocol.Array).Elements), workers*perWorker; got != want {
				t.Fatalf("live list has %d elements, want %d", got, want)
			}
			assertValuesEqual(t, replayed, live)
		})
	}
}

func TestBlockingCommandsInsideATransactionAnswerAtOnce(t *testing.T) {
	// EXEC holds the server's write path while it runs its queued commands, so a
	// command inside it must not wait for another client to act. Redis answers a
	// BLPOP on an empty list with a null and a WAIT with the current count.
	addr, stop, errCh := startTestServer(t, defaultTestConfig())
	defer func() {
		stop()
		waitForServerStop(t, errCh)
	}()

	conn, parser := dialClient(t, addr)
	roundTrip(t, conn, parser, "MULTI")
	roundTrip(t, conn, parser, "BLPOP", "empty")
	roundTrip(t, conn, parser, "RPUSH", "full", "x")
	roundTrip(t, conn, parser, "BLPOP", "full")
	roundTrip(t, conn, parser, "WAIT", "1", "30000")

	started := time.Now()
	reply := roundTrip(t, conn, parser, "EXEC")
	if elapsed := time.Since(started); elapsed > 3*time.Second {
		t.Fatalf("EXEC took %v; a blocking command inside it must not wait", elapsed)
	}
	assertValuesEqual(t, reply, protocol.Array{Elements: []protocol.Value{
		protocol.Array{Null: true},
		protocol.Integer{Value: 1},
		protocol.Array{Elements: []protocol.Value{
			protocol.BulkString{Data: []byte("full")},
			protocol.BulkString{Data: []byte("x")},
		}},
		protocol.Integer{Value: 0},
	}})
}

func TestATransactionRunsWhileAnotherClientIsBlockedInBLPop(t *testing.T) {
	// A client waiting in BLPOP holds nothing, so neither a transaction nor the
	// push that ends the wait is held up by it.
	addr, stop, errCh := startTestServer(t, defaultTestConfig())
	defer func() {
		stop()
		waitForServerStop(t, errCh)
	}()

	waiting, waitingParser := dialClient(t, addr)
	if err := protocol.WriteValue(waiting, request("BLPOP", "queue")); err != nil {
		t.Fatalf("WriteValue(BLPOP) error = %v", err)
	}
	admin, adminParser := dialClient(t, addr)
	waitForClients(t, admin, adminParser, 2, 2*time.Second, "the waiting client should be connected")

	other, otherParser := dialClient(t, addr)
	roundTrip(t, other, otherParser, "MULTI")
	roundTrip(t, other, otherParser, "INCR", "n")
	roundTrip(t, other, otherParser, "INCR", "n")
	assertValuesEqual(t, roundTrip(t, other, otherParser, "EXEC"), protocol.Array{Elements: []protocol.Value{
		protocol.Integer{Value: 1}, protocol.Integer{Value: 2},
	}})

	roundTrip(t, admin, adminParser, "RPUSH", "queue", "job")
	if err := waiting.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline() error = %v", err)
	}
	reply, err := waitingParser.Parse()
	if err != nil {
		t.Fatalf("Parse(BLPOP reply) error = %v", err)
	}
	assertValuesEqual(t, reply, protocol.Array{Elements: []protocol.Value{
		protocol.BulkString{Data: []byte("queue")},
		protocol.BulkString{Data: []byte("job")},
	}})
}

func TestRewriteDoesNotReplayCommandsTwice(t *testing.T) {
	// A rewrite snapshots the store and starts buffering new commands for the new
	// file. A command that had already changed the store but not yet been appended
	// would be in the snapshot and in the buffer both, and INCR would count twice
	// after a restart.
	aofPath := filepath.Join(t.TempDir(), "appendonly.aof")
	cfg := testAOFConfig(aofPath)
	cfg.AppendFsync = "no"

	addr, stop, errCh := startTestServer(t, cfg)
	const workers, perWorker = 4, 1500
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			conn, parser := dialClient(t, addr)
			for i := 0; i < perWorker; i++ {
				roundTrip(t, conn, parser, "INCR", "hits")
			}
		}(w)
	}
	admin, adminParser := dialClient(t, addr)
	rewrites := make(chan struct{})
	go func() {
		defer close(rewrites)
		for i := 0; i < 25; i++ {
			// A rewrite already in progress refuses another; keep asking.
			if err := protocol.WriteValue(admin, request("BGREWRITEAOF")); err != nil {
				return
			}
			if _, err := adminParser.Parse(); err != nil {
				return
			}
			time.Sleep(15 * time.Millisecond)
		}
	}()
	wg.Wait()
	<-rewrites
	time.Sleep(300 * time.Millisecond) // let the last rewrite finish

	reader, readerParser := dialClient(t, addr)
	live := bulkText(t, roundTrip(t, reader, readerParser, "GET", "hits"))
	stop()
	waitForServerStop(t, errCh)

	restartAddr, restartStop, restartErrCh := startTestServer(t, cfg)
	defer func() {
		restartStop()
		waitForServerStop(t, restartErrCh)
	}()
	restarted, restartedParser := dialClient(t, restartAddr)
	replayed := bulkText(t, roundTrip(t, restarted, restartedParser, "GET", "hits"))

	if want := strconv.Itoa(workers * perWorker); live != want || replayed != want {
		t.Fatalf("hits = %s live and %s after restart, want %s in both", live, replayed, want)
	}
}
