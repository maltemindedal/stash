package test

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/maltemindedal/stash/internal/config"
	stashlogger "github.com/maltemindedal/stash/internal/logger"
	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/rdb"
	"github.com/maltemindedal/stash/internal/storage"
)

// TestAWriteDuringAFullResyncWaitsOnlyForTheSnapshotCopy writes from a client
// while the master attaches a replica to a 100,000-key keyspace, and prints
// the client's idle p99, the time sizing and copying the snapshot takes, and the slowest
// write during the resync.
//
// PSYNC holds every stripe while it copies the snapshot, and in the default
// mode encodes and sends it after releasing them, so there a write waits for
// the copy at most: the slowest write must stay within twice the longest of
// three copies of the same keyspace. Under --event-loop PSYNC encodes the
// snapshot on the loop goroutine, which every other client of the loop waits
// for, so that mode prints its numbers without a bound.
func TestAWriteDuringAFullResyncWaitsOnlyForTheSnapshotCopy(t *testing.T) {
	const idleWrites = 2000
	const maxCopyMultiple = 2

	forEachNetworkingMode(t, defaultTestConfig(), func(t *testing.T, cfg config.Config) {
		store := storage.NewStore()
		value := bytes.Repeat([]byte("v"), 200)
		for i := 0; i < fullResyncKeys; i++ {
			if err := store.Set(nil, fmt.Sprintf("bulk-%06d", i), value, 0); err != nil {
				t.Fatalf("Set() error = %v", err)
			}
		}
		var copyTime time.Duration
		for i := 0; i < 3; i++ {
			start := time.Now()
			var sizer rdb.SnapshotSizer
			store.SnapshotStringsIf(sizer.Add, func() bool { return sizer.Size() > 0 })
			copyTime = max(copyTime, time.Since(start))
		}

		srv := newServer(t, cfg, stashlogger.New(cfg.LogLevel), store)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		errCh := make(chan error, 1)
		go func() { errCh <- srv.ListenAndServe(ctx) }()
		addr := waitForAddr(t, srv)

		client, clientParser := dialClient(t, addr)
		set := func() time.Duration {
			start := time.Now()
			assertCommandResponse(t, client, clientParser, protocol.SimpleString{Value: "OK"}, "SET", "written-during-resync", "x")
			return time.Since(start)
		}
		idle := make([]time.Duration, idleWrites)
		for i := range idle {
			idle[i] = set()
		}
		sort.Slice(idle, func(i, j int) bool { return idle[i] < idle[j] })
		idleP99 := idle[len(idle)*99/100]

		replica, replicaParser := dialClient(t, addr)
		assertCommandResponse(t, replica, replicaParser, protocol.SimpleString{Value: "OK"}, "REPLCONF", "listening-port", "6380")
		if err := replica.SetReadDeadline(time.Now().Add(30 * time.Second)); err != nil {
			t.Fatalf("SetReadDeadline() error = %v", err)
		}
		if err := protocol.WriteValue(replica, request("PSYNC", "?", "-1")); err != nil {
			t.Fatalf("WriteValue(PSYNC) error = %v", err)
		}
		resynced := make(chan error, 1)
		go func() {
			if _, err := replicaParser.Parse(); err != nil {
				resynced <- fmt.Errorf("reading FULLRESYNC: %w", err)
				return
			}
			if _, err := replicaParser.Parse(); err != nil {
				resynced <- fmt.Errorf("reading the snapshot: %w", err)
				return
			}
			resynced <- nil
		}()

		var worst time.Duration
		writes := 0
		for done := false; !done; {
			worst = max(worst, set())
			writes++
			select {
			case err := <-resynced:
				if err != nil {
					t.Fatalf("reading the full resync: %v", err)
				}
				done = true
			default:
			}
		}

		t.Logf("idle p99 %v over %d writes; snapshot copy %v; slowest of %d writes during the resync %v (%.0fx the idle p99, %.1fx the copy)",
			idleP99, idleWrites, copyTime, writes, worst, float64(worst)/float64(idleP99), float64(worst)/float64(copyTime))
		encodesOnTheLoop := cfg.EventLoop
		if !encodesOnTheLoop && worst > maxCopyMultiple*copyTime {
			t.Fatalf("the slowest write during the full resync took %v, more than %d times the snapshot copy (%v): a write waited for more than the copy", worst, maxCopyMultiple, copyTime)
		}

		cancel()
		waitForServerStop(t, errCh)
	})
}
