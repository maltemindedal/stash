package test

import (
	"bytes"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/maltemindedal/stash/internal/protocol"
)

// TestAnIncrUnderMaxmemoryIsNotLoggedAfterADelOfTheKeyItFoundLive aims INCR at
// the last moments of the millisecond in which its key's TTL deadline falls. The
// key is live when the write reads its clock and expired a few microseconds
// later. The write must judge the key, and the sweep that makes room for it,
// against that one reading. With a second reading the sweep removed the key the
// write had found live and logged DEL for it ahead of the INCR, so the append-only
// file and the Replicas ended with k=1 and no TTL while the server had the key
// gone.
//
// The test asserts no timing. It fails only when the append-only file shows an
// INCR that replied 10 behind a DEL of its key, so a round that misses the window
// proves nothing and costs nothing.
func TestAnIncrUnderMaxmemoryIsNotLoggedAfterADelOfTheKeyItFoundLive(t *testing.T) {
	const rounds = 400

	aofPath := filepath.Join(t.TempDir(), "appendonly.aof")
	cfg := defaultTestConfig()
	cfg.AOFPath = aofPath
	cfg.AppendFsync = "everysec"
	cfg.MaxMemory = 1 << 30
	// The active loop would sweep expired keys in the middle of a round.
	cfg.EvictionInterval = time.Hour

	addr, stop, errCh := startTestServer(t, cfg)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) error = %v", addr, err)
	}
	parser := protocol.NewParser(conn)

	foundLive := make(map[string]bool)
	for i := 0; i < rounds; i++ {
		key := "k" + strconv.Itoa(i)
		deadline := time.Now().UnixMilli() + 2
		assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "OK"},
			"SET", key, "9", "PXAT", strconv.FormatInt(deadline, 10))

		// Send the INCR 5 to 100 microseconds before the deadline's millisecond
		// ends, so the server reads its clock inside that millisecond at the start
		// of the write and, if it reads again, outside it.
		offset := time.Duration(5+(i*37)%96) * time.Microsecond
		aim := time.UnixMilli(deadline + 1).Add(-offset)
		// A sleep is too coarse for a window this narrow, so spin.
		for time.Now().Before(aim) {
		}

		if err := protocol.WriteValue(conn, request("INCR", key)); err != nil {
			t.Fatalf("WriteValue(INCR %s) error = %v", key, err)
		}
		reply, err := parser.Parse()
		if err != nil {
			t.Fatalf("Parse(INCR %s reply) error = %v", key, err)
		}
		number, ok := reply.(protocol.Integer)
		if !ok {
			t.Fatalf("INCR %s reply = %#v, want an integer", key, reply)
		}
		// 10 means the write found the key live; 1 means it read its clock after
		// the deadline and started a new counter.
		foundLive[key] = number.Value == 10
	}

	closeTestResource(t, conn)
	stop()
	waitForServerStop(t, errCh)

	recorded, err := os.ReadFile(aofPath)
	if err != nil {
		t.Fatalf("ReadFile(%q) error = %v", aofPath, err)
	}
	live := 0
	for _, found := range foundLive {
		if found {
			live++
		}
	}
	t.Logf("%d of %d INCRs found their key live", live, rounds)

	deleted := make(map[string]bool)
	var misordered []string
	fileParser := protocol.NewParser(bytes.NewReader(recorded))
	for {
		frame, err := fileParser.Parse()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Parse(append-only file) error = %v", err)
		}
		args := aofFrameArguments(t, frame)
		switch strings.ToUpper(args[0]) {
		case "DEL":
			for _, key := range args[1:] {
				deleted[key] = true
			}
		case "INCR":
			if key := args[1]; foundLive[key] && deleted[key] {
				misordered = append(misordered, key)
			}
		}
	}
	if len(misordered) > 0 {
		t.Errorf("the append-only file logs a DEL of the key before its INCR for %d INCRs that replied 10, although each found its key live; first: %s",
			len(misordered), misordered[0])
	}
}

// aofFrameArguments returns the words of one command frame read from an
// append-only file.
func aofFrameArguments(t *testing.T, frame protocol.Value) []string {
	t.Helper()

	array, ok := frame.(protocol.Array)
	if !ok || len(array.Elements) == 0 {
		t.Fatalf("append-only file frame = %#v, want a non-empty array", frame)
	}
	args := make([]string, 0, len(array.Elements))
	for _, element := range array.Elements {
		word, _, ok := integrationBulkStringContent(element)
		if !ok {
			t.Fatalf("append-only file frame element = %#v, want a bulk string", element)
		}
		args = append(args, word)
	}
	return args
}
