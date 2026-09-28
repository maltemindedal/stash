package aof_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/maltemindedal/stash/internal/aof"
	"github.com/maltemindedal/stash/internal/command"
	"github.com/maltemindedal/stash/internal/protocol"
)

func TestLoadFile(t *testing.T) {
	t.Run("replays valid RESP commands", func(t *testing.T) {
		path := writeTempAOF(t, mustEncodeValues(t,
			request("SET", "name", "Stash"),
			request("INCR", "counter"),
		))

		var replayed []string
		stats, err := aof.LoadFile(context.Background(), path, func(_ context.Context, value protocol.Value) error {
			req, decodeErr := command.DecodeRequest(value)
			if decodeErr != nil {
				return decodeErr
			}
			replayed = append(replayed, req.Name)
			return nil
		})
		if err != nil {
			t.Fatalf("LoadFile() error = %v", err)
		}
		if stats.ReplayedCommands != 2 {
			t.Fatalf("stats.ReplayedCommands = %d, want 2", stats.ReplayedCommands)
		}
		if stats.TruncatedTail {
			t.Fatal("stats.TruncatedTail = true, want false")
		}
		if len(replayed) != 2 || replayed[0] != "SET" || replayed[1] != "INCR" {
			t.Fatalf("replayed = %v, want [SET INCR]", replayed)
		}
	})

	t.Run("treats truncated tail as recoverable", func(t *testing.T) {
		full := mustEncodeValues(t,
			request("SET", "name", "Stash"),
			request("INCR", "counter"),
		)
		path := writeTempAOF(t, full[:len(full)-3])

		stats, err := aof.LoadFile(context.Background(), path, func(_ context.Context, value protocol.Value) error {
			_, decodeErr := command.DecodeRequest(value)
			return decodeErr
		})
		if err != nil {
			t.Fatalf("LoadFile() error = %v, want truncated tail recovery", err)
		}
		if stats.ReplayedCommands != 1 {
			t.Fatalf("stats.ReplayedCommands = %d, want 1", stats.ReplayedCommands)
		}
		if !stats.TruncatedTail {
			t.Fatal("stats.TruncatedTail = false, want true")
		}
	})

	t.Run("fails on corrupt middle payload", func(t *testing.T) {
		path := writeTempAOF(t, append(mustEncodeValues(t, request("PING")), []byte("not-resp")...))

		_, err := aof.LoadFile(context.Background(), path, func(_ context.Context, value protocol.Value) error {
			_, decodeErr := command.DecodeRequest(value)
			return decodeErr
		})
		if err == nil {
			t.Fatal("LoadFile() error = nil, want parse failure")
		}
	})

	t.Run("honors canceled context", func(t *testing.T) {
		path := writeTempAOF(t, mustEncodeValues(t, request("SET", "name", "Stash")))
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := aof.LoadFile(ctx, path, func(_ context.Context, value protocol.Value) error {
			_, decodeErr := command.DecodeRequest(value)
			return decodeErr
		})
		if err == nil {
			t.Fatal("LoadFile() error = nil, want canceled context failure")
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("LoadFile() error = %v, want context.Canceled", err)
		}
	})
}

func TestLoadFileReportsWhereCompleteCommandsEnd(t *testing.T) {
	// Startup truncates a torn tail at ValidBytes before it appends anything, so
	// ValidBytes must be exactly the end of the last complete command, and TornTail
	// must only be set when the bytes after it are provably one unfinished command
	// that ran into the end of the file.
	first := mustEncodeValues(t, request("SET", "name", "Stash"))
	second := mustEncodeValues(t, request("INCR", "counter"))

	tests := []struct {
		name         string
		tail         []byte
		wantTornTail bool
		wantTrunc    bool // the pre-existing TruncatedTail flag
	}{
		{name: "complete file", tail: second},
		{name: "cut inside a bulk payload", tail: second[:len(second)-3], wantTornTail: true, wantTrunc: true},
		{name: "cut between array elements", tail: []byte("*2\r\n$4\r\nINCR\r\n"), wantTornTail: true, wantTrunc: true},
		{name: "cut inside a header line", tail: []byte("*2\r"), wantTornTail: true, wantTrunc: true},
		{name: "cut between CR and LF of a payload terminator", tail: []byte("*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r"), wantTornTail: true, wantTrunc: true},
		// Bad bytes with more data behind them are corruption (or a tail an older
		// version already glued new commands onto), not a torn write: the file is not
		// cut, so nothing after them is destroyed.
		{name: "bad terminator followed by more commands", tail: append([]byte("*2\r\n$4\r\nINCR\r\n$3\r\nabXY"), second...), wantTrunc: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := writeTempAOF(t, append(bytes.Clone(first), tt.tail...))

			stats, err := aof.LoadFile(context.Background(), path, func(context.Context, protocol.Value) error { return nil })
			if err != nil {
				t.Fatalf("LoadFile() error = %v", err)
			}

			wantValid := int64(len(first))
			if tt.name == "complete file" {
				wantValid += int64(len(second))
			}
			if stats.ValidBytes != wantValid {
				t.Fatalf("stats.ValidBytes = %d, want %d", stats.ValidBytes, wantValid)
			}
			if stats.TornTail != tt.wantTornTail {
				t.Fatalf("stats.TornTail = %v, want %v", stats.TornTail, tt.wantTornTail)
			}
			if stats.TruncatedTail != tt.wantTrunc {
				t.Fatalf("stats.TruncatedTail = %v, want %v", stats.TruncatedTail, tt.wantTrunc)
			}
		})
	}
}

func TestLoadFileTornTailAtEveryCutPoint(t *testing.T) {
	// Cut a file of mixed commands at every byte. ValidBytes must always be the
	// last command boundary at or before the cut, and the tail may be reported as
	// torn (and so discarded) exactly when the cut is not on a boundary.
	frames := [][]byte{
		mustEncodeValues(t, request("SET", "name", "Stash")),
		mustEncodeValues(t, request("RPUSH", "letters", "a", "b", "c")),
		mustEncodeValues(t, request("SET", "empty", "")),
		mustEncodeValues(t, request("INCR", "counter")),
	}
	var boundaries []int
	var full []byte
	for _, frame := range frames {
		full = append(full, frame...)
		boundaries = append(boundaries, len(full))
	}

	for cut := 0; cut <= len(full); cut++ {
		wantValid := 0
		for _, boundary := range boundaries {
			if boundary <= cut {
				wantValid = boundary
			}
		}

		path := writeTempAOF(t, full[:cut])
		stats, err := aof.LoadFile(context.Background(), path, func(context.Context, protocol.Value) error { return nil })
		if err != nil {
			t.Fatalf("cut %d: LoadFile() error = %v", cut, err)
		}
		if stats.ValidBytes != int64(wantValid) {
			t.Fatalf("cut %d: ValidBytes = %d, want %d", cut, stats.ValidBytes, wantValid)
		}
		if wantTorn := cut != wantValid; stats.TornTail != wantTorn {
			t.Fatalf("cut %d: TornTail = %v, want %v", cut, stats.TornTail, wantTorn)
		}
	}
}

func TestTruncateTailCutsTheFileAtTheGivenSize(t *testing.T) {
	path := writeTempAOF(t, []byte("0123456789"))

	if err := aof.TruncateTail(path, 4); err != nil {
		t.Fatalf("TruncateTail() error = %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if string(got) != "0123" {
		t.Fatalf("file after TruncateTail(4) = %q, want %q", got, "0123")
	}
}

func writeTempAOF(t *testing.T, payload []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", path, err)
	}
	return path
}

func mustEncodeValues(t *testing.T, values ...protocol.Value) []byte {
	t.Helper()
	payload, err := protocol.EncodeValues(values)
	if err != nil {
		t.Fatalf("EncodeValues() error = %v", err)
	}
	return payload
}

func request(parts ...string) protocol.Value {
	elements := make([]protocol.Value, 0, len(parts))
	for _, part := range parts {
		elements = append(elements, protocol.BulkString{Data: []byte(part)})
	}
	return protocol.Array{Elements: elements}
}
