package aof_test

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/maltemindedal/stash/internal/aof"
	"github.com/maltemindedal/stash/internal/command"
	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/storage"
)

func TestGenerateRewriteRoundTripsState(t *testing.T) {
	store := storage.NewStore()
	_, _ = store.Set("name", []byte("Stash"), 0)
	_, _ = store.Set("expiring", []byte("soon"), time.Now().Add(time.Minute).UnixMilli())
	if _, _, err := store.RightPush("letters", [][]byte{[]byte("a"), []byte("b")}); err != nil {
		t.Fatalf("RightPush() error = %v", err)
	}
	if _, _, err := store.HSet("profile", []storage.HashFieldValue{{Field: "lang", Value: []byte("go")}, {Field: "tier", Value: []byte("senior")}}); err != nil {
		t.Fatalf("HSet() error = %v", err)
	}
	if _, _, err := store.SAdd("tags", [][]byte{[]byte("fast"), []byte("durable")}); err != nil {
		t.Fatalf("SAdd() error = %v", err)
	}
	if _, _, err := store.ZAdd("leaders", []storage.ZSetEntry{{Member: []byte("alpha"), Score: 1}, {Member: []byte("beta"), Score: 2}}); err != nil {
		t.Fatalf("ZAdd() error = %v", err)
	}
	if _, _, err := store.XAdd("events", "1-0", [][]byte{[]byte("type"), []byte("start")}); err != nil {
		t.Fatalf("XAdd() error = %v", err)
	}

	entries, _ := store.SnapshotAll()
	var payload bytes.Buffer
	stats, err := aof.GenerateRewrite(entries, &payload)
	if err != nil {
		t.Fatalf("GenerateRewrite() error = %v", err)
	}
	if stats.Keys != 7 {
		t.Fatalf("stats.Keys = %d, want 7", stats.Keys)
	}
	if stats.Commands != 7 {
		t.Fatalf("stats.Commands = %d, want 7", stats.Commands)
	}

	replayedStore := storage.NewStore()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	executor := command.NewExecutor(replayedStore, logger)
	parser := protocol.NewParser(bytes.NewReader(payload.Bytes()))
	for {
		value, parseErr := parser.Parse()
		if parseErr != nil {
			if errors.Is(parseErr, io.EOF) {
				break
			}
			t.Fatalf("Parse() error = %v", parseErr)
		}
		if _, execErr := executor.ExecuteDetailed(context.Background(), value); execErr != nil {
			t.Fatalf("ExecuteDetailed() error = %v", execErr)
		}
	}

	if got, ok, err := replayedStore.Get("name"); err != nil {
		t.Fatalf("Get(name) error = %v", err)
	} else if !ok || string(got) != "Stash" {
		t.Fatalf("Get(name) = (%q, %v), want (Stash, true)", string(got), ok)
	}
	if got, ok, err := replayedStore.Get("expiring"); err != nil {
		t.Fatalf("Get(expiring) error = %v", err)
	} else if !ok || string(got) != "soon" {
		t.Fatalf("Get(expiring) = (%q, %v), want (soon, true)", string(got), ok)
	}

	replayedEntries, _ := replayedStore.SnapshotAll()
	var foundTTL bool
	for _, entry := range replayedEntries {
		if entry.Key == "expiring" {
			foundTTL = entry.ExpiresAt > time.Now().UnixMilli()
		}
	}
	if !foundTTL {
		t.Fatal("rewritten expiring key did not retain a future TTL")
	}

	if values, err := replayedStore.ListRange("letters", 0, -1); err != nil {
		t.Fatalf("ListRange() error = %v", err)
	} else if len(values) != 2 || string(values[0]) != "a" || string(values[1]) != "b" {
		t.Fatalf("ListRange() = %q, want [a b]", values)
	}
	if fields, err := replayedStore.HGetAll("profile"); err != nil {
		t.Fatalf("HGetAll() error = %v", err)
	} else if len(fields) != 2 {
		t.Fatalf("len(HGetAll()) = %d, want 2", len(fields))
	}
	if members, err := replayedStore.SMembers("tags"); err != nil {
		t.Fatalf("SMembers() error = %v", err)
	} else if len(members) != 2 {
		t.Fatalf("len(SMembers()) = %d, want 2", len(members))
	}
	if entries, err := replayedStore.ZRange("leaders", 0, -1); err != nil {
		t.Fatalf("ZRange() error = %v", err)
	} else if len(entries) != 2 || entries[0].Member != "alpha" || entries[1].Member != "beta" {
		t.Fatalf("ZRange() = %#v, want alpha/beta order", entries)
	}
	if entries, err := replayedStore.XRead("events", "0-0"); err != nil {
		t.Fatalf("XRead() error = %v", err)
	} else if len(entries) != 1 || entries[0].ID != "1-0" {
		t.Fatalf("XRead() = %#v, want single entry 1-0", entries)
	}
}

func TestGenerateRewriteOutputLoadsBackForHugeCollections(t *testing.T) {
	// The loader rejects any command array over 1,048,576 elements, so a rewrite
	// that emits one command per key produced a file the server could not start
	// from once a collection passed about a million values (half that for hashes
	// and sorted sets). The list and hash sizes below are the smallest that used
	// to fail. Sets and sorted sets share the chunking helper, so a small
	// collection is enough to check that their commands are bounded too; only one
	// single-value and one paired kind run at full size because parsing a million
	// values is slow under the race detector.
	if testing.Short() {
		t.Skip("builds collections of about a million values")
	}

	const listSize = 1 << 20   // +key and command name exceed the array limit
	const pairSize = 1<<19 + 1 // two elements per pair
	const smallSize = 70_000   // one command would exceed maxCommandElements; far below the loader limit
	const maxCommandElements = 1 << 16
	member := func(i int) string { return strconv.Itoa(i) }

	tests := []struct {
		name  string
		entry storage.SnapshotEntry
		size  int // values the replayed commands must carry in total
	}{
		{name: "list", size: listSize, entry: func() storage.SnapshotEntry {
			e := storage.SnapshotEntry{Key: "big", Kind: storage.ValueKindList, List: make([][]byte, listSize)}
			for i := range e.List {
				e.List[i] = []byte(member(i))
			}
			return e
		}()},
		{name: "set", size: smallSize, entry: func() storage.SnapshotEntry {
			e := storage.SnapshotEntry{Key: "big", Kind: storage.ValueKindSet, Set: make([][]byte, smallSize)}
			for i := range e.Set {
				e.Set[i] = []byte(member(i))
			}
			return e
		}()},
		{name: "hash", size: pairSize, entry: func() storage.SnapshotEntry {
			e := storage.SnapshotEntry{Key: "big", Kind: storage.ValueKindHash, Hash: make([]storage.HashFieldValue, pairSize)}
			for i := range e.Hash {
				e.Hash[i] = storage.HashFieldValue{Field: member(i), Value: []byte("v")}
			}
			return e
		}()},
		{name: "sorted set", size: smallSize, entry: func() storage.SnapshotEntry {
			e := storage.SnapshotEntry{Key: "big", Kind: storage.ValueKindZSet, ZSet: make([]storage.ZSetRangeEntry, smallSize)}
			for i := range e.ZSet {
				e.ZSet[i] = storage.ZSetRangeEntry{Member: member(i), Score: float64(i)}
			}
			return e
		}()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "rewrite.aof")
			file, err := os.Create(path)
			if err != nil {
				t.Fatalf("Create() error = %v", err)
			}
			w := bufio.NewWriter(file)
			stats, err := aof.GenerateRewrite([]storage.SnapshotEntry{tt.entry}, w)
			if err == nil {
				err = w.Flush()
			}
			if closeErr := file.Close(); err == nil {
				err = closeErr
			}
			if err != nil {
				t.Fatalf("GenerateRewrite() error = %v", err)
			}
			// Replay the file the way startup does and count the values it carries.
			items := 0
			replayed, err := aof.LoadFile(context.Background(), path, func(_ context.Context, value protocol.Value) error {
				array, ok := value.(protocol.Array)
				if !ok || len(array.Elements) < 3 {
					return fmt.Errorf("replayed frame = %#v, want a command array with values", value)
				}
				perItem := 1
				if tt.name == "hash" || tt.name == "sorted set" {
					perItem = 2
				}
				if tt.name == "list" {
					// RPUSH appends, so the values must arrive in list order across commands.
					for i, element := range array.Elements[2:] {
						if got := string(element.(protocol.BulkString).Data); got != member(items+i) {
							return fmt.Errorf("list value %d = %q, want %q", items+i, got, member(items+i))
						}
					}
				}
				if len(array.Elements) > maxCommandElements {
					return fmt.Errorf("replayed command has %d elements, want at most %d", len(array.Elements), maxCommandElements)
				}
				items += (len(array.Elements) - 2) / perItem
				return nil
			})
			if err != nil {
				t.Fatalf("LoadFile() error = %v: the rewritten file is not loadable", err)
			}
			if replayed.TruncatedTail {
				t.Fatal("LoadFile() reported a truncated tail for a complete rewrite")
			}
			if items != tt.size {
				t.Fatalf("replayed %d values, want %d", items, tt.size)
			}
			if stats.Keys != 1 || stats.Commands < 2 {
				t.Fatalf("stats = %+v, want 1 key split across several commands", stats)
			}
		})
	}
}

// parseRewriteCommands decodes a rewrite's output into one slice of argument
// texts per command, in the order written.
func parseRewriteCommands(t *testing.T, payload []byte) [][]string {
	t.Helper()

	var commands [][]string
	parser := protocol.NewParser(bytes.NewReader(payload))
	for {
		value, err := parser.Parse()
		if errors.Is(err, io.EOF) {
			return commands
		}
		if err != nil {
			t.Fatalf("Parse() error = %v", err)
		}
		array, ok := value.(protocol.Array)
		if !ok {
			t.Fatalf("rewrite frame = %#v, want a command array", value)
		}
		args := make([]string, 0, len(array.Elements))
		for _, element := range array.Elements {
			bulk, ok := element.(protocol.BulkString)
			if !ok {
				t.Fatalf("rewrite argument = %#v, want a bulk string", element)
			}
			args = append(args, string(bulk.Data))
		}
		commands = append(commands, args)
	}
}

func TestRewriteKeepsTheAbsoluteDeadlineOfAStringKey(t *testing.T) {
	inAnHour := time.Now().Add(time.Hour).UnixMilli()
	tests := []struct {
		name      string
		expiresAt int64
		want      [][]string
	}{
		{
			name:      "a deadline an hour out is written as PXAT with the same deadline",
			expiresAt: inAnHour,
			want:      [][]string{{"SET", "k", "v", "PXAT", strconv.FormatInt(inAnHour, 10)}},
		},
		{
			name: "a key without a deadline is written without one",
			want: [][]string{{"SET", "k", "v"}},
		},
		{
			name:      "a deadline that has passed writes nothing",
			expiresAt: time.Now().Add(-time.Minute).UnixMilli(),
			want:      nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entry := storage.SnapshotEntry{Key: "k", Kind: storage.ValueKindString, String: []byte("v"), ExpiresAt: tt.expiresAt}
			var out bytes.Buffer
			if _, err := aof.GenerateRewrite([]storage.SnapshotEntry{entry}, &out); err != nil {
				t.Fatalf("GenerateRewrite() error = %v", err)
			}

			got := parseRewriteCommands(t, out.Bytes())
			if fmt.Sprint(got) != fmt.Sprint(tt.want) {
				t.Fatalf("rewrite = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestRewriteRefusesACollectionWithATTL(t *testing.T) {
	inAnHour := time.Now().Add(time.Hour).UnixMilli()
	tests := []struct {
		name  string
		entry storage.SnapshotEntry
	}{
		{name: "list", entry: storage.SnapshotEntry{Kind: storage.ValueKindList, List: [][]byte{[]byte("a")}}},
		{name: "hash", entry: storage.SnapshotEntry{Kind: storage.ValueKindHash, Hash: []storage.HashFieldValue{{Field: "f", Value: []byte("v")}}}},
		{name: "set", entry: storage.SnapshotEntry{Kind: storage.ValueKindSet, Set: [][]byte{[]byte("m")}}},
		{name: "sorted set", entry: storage.SnapshotEntry{Kind: storage.ValueKindZSet, ZSet: []storage.ZSetRangeEntry{{Member: "m", Score: 1}}}},
		{name: "stream", entry: storage.SnapshotEntry{Kind: storage.ValueKindStream, Stream: []storage.StreamEntry{{ID: "1-0", Values: [][]byte{[]byte("f"), []byte("v")}}}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entry := tt.entry
			entry.Key = "ttl-" + tt.name
			entry.ExpiresAt = inAnHour

			var out bytes.Buffer
			_, err := aof.GenerateRewrite([]storage.SnapshotEntry{entry}, &out)
			if err == nil {
				t.Fatalf("GenerateRewrite() error = nil, want a refusal; it wrote %q and dropped the TTL", out.String())
			}
			want := fmt.Sprintf("aof: cannot rewrite the TTL of %s key %q", entry.Kind, entry.Key)
			if err.Error() != want {
				t.Fatalf("GenerateRewrite() error = %q, want %q", err, want)
			}
		})
	}
}
