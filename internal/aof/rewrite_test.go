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
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/maltemindedal/stash/internal/aof"
	"github.com/maltemindedal/stash/internal/command"
	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/server"
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
	executor := replayExecutor(t, replayedStore)
	parser := protocol.NewParser(bytes.NewReader(payload.Bytes()))
	for {
		value, parseErr := parser.Parse()
		if parseErr != nil {
			if errors.Is(parseErr, io.EOF) {
				break
			}
			t.Fatalf("Parse() error = %v", parseErr)
		}
		// Replayed as the server replays its AOF at startup.
		if _, execErr := executor.ExecuteDetailed(server.WithReplicationOrigin(context.Background()), value); execErr != nil {
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

func TestRewriteKeepsTheAbsoluteDeadlineOfAStringKey(t *testing.T) {
	// The rewrite used to write a TTL as the time left (PX), which replay counts
	// from the loader's clock. After BGREWRITEAOF and a restart every TTL was
	// extended by the downtime, and keys that expired meanwhile came back.
	inAnHour := time.Now().Add(time.Hour).UnixMilli()
	tests := []struct {
		name      string
		expiresAt int64
		want      [][]string
	}{
		{name: "a TTL an hour out", expiresAt: inAnHour, want: [][]string{{"SET", "k", "v", "PXAT", strconv.FormatInt(inAnHour, 10)}}},
		{name: "no TTL", expiresAt: 0, want: [][]string{{"SET", "k", "v"}}},
		{name: "a passed deadline", expiresAt: time.Now().Add(-time.Second).UnixMilli(), want: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entry := storage.SnapshotEntry{Key: "k", Kind: storage.ValueKindString, ExpiresAt: tt.expiresAt, String: []byte("v")}
			var out bytes.Buffer
			stats, err := aof.GenerateRewrite([]storage.SnapshotEntry{entry}, &out)
			if err != nil {
				t.Fatalf("GenerateRewrite() error = %v", err)
			}
			if got := rewriteCommands(t, out.Bytes()); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("rewrite = %q, want %q", got, tt.want)
			}
			if stats.Keys != len(tt.want) || stats.Commands != len(tt.want) {
				t.Fatalf("stats = %+v, want %d key and command", stats, len(tt.want))
			}
		})
	}
}

func TestRewriteRefusesACollectionWithATTL(t *testing.T) {
	// No command gives a collection a TTL yet, and the rewrite has no command that
	// recreates one, so it used to write the collection without its TTL. The
	// rewrite now fails instead, and the writer keeps the old file.
	expiresAt := time.Now().Add(time.Hour).UnixMilli()
	tests := []struct {
		name  string
		entry storage.SnapshotEntry
	}{
		{name: "list", entry: storage.SnapshotEntry{Key: "letters", Kind: storage.ValueKindList, List: [][]byte{[]byte("a")}}},
		{name: "hash", entry: storage.SnapshotEntry{Key: "profile", Kind: storage.ValueKindHash, Hash: []storage.HashFieldValue{{Field: "lang", Value: []byte("go")}}}},
		{name: "set", entry: storage.SnapshotEntry{Key: "tags", Kind: storage.ValueKindSet, Set: [][]byte{[]byte("fast")}}},
		{name: "sorted set", entry: storage.SnapshotEntry{Key: "leaders", Kind: storage.ValueKindZSet, ZSet: []storage.ZSetRangeEntry{{Member: "alpha", Score: 1}}}},
		{name: "stream", entry: storage.SnapshotEntry{Key: "events", Kind: storage.ValueKindStream, Stream: []storage.StreamEntry{{ID: "1-0", Values: [][]byte{[]byte("type"), []byte("start")}}}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entry := tt.entry
			entry.ExpiresAt = expiresAt
			_, err := aof.GenerateRewrite([]storage.SnapshotEntry{entry}, io.Discard)
			if err == nil {
				t.Fatalf("GenerateRewrite() error = nil, want a refusal to rewrite the TTL of %s key %q", entry.Kind, entry.Key)
			}
			if !strings.HasPrefix(err.Error(), "aof: ") || !strings.Contains(err.Error(), strconv.Quote(entry.Key)) {
				t.Fatalf("GenerateRewrite() error = %q, want an aof: error naming key %q", err, entry.Key)
			}
		})
	}
}

// rewriteCommands parses a rewrite's output into the arguments of each command.
func rewriteCommands(t *testing.T, payload []byte) [][]string {
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

func TestSeedFileWritesAFileThatReplaysToTheSnapshot(t *testing.T) {
	// Startup seeds a missing or empty append-only file with the keys an RDB
	// snapshot loaded, and every later start replays that file instead of the
	// snapshot. So it has to replay to exactly those keys, deadlines included,
	// and leave no temp file behind.
	store := storage.NewStore()
	_, _ = store.Set("name", []byte("Stash"), 0)
	_, _ = store.Set("session", []byte("alice"), time.Now().Add(time.Hour).UnixMilli())
	if _, _, err := store.RightPush("letters", [][]byte{[]byte("a"), []byte("b")}); err != nil {
		t.Fatalf("RightPush() error = %v", err)
	}
	entries, _ := store.SnapshotAll()

	tests := []struct {
		name string
		// aofPath prepares dir and returns the path to seed.
		aofPath func(t *testing.T, dir string) string
	}{
		{name: "a missing file", aofPath: func(_ *testing.T, dir string) string {
			return filepath.Join(dir, "appendonly.aof")
		}},
		{name: "an empty file", aofPath: func(t *testing.T, dir string) string {
			path := filepath.Join(dir, "appendonly.aof")
			if err := os.WriteFile(path, nil, 0o600); err != nil {
				t.Fatalf("WriteFile() error = %v", err)
			}
			return path
		}},
		{name: "a file in a missing directory", aofPath: func(_ *testing.T, dir string) string {
			return filepath.Join(dir, "missing", "appendonly.aof")
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := tt.aofPath(t, t.TempDir())

			stats, err := aof.SeedFile(path, entries)
			if err != nil {
				t.Fatalf("SeedFile() error = %v", err)
			}
			if stats.Keys != 3 || stats.Commands != 3 {
				t.Fatalf("SeedFile() stats = %+v, want 3 keys in 3 commands", stats)
			}

			replayedStore := storage.NewStore()
			executor := replayExecutor(t, replayedStore)
			loaded, err := aof.LoadFile(server.WithReplicationOrigin(context.Background()), path, func(ctx context.Context, value protocol.Value) error {
				_, execErr := executor.ExecuteDetailed(ctx, value)
				return execErr
			})
			if err != nil {
				t.Fatalf("LoadFile() error = %v", err)
			}
			if loaded.TornTail || loaded.ReplayedCommands != stats.Commands {
				t.Fatalf("LoadFile() stats = %+v, want %d complete commands", loaded, stats.Commands)
			}
			replayed, _ := replayedStore.SnapshotAll()
			if got, want := sortedSnapshot(replayed), sortedSnapshot(entries); !reflect.DeepEqual(got, want) {
				t.Fatalf("replayed keyspace = %+v, want the snapshot %+v", got, want)
			}

			files, err := os.ReadDir(filepath.Dir(path))
			if err != nil {
				t.Fatalf("ReadDir() error = %v", err)
			}
			if len(files) != 1 || files[0].Name() != filepath.Base(path) {
				t.Fatalf("directory holds %v, want only %s", files, filepath.Base(path))
			}
		})
	}
}

func TestSeedFileWritesThroughASymbolicLinkToTheFileItLeadsTo(t *testing.T) {
	// OpenWriter appends through a symbolic link to the file it leads to. A seed
	// renamed over the link itself would replace the link with a regular file and
	// leave that file empty, moving the AOF off the volume the link chose.
	entries := []storage.SnapshotEntry{{Key: "k", Kind: storage.ValueKindString, String: []byte("v")}}
	var want bytes.Buffer
	if _, err := aof.GenerateRewrite(entries, &want); err != nil {
		t.Fatalf("GenerateRewrite() error = %v", err)
	}

	tests := []struct {
		name         string
		createTarget bool
	}{
		{name: "to an empty file", createTarget: true},
		{name: "to a file that does not exist yet"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			dataDir := filepath.Join(dir, "data")
			if err := os.Mkdir(dataDir, 0o750); err != nil {
				t.Fatalf("Mkdir() error = %v", err)
			}
			target := filepath.Join(dataDir, "appendonly.aof")
			if tt.createTarget {
				if err := os.WriteFile(target, nil, 0o600); err != nil {
					t.Fatalf("WriteFile() error = %v", err)
				}
			}
			// A relative link resolves from the link's own directory.
			link := filepath.Join(dir, "appendonly.aof")
			linkTo := filepath.Join("data", "appendonly.aof")
			if err := os.Symlink(linkTo, link); err != nil {
				t.Skipf("Symlink() error = %v", err)
			}

			if _, err := aof.SeedFile(link, entries); err != nil {
				t.Fatalf("SeedFile() error = %v", err)
			}
			if got, err := os.Readlink(link); err != nil || got != linkTo {
				t.Fatalf("Readlink() = (%q, %v), want the link to %q left in place", got, err, linkTo)
			}
			if got, err := os.ReadFile(target); err != nil || !bytes.Equal(got, want.Bytes()) {
				t.Fatalf("file the link leads to = (%q, %v), want the seed %q", got, err, want.Bytes())
			}
			for _, d := range []struct {
				dir  string
				want []string
			}{{dir, []string{"appendonly.aof", "data"}}, {dataDir, []string{"appendonly.aof"}}} {
				files, err := os.ReadDir(d.dir)
				if err != nil {
					t.Fatalf("ReadDir(%q) error = %v", d.dir, err)
				}
				names := make([]string, 0, len(files))
				for _, file := range files {
					names = append(names, file.Name())
				}
				if !reflect.DeepEqual(names, d.want) {
					t.Fatalf("%s holds %q, want %q and no temp file", d.dir, names, d.want)
				}
			}
		})
	}
}

func TestSeedFileRefusesToReplaceAnythingButAnEmptyFile(t *testing.T) {
	// The rename that ends seeding replaces whatever the path names. Over
	// commands it would lose them, and over a device such as /dev/full, which
	// reports a size of zero, it would put a regular file in the device's place.
	entries := []storage.SnapshotEntry{{Key: "k", Kind: storage.ValueKindString, String: []byte("v")}}
	commands := []byte("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$3\r\nold\r\n")

	tests := []struct {
		name    string
		prepare func(t *testing.T, path string)
		// untouched fails the test unless path is as prepare left it.
		untouched func(t *testing.T, path string)
		reason    string
	}{
		{
			name: "a file that holds commands",
			prepare: func(t *testing.T, path string) {
				if err := os.WriteFile(path, commands, 0o600); err != nil {
					t.Fatalf("WriteFile() error = %v", err)
				}
			},
			untouched: func(t *testing.T, path string) {
				if got, err := os.ReadFile(path); err != nil || !bytes.Equal(got, commands) {
					t.Fatalf("file after the refusal = (%q, %v), want it untouched", got, err)
				}
			},
			reason: "already holds",
		},
		{
			name: "a directory",
			prepare: func(t *testing.T, path string) {
				if err := os.Mkdir(path, 0o750); err != nil {
					t.Fatalf("Mkdir() error = %v", err)
				}
			},
			untouched: func(t *testing.T, path string) {
				if info, err := os.Stat(path); err != nil || !info.IsDir() {
					t.Fatalf("Stat() after the refusal = (%v, %v), want the directory untouched", info, err)
				}
			},
			reason: "not a regular file",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "appendonly.aof")
			tt.prepare(t, path)

			_, err := aof.SeedFile(path, entries)
			if err == nil {
				t.Fatal("SeedFile() error = nil, want a refusal")
			}
			if msg := err.Error(); !strings.HasPrefix(msg, "aof: ") || !strings.Contains(msg, strconv.Quote(path)) || !strings.Contains(msg, tt.reason) {
				t.Fatalf("SeedFile() error = %q, want an aof: error naming %q and saying %q", msg, path, tt.reason)
			}
			tt.untouched(t, path)
			if files, err := os.ReadDir(dir); err != nil || len(files) != 1 {
				t.Fatalf("directory after the refusal = (%v, %v), want only %s", files, err, filepath.Base(path))
			}
		})
	}
}

// replayExecutor is a command executor over store, as the server builds one,
// with stand-ins for the server's other collaborators: nothing records writes,
// as nothing does while a server replays its AOF at startup.
func replayExecutor(t *testing.T, store *storage.Store) *command.Executor {
	t.Helper()

	executor, err := command.New(server.Services{
		Store:            store,
		Logger:           slog.New(slog.NewTextHandler(io.Discard, nil)),
		Watches:          server.NewWatchRegistry(),
		PubSub:           server.NewPubSubRegistry(),
		Slowlog:          server.NewSlowlogRegistry(),
		SlowlogThreshold: -1,
		Replication:      &server.ReplicationState{},
		Replicas:         server.NewReplicaRegistry(),
		Stats:            func() server.Stats { return server.Stats{Role: "master", AOFLastWriteOK: true} },
		RewriteAOF:       func(context.Context) error { return errors.New("append only file persistence is not enabled") },
		RecordsWrites:    func() bool { return false },
	})
	if err != nil {
		t.Fatalf("command.New() error = %v", err)
	}
	return executor
}

// sortedSnapshot orders a snapshot by key, which SnapshotAll does not.
func sortedSnapshot(entries []storage.SnapshotEntry) []storage.SnapshotEntry {
	sorted := append([]storage.SnapshotEntry(nil), entries...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Key < sorted[j].Key })
	return sorted
}
