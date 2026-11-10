package aof_test

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/maltemindedal/stash/internal/aof"
	"github.com/maltemindedal/stash/internal/command"
	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/record"
	"github.com/maltemindedal/stash/internal/replication"
	"github.com/maltemindedal/stash/internal/server"
	"github.com/maltemindedal/stash/internal/storage"
)

// rewriteOf returns the rewrite of the store's keyspace image.
func rewriteOf(t *testing.T, store *storage.Store) ([]byte, aof.RewriteStats) {
	t.Helper()

	var payload bytes.Buffer
	stats, err := aof.GenerateRewrite(store.Image(nil), &payload)
	if err != nil {
		t.Fatalf("GenerateRewrite() error = %v", err)
	}
	return payload.Bytes(), stats
}

// replayInto replays the append-only file at path into a new store, through
// the command executor as startup does, and returns the store.
func replayInto(t *testing.T, path string) (*storage.Store, aof.LoadStats) {
	t.Helper()

	store := storage.NewStore()
	executor := replayExecutor(t, store)
	loaded, err := aof.LoadFile(context.Background(), path, func(ctx context.Context, value protocol.Value) error {
		_, execErr := executor.Handle(ctx, server.Call{Origin: record.OriginReplay}, value)
		return execErr
	})
	if err != nil {
		t.Fatalf("LoadFile() error = %v", err)
	}
	return store, loaded
}

// TestRewrittenAppendOnlyFileReplaysToTheSameKeyspace pins storage's recreation
// commands to the command grammar: a rewrite, replayed through the loader and
// the real executor into an empty store, must rewrite to the same bytes. It
// holds every value kind in both of its encodings, a collection that takes
// several commands, and a TTL.
func TestRewrittenAppendOnlyFileReplaysToTheSameKeyspace(t *testing.T) {
	long := strings.Repeat("l", 100) // too long for a compact hash or sorted set
	bulk := func(items ...string) [][]byte {
		out := make([][]byte, len(items))
		for i, item := range items {
			out[i] = []byte(item)
		}
		return out
	}
	many := make([]string, 2500)
	for i := range many {
		many[i] = strconv.Itoa(i)
	}
	manyPairs := make([]storage.HashFieldValue, len(many))
	manyScores := make([]storage.ZSetEntry, len(many))
	for i, item := range many {
		manyPairs[i] = storage.HashFieldValue{Field: item, Value: []byte("v" + item)}
		manyScores[i] = storage.ZSetEntry{Member: []byte(item), Score: float64(i%7) + 0.25}
	}

	store := storage.NewStore()
	deadline := time.Now().Add(time.Hour).UnixMilli()
	writes := []func() error{
		func() error { err := store.Set(nil, "name", []byte("Stash"), 0); return err },
		func() error { err := store.Set(nil, "empty", nil, 0); return err },
		func() error { err := store.Set(nil, "binary", []byte("a\r\nb\x00c"), 0); return err },
		func() error { err := store.Set(nil, "expiring", []byte("soon"), deadline); return err },
		func() error { _, err := store.RightPush(nil, "letters", bulk("a", "b", "a")); return err },
		func() error { _, err := store.RightPush(nil, "many-values", bulk(many...)); return err },
		func() error {
			_, err := store.HSet(nil, "profile", []storage.HashFieldValue{{Field: "lang", Value: []byte("go")}, {Field: "tier", Value: []byte("senior")}})
			return err
		},
		func() error {
			_, err := store.HSet(nil, "wide-profile", []storage.HashFieldValue{{Field: "bio", Value: []byte(long)}, {Field: "", Value: nil}})
			return err
		},
		func() error { _, err := store.HSet(nil, "many-fields", manyPairs); return err },
		func() error { _, err := store.SAdd(nil, "numbers", bulk("3", "-1", "20")); return err },
		func() error { _, err := store.SAdd(nil, "tags", bulk("fast", "durable", "7")); return err },
		func() error { _, err := store.SAdd(nil, "many-members", bulk(many...)); return err },
		func() error {
			_, err := store.ZAdd(nil, "leaders", []storage.ZSetEntry{{Member: []byte("beta"), Score: 2}, {Member: []byte("alpha"), Score: 1}, {Member: []byte("aardvark"), Score: 2}, {Member: []byte("low"), Score: -math.MaxFloat64}, {Member: []byte("tiny"), Score: 1e-7}})
			return err
		},
		func() error {
			_, err := store.ZAdd(nil, "wide-leaders", []storage.ZSetEntry{{Member: []byte(long), Score: 1.5}, {Member: []byte("m"), Score: -3}})
			return err
		},
		func() error { _, err := store.ZAdd(nil, "many-scores", manyScores); return err },
		func() error { _, err := store.XAdd(nil, "events", "1-0", bulk("type", "start")); return err },
		func() error { _, err := store.XAdd(nil, "events", "1-1", bulk("type", "stop", "code", "")); return err },
	}
	for i, write := range writes {
		if err := write(); err != nil {
			t.Fatalf("write %d error = %v", i, err)
		}
	}

	first, stats := rewriteOf(t, store)
	// 16 keys: 11 take one command, the stream two, and each collection of 2,500
	// items three.
	if stats.Keys != 16 || stats.Commands != 11+2+4*3 {
		t.Fatalf("stats = %+v, want 16 keys in 25 commands", stats)
	}
	if again, _ := rewriteOf(t, store); !bytes.Equal(again, first) {
		t.Fatal("a second rewrite of the same store differs from the first")
	}

	path := filepath.Join(t.TempDir(), "rewrite.aof")
	if err := os.WriteFile(path, first, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	replayed, loaded := replayInto(t, path)
	if loaded.TruncatedTail || loaded.ReplayedCommands != stats.Commands {
		t.Fatalf("LoadFile() stats = %+v, want %d complete commands", loaded, stats.Commands)
	}
	second, _ := rewriteOf(t, replayed)
	if !bytes.Equal(second, first) {
		firstCommands, secondCommands := rewriteCommands(t, first), rewriteCommands(t, second)
		for i := 0; i < len(firstCommands) && i < len(secondCommands); i++ {
			if !reflect.DeepEqual(firstCommands[i], secondCommands[i]) {
				t.Fatalf("command %d of the replayed keyspace's rewrite = %.200q, want %.200q", i, secondCommands[i], firstCommands[i])
			}
		}
		t.Fatalf("the replayed keyspace rewrites to %d commands, want %d", len(secondCommands), len(firstCommands))
	}

	if values, err := replayed.ListRange("letters", 0, -1); err != nil || len(values) != 3 || string(values[0]) != "a" || string(values[1]) != "b" || string(values[2]) != "a" {
		t.Fatalf("ListRange(letters) = (%q, %v), want [a b a]", values, err)
	}
	if entries, err := replayed.XRead("events", "0-0"); err != nil || len(entries) != 2 || entries[0].ID != "1-0" || entries[1].ID != "1-1" {
		t.Fatalf("XRead(events) = (%#v, %v), want entries 1-0 and 1-1", entries, err)
	}
}

func TestGenerateRewriteOutputLoadsBackForAHugeCollection(t *testing.T) {
	// The loader rejects any command array over 1,048,576 elements, so a rewrite
	// that emits one command per key produced a file the server could not start
	// from once a list passed about a million values. This is the smallest list
	// that used to fail. That every kind bounds its commands is checked, on small
	// collections, by storage's TestEveryValueKindKeepsItsContract.
	if testing.Short() {
		t.Skip("builds a list of about a million values")
	}
	const listSize = 1 << 20 // +key and command name exceed the array limit
	const maxCommandElements = 1 << 16
	member := func(i int) string { return strconv.Itoa(i) }

	store := storage.NewStore()
	values := make([][]byte, listSize)
	for i := range values {
		values[i] = []byte(member(i))
	}
	if _, err := store.RightPush(nil, "big", values); err != nil {
		t.Fatalf("RightPush() error = %v", err)
	}

	path := filepath.Join(t.TempDir(), "rewrite.aof")
	file, err := os.Create(path)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	w := bufio.NewWriter(file)
	stats, err := aof.GenerateRewrite(store.Image(nil), w)
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
		// RPUSH appends, so the values must arrive in list order across commands.
		for i, element := range array.Elements[2:] {
			if got := string(element.(protocol.BulkString).Data); got != member(items+i) {
				return fmt.Errorf("list value %d = %q, want %q", items+i, got, member(items+i))
			}
		}
		if len(array.Elements) > maxCommandElements {
			return fmt.Errorf("replayed command has %d elements, want at most %d", len(array.Elements), maxCommandElements)
		}
		items += len(array.Elements) - 2
		return nil
	})
	if err != nil {
		t.Fatalf("LoadFile() error = %v: the rewritten file is not loadable", err)
	}
	if replayed.TruncatedTail {
		t.Fatal("LoadFile() reported a truncated tail for a complete rewrite")
	}
	if items != listSize {
		t.Fatalf("replayed %d values, want %d", items, listSize)
	}
	if stats.Keys != 1 || stats.Commands < 2 {
		t.Fatalf("stats = %+v, want 1 key split across several commands", stats)
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
			store := storage.NewStore()
			if err := store.Set(nil, "k", []byte("v"), tt.expiresAt); err != nil {
				t.Fatalf("Set() error = %v", err)
			}
			out, stats := rewriteOf(t, store)
			if got := rewriteCommands(t, out); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("rewrite = %q, want %q", got, tt.want)
			}
			if stats.Keys != len(tt.want) || stats.Commands != len(tt.want) {
				t.Fatalf("stats = %+v, want %d key and command", stats, len(tt.want))
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
	_ = store.Set(nil, "name", []byte("Stash"), 0)
	_ = store.Set(nil, "session", []byte("alice"), time.Now().Add(time.Hour).UnixMilli())
	if _, err := store.RightPush(nil, "letters", [][]byte{[]byte("a"), []byte("b")}); err != nil {
		t.Fatalf("RightPush() error = %v", err)
	}
	want, _ := rewriteOf(t, store)

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

			stats, err := aof.SeedFile(path, store.Image(nil))
			if err != nil {
				t.Fatalf("SeedFile() error = %v", err)
			}
			if stats.Keys != 3 || stats.Commands != 3 {
				t.Fatalf("SeedFile() stats = %+v, want 3 keys in 3 commands", stats)
			}

			replayed, loaded := replayInto(t, path)
			if loaded.TornTail || loaded.ReplayedCommands != stats.Commands {
				t.Fatalf("LoadFile() stats = %+v, want %d complete commands", loaded, stats.Commands)
			}
			if got, _ := rewriteOf(t, replayed); !bytes.Equal(got, want) {
				t.Fatalf("the replayed keyspace rewrites to %q, want the seeded keyspace's %q", got, want)
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
	store := storage.NewStore()
	if err := store.Set(nil, "k", []byte("v"), 0); err != nil {
		t.Fatalf("Set() error = %v", err)
	}
	want, _ := rewriteOf(t, store)

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

			if _, err := aof.SeedFile(link, store.Image(nil)); err != nil {
				t.Fatalf("SeedFile() error = %v", err)
			}
			if got, err := os.Readlink(link); err != nil || got != linkTo {
				t.Fatalf("Readlink() = (%q, %v), want the link to %q left in place", got, err, linkTo)
			}
			if got, err := os.ReadFile(target); err != nil || !bytes.Equal(got, want) {
				t.Fatalf("file the link leads to = (%q, %v), want the seed %q", got, err, want)
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
	store := storage.NewStore()
	if err := store.Set(nil, "k", []byte("v"), 0); err != nil {
		t.Fatalf("Set() error = %v", err)
	}
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

			_, err := aof.SeedFile(path, store.Image(nil))
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

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	master := replication.NewMaster(store, nil, logger)
	executor, err := command.New(server.Services{
		Store:            store,
		Logger:           logger,
		Watches:          server.NewWatchRegistry(),
		PubSub:           server.NewPubSubRegistry(),
		Slowlog:          server.NewSlowlogRegistry(),
		SlowlogThreshold: -1,
		Master:           master,
		Stats:            func() server.Stats { return server.Stats{Role: "master", AOFLastWriteOK: true} },
		RewriteAOF:       func(context.Context) error { return errors.New("append only file persistence is not enabled") },
		Recorder:         record.New(master, logger),
	})
	if err != nil {
		t.Fatalf("command.New() error = %v", err)
	}
	return executor
}
