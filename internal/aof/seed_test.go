package aof_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/maltemindedal/stash/internal/aof"
	"github.com/maltemindedal/stash/internal/command"
	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/storage"
)

// directoryNames lists what a directory holds, so a test can tell that Seed left
// nothing but the file it was asked for.
func directoryNames(t *testing.T, dir string) []string {
	t.Helper()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir(%q) error = %v", dir, err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	sort.Strings(names)
	return names
}

func TestSeedWritesTheSnapshotAsAFileThatReplaysToTheSameKeyspace(t *testing.T) {
	source := storage.NewStore()
	if _, err := source.Set("plain", []byte("value"), 0); err != nil {
		t.Fatalf("Set(plain) error = %v", err)
	}
	if _, err := source.Set("binary", []byte("a\r\nb\x00c"), 0); err != nil {
		t.Fatalf("Set(binary) error = %v", err)
	}
	deadline := time.Now().Add(time.Hour).UnixMilli()
	if _, err := source.Set("expiring", []byte("soon"), deadline); err != nil {
		t.Fatalf("Set(expiring) error = %v", err)
	}
	entries, _ := source.SnapshotAll()

	tests := []struct {
		name  string
		setup func(t *testing.T, path string)
	}{
		{name: "the file does not exist and nor does its directory", setup: func(*testing.T, string) {}},
		{name: "the file is empty", setup: func(t *testing.T, path string) {
			if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
				t.Fatalf("MkdirAll() error = %v", err)
			}
			if err := os.WriteFile(path, nil, 0o600); err != nil {
				t.Fatalf("WriteFile() error = %v", err)
			}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "data")
			path := filepath.Join(dir, "appendonly.aof")
			tt.setup(t, path)

			stats, err := aof.Seed(path, entries)
			if err != nil {
				t.Fatalf("Seed() error = %v", err)
			}
			if stats.Keys != 3 || stats.Commands != 3 {
				t.Fatalf("Seed() stats = %+v, want 3 keys in 3 commands", stats)
			}
			if got := directoryNames(t, dir); len(got) != 1 || got[0] != "appendonly.aof" {
				t.Fatalf("directory holds %q after Seed(), want only appendonly.aof", got)
			}
			if runtime.GOOS != "windows" {
				info, err := os.Stat(path)
				if err != nil {
					t.Fatalf("Stat() error = %v", err)
				}
				if info.Mode().Perm() != 0o600 {
					t.Fatalf("file mode = %v, want 0600 like the writer's", info.Mode().Perm())
				}
			}

			// Replay it the way startup does and compare what comes out.
			replayed := storage.NewStore()
			executor := command.NewExecutor(replayed, slog.New(slog.NewTextHandler(io.Discard, nil)))
			loaded, err := aof.LoadFile(context.Background(), path, func(ctx context.Context, value protocol.Value) error {
				_, execErr := executor.ExecuteDetailed(ctx, value)
				return execErr
			})
			if err != nil {
				t.Fatalf("LoadFile() error = %v", err)
			}
			if loaded.ReplayedCommands != 3 || loaded.TornTail {
				t.Fatalf("LoadFile() stats = %+v, want 3 complete commands", loaded)
			}
			got, _ := replayed.SnapshotAll()
			if len(got) != len(entries) {
				t.Fatalf("replayed %d keys, want %d", len(got), len(entries))
			}
			sort.Slice(got, func(i, j int) bool { return got[i].Key < got[j].Key })
			want := append([]storage.SnapshotEntry(nil), entries...)
			sort.Slice(want, func(i, j int) bool { return want[i].Key < want[j].Key })
			for i := range want {
				if got[i].Key != want[i].Key || string(got[i].String) != string(want[i].String) || got[i].ExpiresAt != want[i].ExpiresAt {
					t.Fatalf("replayed key %d = %+v, want %+v", i, got[i], want[i])
				}
			}
		})
	}
}

func TestSeedRefusesAFileThatAlreadyHoldsData(t *testing.T) {
	// Seeding replaces the file, so it may only ever run on one with nothing in it.
	dir := t.TempDir()
	path := filepath.Join(dir, "appendonly.aof")
	existing := []byte("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n")
	if err := os.WriteFile(path, existing, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	_, err := aof.Seed(path, []storage.SnapshotEntry{{Key: "a", Kind: storage.ValueKindString, String: []byte("1")}})
	if err == nil {
		t.Fatal("Seed() error = nil, want a refusal to replace a file that holds commands")
	}
	if !strings.HasPrefix(err.Error(), "aof: ") || !strings.Contains(err.Error(), fmt.Sprintf("%q", path)) {
		t.Fatalf("Seed() error = %q, want an aof: error naming %q", err, path)
	}
	if got, readErr := os.ReadFile(path); readErr != nil || string(got) != string(existing) {
		t.Fatalf("file = (%q, %v), want it untouched", got, readErr)
	}
	if got := directoryNames(t, dir); len(got) != 1 {
		t.Fatalf("directory holds %q after the refusal, want only the original file", got)
	}
}

func TestSeedRefusesAPathThatIsADirectory(t *testing.T) {
	// A path that is not a regular file is never replaced: renaming a temporary
	// file over a device, a FIFO or a directory would destroy it. The unix-only
	// seed_unix_test.go covers the FIFO.
	parent := t.TempDir()
	path := filepath.Join(parent, "appendonly.aof")
	if err := os.Mkdir(path, 0o750); err != nil {
		t.Fatalf("Mkdir() error = %v", err)
	}

	_, err := aof.Seed(path, []storage.SnapshotEntry{{Key: "a", Kind: storage.ValueKindString, String: []byte("1")}})
	if err == nil {
		t.Fatal("Seed() error = nil, want a refusal to replace a directory")
	}
	if !strings.HasPrefix(err.Error(), "aof: ") || !strings.Contains(err.Error(), fmt.Sprintf("%q", path)) {
		t.Fatalf("Seed() error = %q, want an aof: error naming %q", err, path)
	}
	if info, statErr := os.Stat(path); statErr != nil || !info.IsDir() {
		t.Fatalf("Stat(%q) = (%v, %v), want the directory still in place", path, info, statErr)
	}
	if got := directoryNames(t, parent); len(got) != 1 || got[0] != "appendonly.aof" {
		t.Fatalf("directory holds %q after the refusal, want only appendonly.aof", got)
	}
}

func TestSeedLeavesNoFileWhenTheSnapshotCannotBeWritten(t *testing.T) {
	// A collection with a TTL is one GenerateRewrite refuses. The failure must
	// not leave the half-written temporary file behind, or an AOF in its place.
	dir := t.TempDir()
	path := filepath.Join(dir, "appendonly.aof")

	_, err := aof.Seed(path, []storage.SnapshotEntry{
		{Key: "a", Kind: storage.ValueKindString, String: []byte("1")},
		{Key: "h", Kind: storage.ValueKindHash, Hash: []storage.HashFieldValue{{Field: "f", Value: []byte("v")}}, ExpiresAt: time.Now().Add(time.Hour).UnixMilli()},
	})
	if err == nil {
		t.Fatal("Seed() error = nil, want the rewrite's refusal of a hash with a TTL")
	}
	if got := directoryNames(t, dir); len(got) != 0 {
		t.Fatalf("directory holds %q after a failed Seed(), want nothing", got)
	}
}
