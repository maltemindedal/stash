package aof

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/maltemindedal/stash/internal/storage"
)

func seedTestEntries() []storage.SnapshotEntry {
	return []storage.SnapshotEntry{
		{Key: "a", Kind: storage.ValueKindString, String: []byte("1")},
		{Key: "b", Kind: storage.ValueKindString, String: []byte("2")},
	}
}

// TestSeedPublishesAFinishedFileAndThenSyncsTheDirectory pins the order that
// keeps a crash from leaving a half-written append-only file in place: the
// whole snapshot is in the temporary file, in the file's own directory so the
// rename is atomic, before it is renamed over the path, and the directory is
// fsynced after that rename and not before.
func TestSeedPublishesAFinishedFileAndThenSyncsTheDirectory(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "appendonly.aof")

	var events []string
	var renamedFrom string
	var renamedContent []byte
	originalRename, originalSyncDir := renameFile, syncDir
	renameFile = func(oldPath, newPath string) error {
		events = append(events, "rename "+newPath)
		renamedFrom = oldPath
		content, err := os.ReadFile(oldPath)
		if err != nil {
			return err
		}
		renamedContent = content
		return originalRename(oldPath, newPath)
	}
	syncDir = func(target string) error {
		events = append(events, "sync directory of "+target)
		return originalSyncDir(target)
	}
	defer func() { renameFile, syncDir = originalRename, originalSyncDir }()

	if _, err := Seed(path, seedTestEntries()); err != nil {
		t.Fatalf("Seed() error = %v", err)
	}

	want := []string{"rename " + path, "sync directory of " + path}
	if len(events) != len(want) || events[0] != want[0] || events[1] != want[1] {
		t.Fatalf("events = %q, want %q", events, want)
	}
	if filepath.Dir(renamedFrom) != dir {
		t.Fatalf("temporary file %q is not in the directory of %q", renamedFrom, path)
	}
	final, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if len(renamedContent) == 0 || !bytes.Equal(renamedContent, final) {
		t.Fatalf("temporary file held %q when renamed, want the finished file %q", renamedContent, final)
	}
}

func TestSeedFailureLeavesNoTemporaryFile(t *testing.T) {
	sentinel := errors.New("boom")
	tests := []struct {
		name string
		fail func()
		// seeded is whether the failure comes after the rename, so the path
		// already holds the finished file. A failed rename leaves the empty file.
		seeded bool
	}{
		{name: "the rename fails", fail: func() { renameFile = func(string, string) error { return sentinel } }},
		{name: "the directory sync fails", fail: func() { syncDir = func(string) error { return sentinel } }, seeded: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "appendonly.aof")
			if err := os.WriteFile(path, nil, 0o600); err != nil {
				t.Fatalf("WriteFile() error = %v", err)
			}

			originalRename, originalSyncDir := renameFile, syncDir
			defer func() { renameFile, syncDir = originalRename, originalSyncDir }()
			tt.fail()

			_, err := Seed(path, seedTestEntries())
			if !errors.Is(err, sentinel) {
				t.Fatalf("Seed() error = %v, want it to wrap %v", err, sentinel)
			}

			entries, readErr := os.ReadDir(dir)
			if readErr != nil {
				t.Fatalf("ReadDir() error = %v", readErr)
			}
			for _, entry := range entries {
				if entry.Name() != "appendonly.aof" {
					t.Fatalf("directory holds %q after a failed Seed(), want only appendonly.aof", entry.Name())
				}
			}
			info, statErr := os.Stat(path)
			if statErr != nil {
				t.Fatalf("Stat() error = %v", statErr)
			}
			if seeded := info.Size() > 0; seeded != tt.seeded {
				t.Fatalf("file size = %d after the failure, want a finished file: %v", info.Size(), tt.seeded)
			}
		})
	}
}
