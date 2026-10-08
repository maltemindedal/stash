//go:build linux || darwin

package aof_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/maltemindedal/stash/internal/aof"
	"github.com/maltemindedal/stash/internal/storage"
)

func TestSeedRefusesAPathThatIsAFIFO(t *testing.T) {
	// The server decides whether to seed from the file's size, and a FIFO or a
	// device reports 0, the same as a new file. Replacing one with a regular file
	// would destroy it (/dev/full or /dev/null as the AOF path, say), so Seed
	// refuses. The FIFO lives under t.TempDir(), never in /dev.
	parent := t.TempDir()
	path := filepath.Join(parent, "appendonly.aof")
	if err := syscall.Mkfifo(path, 0o600); err != nil {
		t.Fatalf("Mkfifo() error = %v", err)
	}

	_, err := aof.Seed(path, []storage.SnapshotEntry{{Key: "a", Kind: storage.ValueKindString, String: []byte("1")}})
	if err == nil {
		t.Fatal("Seed() error = nil, want a refusal to replace a FIFO")
	}
	if !strings.HasPrefix(err.Error(), "aof: ") || !strings.Contains(err.Error(), fmt.Sprintf("%q", path)) {
		t.Fatalf("Seed() error = %q, want an aof: error naming %q", err, path)
	}
	info, statErr := os.Lstat(path)
	if statErr != nil || info.Mode()&os.ModeNamedPipe == 0 {
		t.Fatalf("Lstat(%q) = (%v, %v), want the FIFO still in place", path, info, statErr)
	}
	if got := directoryNames(t, parent); len(got) != 1 || got[0] != "appendonly.aof" {
		t.Fatalf("directory holds %q after the refusal, want only appendonly.aof", got)
	}
}
