package aof_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/maltemindedal/stash/internal/aof"
)

func TestWriterPolicyNoFlushesOnAppend(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	writer, err := aof.OpenWriter(context.Background(), path, aof.PolicyNo, nil)
	if err != nil {
		t.Fatalf("OpenWriter() error = %v", err)
	}
	defer func() {
		if closeErr := writer.Close(); closeErr != nil {
			t.Errorf("Close() error = %v", closeErr)
		}
	}()

	payload := []byte("*1\r\n$4\r\nPING\r\n")
	if err := writer.Append(payload); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%q) error = %v", path, err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("file contents = %q, want %q", got, payload)
	}
}

// TestOpenWriterCreatesTheDirectoryPrivately pins that a data directory the
// server creates is not readable or searchable by other users: the AOF file
// itself is 0600, and a world-searchable parent only leaks its name and size.
func TestOpenWriterCreatesTheDirectoryPrivately(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows does not use Unix permission bits")
	}

	dir := filepath.Join(t.TempDir(), "data", "aof")
	writer, err := aof.OpenWriter(context.Background(), filepath.Join(dir, "appendonly.aof"), aof.PolicyNo, nil)
	if err != nil {
		t.Fatalf("OpenWriter() error = %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	info, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("Stat(%q) error = %v", dir, err)
	}
	if extra := info.Mode().Perm() &^ 0o750; extra != 0 {
		t.Fatalf("directory mode = %#o, want no bits outside 0750 (extra %#o)", info.Mode().Perm(), extra)
	}
}
