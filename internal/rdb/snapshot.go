package rdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"time"

	"github.com/maltemindedal/stash/internal/storage"
)

// syncDir fsyncs the directory containing path so a preceding rename is durable
// across a crash. On POSIX a rename's new directory entry is not guaranteed to
// survive a crash until the parent directory's inode is fsynced. It is a no-op
// on Windows, where directory handles cannot be synced this way.
var syncDir = func(path string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	syncErr := dir.Sync()
	closeErr := dir.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

var emptySnapshot = func() []byte {
	payload := make([]byte, 0, len(fileHeader)+1+8)
	payload = append(payload, fileHeader...)
	payload = append(payload, opcodeEOF)
	payload = append(payload, make([]byte, 8)...)
	return payload
}()

var (
	renameFile = os.Rename
	removeFile = os.Remove
)

// SnapshotStats summarizes the shutdown snapshot written to disk.
type SnapshotStats struct {
	WrittenKeys        int
	SkippedExpiredKeys int
}

// EmptySnapshot returns the canonical empty RDB payload used for FULLRESYNC.
func EmptySnapshot() []byte {
	return bytes.Clone(emptySnapshot)
}

// BuildSnapshot encodes DB 0 string keys into an RDB payload compatible with
// the current startup loader implementation.
func BuildSnapshot(entries []storage.StringSnapshotEntry) ([]byte, SnapshotStats) {
	if len(entries) == 0 {
		return EmptySnapshot(), SnapshotStats{}
	}

	now := time.Now().UnixMilli()
	sortedEntries := make([]storage.StringSnapshotEntry, len(entries))
	copy(sortedEntries, entries)
	sort.Slice(sortedEntries, func(i, j int) bool {
		return sortedEntries[i].Key < sortedEntries[j].Key
	})

	payload := make([]byte, 0, len(fileHeader)+len(sortedEntries)*32+16)
	payload = append(payload, fileHeader...)
	payload = append(payload, opcodeSelectDB)
	payload = appendLength(payload, 0)

	stats := SnapshotStats{}
	for _, entry := range sortedEntries {
		if entry.ExpiresAt > 0 && entry.ExpiresAt <= now {
			stats.SkippedExpiredKeys++
			continue
		}

		if entry.ExpiresAt > 0 {
			payload = append(payload, opcodeExpireTimeMS)
			payload = binary.LittleEndian.AppendUint64(payload, uint64(entry.ExpiresAt))
		}

		payload = append(payload, valueTypeString)
		payload = appendString(payload, []byte(entry.Key))
		payload = appendString(payload, entry.Value)
		stats.WrittenKeys++
	}

	payload = append(payload, opcodeEOF)
	payload = append(payload, make([]byte, 8)...)
	return payload, stats
}

// SaveSnapshot writes an RDB snapshot to disk using a same-directory temporary
// file and best-effort replacement of any existing target file.
func SaveSnapshot(path string, entries []storage.StringSnapshotEntry) (stats SnapshotStats, err error) {
	if path == "" {
		return SnapshotStats{}, errors.New("rdb: empty snapshot path")
	}

	payload, builtStats := BuildSnapshot(entries)
	dir := filepath.Dir(path)
	if dir == "" {
		dir = "."
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return SnapshotStats{}, fmt.Errorf("rdb: create snapshot directory %q: %w", dir, err)
	}

	tempFile, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*")
	if err != nil {
		return SnapshotStats{}, fmt.Errorf("rdb: create temp snapshot for %q: %w", path, err)
	}
	tempPath := tempFile.Name()
	cleanupTemp := true
	defer func() {
		if cleanupTemp {
			if removeErr := removeFile(tempPath); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
				wrapped := fmt.Errorf("rdb: remove temp snapshot %q: %w", tempPath, removeErr)
				if err == nil {
					err = wrapped
				} else {
					err = errors.Join(err, wrapped)
				}
			}
		}
	}()

	if _, err := tempFile.Write(payload); err != nil {
		writeErr := fmt.Errorf("rdb: write temp snapshot %q: %w", tempPath, err)
		if closeErr := closeTempSnapshotFile(tempFile, "write failure"); closeErr != nil {
			return SnapshotStats{}, errors.Join(writeErr, closeErr)
		}
		return SnapshotStats{}, writeErr
	}
	if err := tempFile.Sync(); err != nil {
		syncErr := fmt.Errorf("rdb: sync temp snapshot %q: %w", tempPath, err)
		if closeErr := closeTempSnapshotFile(tempFile, "sync failure"); closeErr != nil {
			return SnapshotStats{}, errors.Join(syncErr, closeErr)
		}
		return SnapshotStats{}, syncErr
	}
	if err := closeTempSnapshotFile(tempFile, "snapshot finalization"); err != nil {
		return SnapshotStats{}, err
	}

	if err := replaceFile(tempPath, path); err != nil {
		return SnapshotStats{}, err
	}

	cleanupTemp = false
	return builtStats, nil
}

func closeTempSnapshotFile(file *os.File, reason string) error {
	if err := file.Close(); err != nil {
		return fmt.Errorf("rdb: close temp snapshot %q after %s: %w", file.Name(), reason, err)
	}

	return nil
}

func appendString(dst []byte, value []byte) []byte {
	dst = appendLength(dst, uint64(len(value)))
	return append(dst, value...)
}

func appendLength(dst []byte, length uint64) []byte {
	if length < 1<<6 {
		return append(dst, byte(length))
	}
	if length < 1<<14 {
		return append(dst, byte((length>>8)&0x3F)|0x40, byte(length))
	}

	buf := make([]byte, 5)
	buf[0] = 0x80
	binary.BigEndian.PutUint32(buf[1:], uint32(length))
	return append(dst, buf...)
}

// replaceFile moves tempPath over targetPath with a single rename. os.Rename
// replaces an existing file atomically on every supported platform, so there is
// no remove-then-rename fallback: it opened a window in which a failed second
// rename left no snapshot at all.
func replaceFile(tempPath string, targetPath string) error {
	if err := renameFile(tempPath, targetPath); err != nil {
		return fmt.Errorf("rdb: replace snapshot %q: %w", targetPath, err)
	}

	// Persist the rename itself: the new directory entry is not crash-durable
	// until the containing directory is fsynced.
	if err := syncDir(targetPath); err != nil {
		return fmt.Errorf("rdb: sync directory after replacing snapshot %q: %w", targetPath, err)
	}

	return nil
}
