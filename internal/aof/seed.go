package aof

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/maltemindedal/stash/internal/storage"
)

// Seed writes entries as a new append-only file at path, for a server whose data
// came from somewhere other than the file, such as an RDB snapshot. Without it
// the first command appended after startup makes the file non-empty, the next
// start replays only that and skips the snapshot, and everything the snapshot
// held is lost.
//
// The file is put in place the way a rewrite swaps one in, so a crash cannot
// leave a half-written file that a later start would replay as the whole log:
// the frames go to a temporary file in the same directory, which is fsynced and
// then renamed over path, and the directory is fsynced after the rename. The
// frames are GenerateRewrite's, so a TTL is written as an absolute deadline.
// Until the rename the path is as it was. If only the directory sync fails, the
// finished file is already in place and the error is reported anyway.
//
// path must be missing or an empty regular file. Seed replaces what is there, so
// it refuses anything else: a file that holds commands would lose them, and a
// device, FIFO or directory would be destroyed by the rename. A symlink is
// followed to decide this and is itself replaced, as an AOF rewrite's swap does.
func Seed(path string, entries []storage.SnapshotEntry) (stats RewriteStats, err error) {
	if path == "" {
		return RewriteStats{}, errors.New("aof: empty file path")
	}
	if err = requireMissingOrEmptyFile(path); err != nil {
		return RewriteStats{}, err
	}

	dir := filepath.Dir(path)
	if err = os.MkdirAll(dir, 0o750); err != nil {
		return RewriteStats{}, fmt.Errorf("aof: create directory %q: %w", dir, err)
	}
	tempFile, err := os.CreateTemp(dir, filepath.Base(path)+".seed-*")
	if err != nil {
		return RewriteStats{}, fmt.Errorf("aof: create seed temp file in %q: %w", dir, err)
	}
	tempPath := tempFile.Name()
	defer func() {
		if err == nil {
			return
		}
		// On the failures before the close the file is still open, and after a
		// failed directory sync it has already been renamed away. Neither makes
		// these two calls a problem.
		_ = tempFile.Close()
		if removeErr := removeFile(tempPath); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			err = errors.Join(err, fmt.Errorf("aof: remove seed temp file %q: %w", tempPath, removeErr))
		}
	}()

	buffered := bufio.NewWriter(tempFile)
	stats, err = GenerateRewrite(entries, buffered)
	if err != nil {
		return RewriteStats{}, fmt.Errorf("aof: generate seed file for %q: %w", path, err)
	}
	if err = buffered.Flush(); err != nil {
		return RewriteStats{}, fmt.Errorf("aof: write seed temp file %q: %w", tempPath, err)
	}
	if err = tempFile.Sync(); err != nil {
		return RewriteStats{}, fmt.Errorf("aof: sync seed temp file %q: %w", tempPath, err)
	}
	if err = tempFile.Close(); err != nil {
		return RewriteStats{}, fmt.Errorf("aof: close seed temp file %q: %w", tempPath, err)
	}
	if err = replaceFile(tempPath, path); err != nil {
		return RewriteStats{}, err
	}

	return stats, nil
}

// requireMissingOrEmptyFile reports an error unless path does not exist or is a
// regular file with nothing in it.
func requireMissingOrEmptyFile(path string) error {
	info, err := os.Stat(path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return nil
	case err != nil:
		return fmt.Errorf("aof: inspect %q: %w", path, err)
	case !info.Mode().IsRegular():
		return fmt.Errorf("aof: refusing to seed %q: it is not a regular file", path)
	case info.Size() > 0:
		return fmt.Errorf("aof: refusing to seed %q: it already holds %d bytes", path, info.Size())
	}

	return nil
}
