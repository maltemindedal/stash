package aof

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/storage"
)

// GenerateRewrite writes the recreation commands of img to writer as RESP
// command arrays. Which commands those are, how they are split and how a TTL is
// written is storage's to decide (see storage.Image.Recreate). It returns an
// error for an image that cannot be recreated, such as one holding a collection
// with a TTL.
func GenerateRewrite(img *storage.Image, writer io.Writer) (RewriteStats, error) {
	stats := RewriteStats{}
	var (
		elements []protocol.Value
		writeErr error
	)
	keys, err := img.Recreate(func(args [][]byte) error {
		stats.Commands++
		elements = elements[:0]
		for _, arg := range args {
			elements = append(elements, protocol.BulkString{Data: arg})
		}
		if err := protocol.WriteValue(writer, protocol.Array{Elements: elements}); err != nil {
			writeErr = fmt.Errorf("aof: write rewrite command for key %q: %w", args[1], err)
		}
		return writeErr
	})
	stats.Keys = keys
	switch {
	case writeErr != nil:
		return stats, writeErr
	case err != nil:
		return stats, fmt.Errorf("aof: generate rewrite: %w", err)
	}

	return stats, nil
}

// SeedFile writes img as the append-only file at path, which must be
// missing or an empty regular file. Startup uses it when an RDB snapshot loaded
// keys beside such a file: from the next start on the file is not empty, so it
// is loaded instead of the RDB snapshot, and it has to hold those keys. If path
// is a symbolic link, the file it leads to is the one written, as OpenWriter
// would append to it.
//
// It builds the file the way a rewrite does, so that a crash at any point
// leaves either the file as it was or all of the new one: the commands go to a
// temp file in the same directory, which is fsynced and closed, then renamed
// over the file, and then the directory is fsynced. If a step before the rename
// fails, the temp file is removed and the file is left as it was. If only the
// directory sync fails, the file already holds the complete seed, but the
// rename may not survive a crash. SeedFile returns an error then too, so that
// the caller appends nothing to a file that may not keep its name.
func SeedFile(path string, img *storage.Image) (stats RewriteStats, err error) {
	if path == "" {
		return RewriteStats{}, errors.New("aof: empty file path")
	}
	if path, err = resolveSeedPath(path); err != nil {
		return RewriteStats{}, err
	}
	// The rename replaces whatever path names: that must not be commands, or a
	// device such as /dev/full that reports a size of zero.
	info, err := os.Stat(path)
	switch {
	case errors.Is(err, os.ErrNotExist):
	case err != nil:
		return RewriteStats{}, fmt.Errorf("aof: inspect %q: %w", path, err)
	case !info.Mode().IsRegular():
		return RewriteStats{}, fmt.Errorf("aof: %q is not a regular file, and seeding it would put one in its place", path)
	case info.Size() > 0:
		return RewriteStats{}, fmt.Errorf("aof: %q already holds %d bytes, and seeding it would replace them", path, info.Size())
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return RewriteStats{}, fmt.Errorf("aof: create directory %q: %w", dir, err)
	}
	tempFile, err := os.CreateTemp(dir, filepath.Base(path)+".seed-*")
	if err != nil {
		return RewriteStats{}, fmt.Errorf("aof: create a temp file beside %q: %w", path, err)
	}
	tempPath := tempFile.Name()
	closed, renamed := false, false
	defer func() {
		if !closed {
			_ = tempFile.Close()
		}
		if renamed {
			return
		}
		if removeErr := removeFile(tempPath); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			err = errors.Join(err, fmt.Errorf("aof: remove temp file %q: %w", tempPath, removeErr))
		}
	}()

	buffered := bufio.NewWriter(tempFile)
	if stats, err = GenerateRewrite(img, buffered); err != nil {
		return stats, err
	}
	if err := buffered.Flush(); err != nil {
		return stats, fmt.Errorf("aof: write %q: %w", tempPath, err)
	}
	// The commands must be on disk before the rename lets path name them.
	if err := tempFile.Sync(); err != nil {
		return stats, fmt.Errorf("aof: sync %q: %w", tempPath, err)
	}
	closed = true
	if err := tempFile.Close(); err != nil {
		return stats, fmt.Errorf("aof: close %q: %w", tempPath, err)
	}
	if err := replaceFile(tempPath, path); err != nil {
		var unsynced *unsyncedReplaceError
		renamed = errors.As(err, &unsynced)
		return stats, err
	}
	renamed = true

	return stats, nil
}

// resolveSeedPath returns the file a seed of path replaces: path itself, unless
// it is a symbolic link. OpenWriter's open of path follows a link, but a rename
// over the link would replace the link with a regular file in its directory and
// leave the file it points to empty, moving the AOF off the volume the link
// chose. So a link resolves to the file it leads to. If that file does not
// exist yet, it is first created empty through the link, as OpenWriter would
// create it; an empty file there is also all a failed seed leaves behind.
func resolveSeedPath(path string) (string, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return path, nil
	}
	if err != nil {
		return "", fmt.Errorf("aof: inspect %q: %w", path, err)
	}
	if info.Mode()&os.ModeSymlink == 0 {
		return path, nil
	}
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			return "", fmt.Errorf("aof: create the file symbolic link %q leads to: %w", path, err)
		}
		if err := file.Close(); err != nil {
			return "", fmt.Errorf("aof: create the file symbolic link %q leads to: %w", path, err)
		}
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", fmt.Errorf("aof: resolve symbolic link %q: %w", path, err)
	}

	return resolved, nil
}

// ItemsPerCommand bounds how many keys one DEL the server composes for the
// append-only file carries, as it writes one for its own evictions. The loader's
// parser rejects a command array of more than 1,048,576 elements, so the keys go
// into several bounded commands, which replay to the same state. storage bounds
// the recreation commands of a rewrite the same way.
const ItemsPerCommand = 1024
