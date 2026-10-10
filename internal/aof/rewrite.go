package aof

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/storage"
)

// GenerateRewrite emits the shortest practical RESP command stream for the supplied snapshot.
// A TTL is written as the absolute deadline it had, so replay does not restart
// the countdown from the loader's clock. It returns an error for a collection
// with a TTL, which no command it emits can recreate.
func GenerateRewrite(entries []storage.SnapshotEntry, writer io.Writer) (RewriteStats, error) {
	sorted := make([]storage.SnapshotEntry, len(entries))
	copy(sorted, entries)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Key < sorted[j].Key
	})

	now := time.Now().UnixMilli()
	stats := RewriteStats{}
	for _, entry := range sorted {
		frames, err := rewriteFramesForEntry(entry, now)
		if err != nil {
			return stats, err
		}
		if len(frames) == 0 {
			continue
		}
		stats.Keys++
		stats.Commands += len(frames)
		for _, frame := range frames {
			if err := protocol.WriteValue(writer, frame); err != nil {
				return stats, fmt.Errorf("aof: write rewrite frame for key %q: %w", entry.Key, err)
			}
		}
	}

	return stats, nil
}

// SeedFile writes entries as the append-only file at path, which must be
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
func SeedFile(path string, entries []storage.SnapshotEntry) (stats RewriteStats, err error) {
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
	if stats, err = GenerateRewrite(entries, buffered); err != nil {
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

func rewriteFramesForEntry(entry storage.SnapshotEntry, now int64) ([]protocol.Value, error) {
	// Only SET's PXAT recreates a TTL. No command gives a collection one today,
	// but refusing it keeps a future TTL from being dropped without a word.
	if entry.Kind != storage.ValueKindString && entry.ExpiresAt > 0 {
		return nil, fmt.Errorf("aof: cannot rewrite the TTL of %s key %q", entry.Kind, entry.Key)
	}

	switch entry.Kind {
	case storage.ValueKindString:
		if entry.ExpiresAt > 0 && entry.ExpiresAt <= now {
			return nil, nil
		}
		args := []protocol.Value{bulkString(entry.Key), bulkBytes(entry.String)}
		if entry.ExpiresAt > 0 {
			args = append(args, bulkString("PXAT"), bulkString(strconv.FormatInt(entry.ExpiresAt, 10)))
		}
		return []protocol.Value{rewriteCommand("SET", args...)}, nil
	case storage.ValueKindList:
		if len(entry.List) == 0 {
			return nil, nil
		}
		return chunkedCommands("RPUSH", entry.Key, len(entry.List), 1, func(args []protocol.Value, i int) []protocol.Value {
			return append(args, bulkBytes(entry.List[i]))
		}), nil
	case storage.ValueKindHash:
		if len(entry.Hash) == 0 {
			return nil, nil
		}
		sorted := make([]storage.HashFieldValue, len(entry.Hash))
		copy(sorted, entry.Hash)
		sort.Slice(sorted, func(i, j int) bool {
			return sorted[i].Field < sorted[j].Field
		})
		return chunkedCommands("HSET", entry.Key, len(sorted), 2, func(args []protocol.Value, i int) []protocol.Value {
			return append(args, bulkString(sorted[i].Field), bulkBytes(sorted[i].Value))
		}), nil
	case storage.ValueKindSet:
		if len(entry.Set) == 0 {
			return nil, nil
		}
		members := make([]string, 0, len(entry.Set))
		for _, member := range entry.Set {
			members = append(members, string(member))
		}
		sort.Strings(members)
		return chunkedCommands("SADD", entry.Key, len(members), 1, func(args []protocol.Value, i int) []protocol.Value {
			return append(args, bulkString(members[i]))
		}), nil
	case storage.ValueKindZSet:
		if len(entry.ZSet) == 0 {
			return nil, nil
		}
		return chunkedCommands("ZADD", entry.Key, len(entry.ZSet), 2, func(args []protocol.Value, i int) []protocol.Value {
			zsetEntry := entry.ZSet[i]
			return append(args, bulkString(strconv.FormatFloat(zsetEntry.Score, 'g', -1, 64)), bulkString(zsetEntry.Member))
		}), nil
	case storage.ValueKindStream:
		frames := make([]protocol.Value, 0, len(entry.Stream))
		for _, streamEntry := range entry.Stream {
			args := make([]protocol.Value, 0, len(streamEntry.Values)+2)
			args = append(args, bulkString(entry.Key), bulkString(streamEntry.ID))
			for _, value := range streamEntry.Values {
				args = append(args, bulkBytes(value))
			}
			frames = append(frames, rewriteCommand("XADD", args...))
		}
		return frames, nil
	default:
		return nil, fmt.Errorf("aof: unsupported rewrite value kind %q for key %q", entry.Kind, entry.Key)
	}
}

// rewriteItemsPerCommand bounds how many values (or field/value pairs) one
// rewrite command carries. The loader's parser rejects a command array of more
// than 1,048,576 elements, so one command per key made a collection of about a
// million values, or half that many pairs, into an append-only file the server
// could not read back at startup. Several bounded commands replay to the same
// state; Redis's own rewrite groups items the same way.
const rewriteItemsPerCommand = 1024

// chunkedCommands emits name commands that together carry count items for key,
// at most rewriteItemsPerCommand items each. appendItem adds the arguments of
// item i, and argsPerItem is how many arguments that is.
func chunkedCommands(name, key string, count, argsPerItem int, appendItem func(args []protocol.Value, i int) []protocol.Value) []protocol.Value {
	frames := make([]protocol.Value, 0, (count+rewriteItemsPerCommand-1)/rewriteItemsPerCommand)
	for start := 0; start < count; start += rewriteItemsPerCommand {
		end := min(start+rewriteItemsPerCommand, count)
		args := make([]protocol.Value, 0, (end-start)*argsPerItem+1)
		args = append(args, bulkString(key))
		for i := start; i < end; i++ {
			args = appendItem(args, i)
		}
		frames = append(frames, rewriteCommand(name, args...))
	}
	return frames
}

func rewriteCommand(name string, args ...protocol.Value) protocol.Array {
	elements := make([]protocol.Value, 0, len(args)+1)
	elements = append(elements, protocol.TextBulkString{Value: name})
	elements = append(elements, args...)
	return protocol.Array{Elements: elements}
}

func bulkString(value string) protocol.BulkString {
	return protocol.BulkString{Data: []byte(value)}
}

func bulkBytes(value []byte) protocol.BulkString {
	return protocol.BulkString{Data: value}
}
