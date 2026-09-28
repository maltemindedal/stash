package aof

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/maltemindedal/stash/internal/protocol"
)

// LoadFile replays RESP-encoded commands from an append-only file.
func LoadFile(ctx context.Context, path string, replay func(context.Context, protocol.Value) error) (stats LoadStats, err error) {
	if replay == nil {
		return LoadStats{}, errors.New("aof: nil replay function")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	file, err := os.Open(path)
	if err != nil {
		return LoadStats{}, fmt.Errorf("aof: read %q: %w", path, err)
	}
	defer func() {
		closeErr := file.Close()
		if closeErr != nil && err == nil {
			err = fmt.Errorf("aof: close %q: %w", path, closeErr)
		}
	}()

	counter := &countingReader{reader: file}
	reader := bufio.NewReader(counter)
	parser := protocol.NewParser(reader)
	for {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return stats, fmt.Errorf("aof: load %q canceled: %w", path, ctxErr)
		}

		value, parseErr := parser.Parse()
		if parseErr != nil {
			if errors.Is(parseErr, io.EOF) || errors.Is(parseErr, io.ErrUnexpectedEOF) {
				// The parser ran into the end of the file. If it had consumed
				// anything after the last complete command, that is one unfinished
				// command, and it runs to the end of the file.
				if counter.n > stats.ValidBytes {
					stats.TornTail = true
					stats.TruncatedTail = true
				}
				return stats, nil
			}
			if isRecoverableTruncation(parseErr) {
				stats.TruncatedTail = true
				return stats, nil
			}

			return stats, fmt.Errorf("aof: parse %q after %d commands: %w", path, stats.ReplayedCommands, parseErr)
		}
		stats.ValidBytes = counter.n - int64(reader.Buffered())
		if replayErr := replay(ctx, value); replayErr != nil {
			return stats, fmt.Errorf("aof: replay command %d from %q: %w", stats.ReplayedCommands+1, path, replayErr)
		}
		stats.ReplayedCommands++
	}
}

// countingReader counts the bytes read through it, so the loader can tell where
// in the file the last complete command ends.
type countingReader struct {
	reader io.Reader
	n      int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.reader.Read(p)
	c.n += int64(n)
	return n, err
}

// TruncateTail cuts the append-only file to size bytes and syncs it. Startup uses
// it to drop an unfinished trailing command (LoadStats.TornTail), so that the
// commands appended next follow the last complete one instead of being read as
// the rest of the torn command on the following restart.
func TruncateTail(path string, size int64) (err error) {
	file, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		return fmt.Errorf("aof: open %q to truncate: %w", path, err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("aof: close %q after truncate: %w", path, closeErr)
		}
	}()

	if err := file.Truncate(size); err != nil {
		return fmt.Errorf("aof: truncate %q to %d bytes: %w", path, size, err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("aof: sync %q after truncate: %w", path, err)
	}
	return nil
}

func isRecoverableTruncation(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}

	// A frame whose final CRLF terminator never arrived is a torn trailing write,
	// not corruption. The parser reports it with the typed ErrMissingCRLF
	// sentinel, so match on that rather than the error message text.
	return errors.Is(err, protocol.ErrMissingCRLF)
}
