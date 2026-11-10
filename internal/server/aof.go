package server

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/maltemindedal/stash/internal/aof"
	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/rdb"
	"github.com/maltemindedal/stash/internal/record"
	"github.com/maltemindedal/stash/internal/storage"
)

func (s *Server) initializePersistence(ctx context.Context) error {
	if s == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	loadRDB := func() (int, error) {
		if s.cfg.RDBPath == "" {
			return 0, nil
		}

		startedAt := time.Now()
		stats, err := rdb.LoadFile(s.cfg.RDBPath, s.store)
		if err != nil {
			return 0, fmt.Errorf("server: load rdb %q: %w", s.cfg.RDBPath, err)
		}

		s.logger.Info(
			"loaded RDB snapshot",
			"path", s.cfg.RDBPath,
			"loaded_keys", stats.LoadedKeys,
			"skipped_expired_keys", stats.SkippedExpiredKeys,
			"duration", time.Since(startedAt),
		)
		return stats.LoadedKeys, nil
	}

	if s.cfg.AOFPath == "" {
		_, err := loadRDB()
		return err
	}

	policy, err := aof.ParsePolicy(s.cfg.AppendFsync)
	if err != nil {
		return fmt.Errorf("server: parse appendfsync policy %q: %w", s.cfg.AppendFsync, err)
	}

	exists, size, err := aofFileState(s.cfg.AOFPath)
	if err != nil {
		return fmt.Errorf("server: inspect aof %q: %w", s.cfg.AOFPath, err)
	}
	if exists && size > 0 {
		s.logger.Info("AOF detected, skipping RDB startup load", "aof_path", s.cfg.AOFPath, "rdb_path", s.cfg.RDBPath)
		startedAt := time.Now()
		// Replay is ordered like any request. Nothing records writes yet (the
		// Recorder gets the writer below), so each one takes the sequencer's gate
		// shared and no stripe. The Recorder records nothing for replay's Origin.
		replayCall := Call{Origin: record.OriginReplay}
		stats, err := aof.LoadFile(ctx, s.cfg.AOFPath, func(replayCtx context.Context, value protocol.Value) error {
			_, replayErr := s.executor.Handle(replayCtx, replayCall, value)
			return replayErr
		})
		if err != nil {
			var corrupt *aof.CorruptError
			if errors.As(err, &corrupt) {
				return fmt.Errorf("server: load aof %q: %w; the file was not modified, see \"Repairing a corrupt append-only file\" in docs/guides/persistence.md", s.cfg.AOFPath, err)
			}
			return fmt.Errorf("server: load aof %q: %w", s.cfg.AOFPath, err)
		}

		s.logger.Info(
			"loaded append-only file",
			"path", s.cfg.AOFPath,
			"replayed_commands", stats.ReplayedCommands,
			"truncated_tail", stats.TruncatedTail,
			"duration", time.Since(startedAt),
		)
		if stats.TornTail {
			// New commands are appended to this file, so a torn command left in
			// place would swallow them on the next restart.
			if err := aof.TruncateTail(s.cfg.AOFPath, stats.ValidBytes); err != nil {
				return fmt.Errorf("server: discard torn tail of aof %q: %w", s.cfg.AOFPath, err)
			}
			s.logger.Warn(
				"discarded an unfinished command at the end of the append-only file",
				"path", s.cfg.AOFPath,
				"kept_bytes", stats.ValidBytes,
			)
		}
	} else {
		loadedKeys, err := loadRDB()
		if err != nil {
			return err
		}
		if loadedKeys > 0 {
			// The first command appended makes the file non-empty, and from then on
			// startup loads it and skips the RDB snapshot, so the keys the snapshot
			// loaded go into the file first.
			startedAt := time.Now()
			stats, err := aof.SeedFile(s.cfg.AOFPath, s.store.Image(nil))
			if err != nil {
				return fmt.Errorf("server: write the keys loaded from rdb %q into aof %q: %w", s.cfg.RDBPath, s.cfg.AOFPath, err)
			}
			s.logger.Info(
				"seeded append-only file from RDB snapshot",
				"path", s.cfg.AOFPath,
				"written_keys", stats.Keys,
				"duration", time.Since(startedAt),
			)
		}
	}

	writer, err := aof.OpenWriter(ctx, s.cfg.AOFPath, policy, s.logger)
	if err != nil {
		return fmt.Errorf("server: open aof writer %q: %w", s.cfg.AOFPath, err)
	}
	if s.aofWriter != nil {
		if closeErr := s.aofWriter.Close(); closeErr != nil {
			s.logger.Warn("failed to close stale AOF writer before reopening", "path", s.cfg.AOFPath, "error", closeErr)
		}
	}
	writer.SetRewriteGuard(s.executor.BeginBackgroundWrite)
	s.aofWriter = writer
	s.recorder.SetLog(writer)
	return nil
}

func (s *Server) beginAOFRewrite(_ context.Context) error {
	if s == nil || s.aofWriter == nil {
		return fmt.Errorf("append only file persistence is not enabled")
	}

	runtimeCtx := s.runtimeCtx
	if runtimeCtx == nil {
		runtimeCtx = context.Background()
	}

	return s.aofWriter.BeginRewrite(runtimeCtx, s.store)
}

func (s *Server) closeAOFWriter() error {
	if s == nil || s.aofWriter == nil {
		return nil
	}

	writer := s.aofWriter
	s.recorder.SetLog(nil)
	s.aofWriter = nil
	if err := writer.Close(); err != nil {
		return fmt.Errorf("server: close aof writer: %w", err)
	}

	return nil
}

// evictionCount is a Changes that passes each change on and counts the keys
// removed among them, and how many of those a memory-pressure eviction removed.
type evictionCount struct {
	changes storage.Changes
	removed int
	evicted int
}

func (c *evictionCount) KeyChanged(key string, cause storage.Cause) {
	if cause != storage.Written {
		c.removed++
	}
	if cause == storage.MemoryEvicted {
		c.evicted++
	}
	if c.changes != nil {
		c.changes.KeyChanged(key, cause)
	}
}

// enforceMaxMemoryAtStartup turns memory accounting on and evicts keys until
// the keyspace fits the limit, as one Background unit, which records a DEL of
// the keys it removed. It then fsyncs the append-only file under every fsync
// policy, so a failure is known before the server accepts a client. Without
// the DEL the next start replays the keys' SETs and brings them back. Startup
// runs before the listener opens, so no client has a WATCH and no Replica is
// attached, and a Replica that attaches later gets the keyspace without these
// keys in its full resync.
func (s *Server) enforceMaxMemoryAtStartup() error {
	count := &evictionCount{}
	var enforceErr error
	recordErr := s.executor.RunBackgroundUnit(func(changes storage.Changes) {
		count.changes = changes
		s.store.ConfigureMaxMemory(count, s.cfg.MaxMemory, s.cfg.EvictionSampleSize)
		enforceErr = s.store.EnforceMaxMemory(count)
	})
	if enforceErr != nil {
		return fmt.Errorf("server: enforce maxmemory: %w", enforceErr)
	}
	if count.evicted > 0 {
		s.logger.Info("applied startup maxmemory eviction", "evicted_keys", count.evicted, "used_memory", s.store.UsedMemory(), "maxmemory", s.cfg.MaxMemory)
	}

	err := recordErr
	if err != nil {
		err = fmt.Errorf("server: append a DEL of the %d keys expired or evicted at startup to aof %q: %w", count.removed, s.cfg.AOFPath, err)
	} else if count.removed > 0 && s.aofWriter != nil {
		if syncErr := s.aofWriter.Sync(); syncErr != nil {
			err = fmt.Errorf("server: fsync a DEL of the %d keys expired or evicted at startup to aof %q: %w", count.removed, s.cfg.AOFPath, syncErr)
		}
	}
	if err != nil {
		if closeErr := s.closeAOFWriter(); closeErr != nil {
			s.logger.Warn("failed to close AOF writer after the startup eviction append failed", "error", closeErr)
		}
	}
	return err
}

func aofFileState(path string) (bool, int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, 0, nil
		}
		return false, 0, err
	}

	return true, info.Size(), nil
}
