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
		// shared and no stripe, and gives it back here. The Recorder records
		// nothing for replay's Origin.
		replayCall := Call{Origin: record.OriginReplay}
		stats, err := aof.LoadFile(ctx, s.cfg.AOFPath, func(replayCtx context.Context, value protocol.Value) error {
			result, replayErr := s.executor.Handle(replayCtx, replayCall, value)
			if result.Release != nil {
				result.Release()
			}
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
			entries, _ := s.store.SnapshotAll()
			stats, err := aof.SeedFile(s.cfg.AOFPath, entries)
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
	if sequencer, ok := s.executor.(backgroundWriteSequencer); ok {
		writer.SetRewriteGuard(sequencer.BeginBackgroundWrite)
	}
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

// recordResult records the command frames of result, which a request with
// call's Origin produced, through the Recorder: its durability frames go to the
// AOF and its propagation frames to the replicas. The caller holds the
// request's sequencer locks (ExecuteResult.Release).
func (s *Server) recordResult(call Call, result ExecuteResult) record.Outcome {
	w := record.Write{Origin: call.Origin}
	addCommandFrames(&w, result.Durability, record.ToAOF)
	addCommandFrames(&w, result.Propagation, record.ToReplicas)
	return s.recorder.Record(&w)
}

func addCommandFrames(w *record.Write, frames []protocol.Value, to record.Dest) {
	for _, frame := range frames {
		if cmd, ok := frame.(protocol.Array); ok {
			w.Add(cmd, to)
		}
	}
}

// recordExpiredKeys gives an active TTL eviction the same effects a
// client-issued DEL would have had: it invalidates WATCH on the keys, appends
// the deletion to the AOF, and forwards it to replicas. The store evicts expired
// keys on its own schedule, so without this the deletion exists only in this
// process's memory: replicas keep serving the keys and replaying the AOF
// restores them.
func (s *Server) recordExpiredKeys(keys []string) {
	if s == nil || len(keys) == 0 {
		return
	}

	s.watchRegistry.Touch(keys...)

	// Under appendfsync always, which promises durability before a mutation is
	// announced, a DEL that could not be appended is not propagated either,
	// exactly as a client's write is not. The deferred policies make no such
	// promise, and a replica left serving a key this server has already
	// dropped is the worse outcome, so there the failure is logged and the
	// deletion still goes. The sweep and an accounted write publish expiries
	// while the sequencer orders them, but a replica's full resync publishes the
	// ones its new dataset holds with no sequencer lock (consumeFullResync), and
	// the store hands every expiry to whichever of them publishes next, so the
	// Recorder propagates the server's own writes as unordered.
	w := record.Write{Origin: record.OriginServer}
	w.Add(record.DeleteFrame(keys), record.ToBoth)
	s.recorder.Record(&w)
}

// persistStartupEvictions appends a DEL of the keys the startup memory-pressure
// eviction removed to the AOF and fsyncs it, under every fsync policy, so a
// failure is known before the server accepts a client. Without it the next
// start replays their SETs and brings them back. Each DEL names at most
// aof.ItemsPerCommand keys, because one start can evict more keys than the
// loader accepts in one command. Startup runs before the listener opens, so no
// client has a WATCH and no Replica is attached, and a Replica that attaches
// later gets the keyspace without these keys in its full resync: the AOF is the
// only place the DEL goes.
func (s *Server) persistStartupEvictions(keys []string) error {
	if s == nil || s.aofWriter == nil || len(keys) == 0 {
		return nil
	}

	w := record.Write{Origin: record.OriginServer}
	for start := 0; start < len(keys); start += aof.ItemsPerCommand {
		end := min(start+aof.ItemsPerCommand, len(keys))
		w.Add(record.DeleteFrame(keys[start:end]), record.ToAOF)
	}
	if outcome := s.recorder.Record(&w); outcome.Err != nil {
		return fmt.Errorf("server: append a DEL of the %d keys evicted at startup to aof %q: %w", len(keys), s.cfg.AOFPath, outcome.Err)
	}
	if err := s.aofWriter.Sync(); err != nil {
		return fmt.Errorf("server: fsync a DEL of the %d keys evicted at startup to aof %q: %w", len(keys), s.cfg.AOFPath, err)
	}
	return nil
}

func persistenceFailureResponse() protocol.ErrorValue {
	return protocol.ErrorValue{Message: "ERR persistence failure"}
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
