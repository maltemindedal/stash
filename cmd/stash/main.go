// Command stash runs the Redis-compatible TCP key/value server.
//
// It parses command-line configuration, wires the storage engine, command
// executor, and TCP server together, and serves clients until a shutdown
// signal arrives. See docs/reference/configuration.md for the available flags.
package main

import (
	"context"
	"os"

	"github.com/maltemindedal/stash/internal/command"
	"github.com/maltemindedal/stash/internal/config"
	stashlogger "github.com/maltemindedal/stash/internal/logger"
	"github.com/maltemindedal/stash/internal/server"
	"github.com/maltemindedal/stash/internal/storage"
)

func main() {
	ctx, stop := server.NotifyContext(context.Background())
	defer stop()

	cfg := config.ParseFlags()
	logger := stashlogger.New(cfg.LogLevel)
	store := storage.NewStore()
	executor := command.NewExecutor(store, logger)
	srv := server.New(cfg, logger, store, executor)

	if err := srv.ListenAndServe(ctx); err != nil {
		logger.Error("Stash exited with error", "error", err)
		os.Exit(1)
	}
}
