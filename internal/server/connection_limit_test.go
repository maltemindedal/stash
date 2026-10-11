package server

import (
	"io"
	"log/slog"
	"testing"

	"github.com/maltemindedal/stash/internal/config"
	"github.com/maltemindedal/stash/internal/storage"
)

// TestOverConnectionLimit verifies the maxclients soft cap: it trips once the
// active connection count reaches the configured limit, and a zero limit
// disables the check.
func TestOverConnectionLimit(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	cfg := config.Default()
	cfg.MaxClients = 2
	srv := newTestServer(t, cfg, logger, storage.NewStore(), stubExecutor{})

	if srv.overConnectionLimit() {
		t.Fatal("overConnectionLimit() = true with 0 connections, want false")
	}
	srv.registerClient(&stubConn{})
	if srv.overConnectionLimit() {
		t.Fatal("overConnectionLimit() = true with 1/2 connections, want false")
	}
	srv.registerClient(&stubConn{})
	if !srv.overConnectionLimit() {
		t.Fatal("overConnectionLimit() = false at 2/2 connections, want true")
	}

	unlimitedCfg := config.Default()
	unlimitedCfg.MaxClients = 0
	unlimited := newTestServer(t, unlimitedCfg, logger, storage.NewStore(), stubExecutor{})
	unlimited.registerClient(&stubConn{})
	if unlimited.overConnectionLimit() {
		t.Fatal("overConnectionLimit() = true with MaxClients=0, want false (disabled)")
	}
}
