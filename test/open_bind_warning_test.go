package test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maltemindedal/stash/internal/command"
	"github.com/maltemindedal/stash/internal/server"
	"github.com/maltemindedal/stash/internal/storage"
)

// syncBuffer is a bytes.Buffer that the server's goroutines and the test can
// share.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestServerWarnsWhenReachableFromTheNetworkWithoutAPassword(t *testing.T) {
	// Without --requirepass a non-loopback bind is refused unless it is allowed
	// explicitly; the warning is what the allowed case still logs.
	tests := []struct {
		name          string
		host          string
		password      string
		allowOpenBind bool
		wantWarn      bool
	}{
		{name: "loopback without a password", host: "127.0.0.1"},
		{name: "all interfaces without a password, explicitly allowed", host: "0.0.0.0", allowOpenBind: true, wantWarn: true},
		{name: "all interfaces with a password", host: "0.0.0.0", password: "s3cret"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := defaultTestConfig()
			cfg.Host = tt.host
			cfg.RequirePass = tt.password
			cfg.AllowOpenBind = tt.allowOpenBind

			logs := &syncBuffer{}
			logger := slog.New(slog.NewTextHandler(logs, nil))
			store := storage.NewStore()
			srv := server.New(cfg, logger, store, command.NewExecutor(store, logger))

			ctx, cancel := context.WithCancel(context.Background())
			errCh := make(chan error, 1)
			go func() { errCh <- srv.ListenAndServe(ctx) }()
			waitForAddr(t, srv)

			// The warning is logged right after the "listening" line.
			deadline := time.Now().Add(2 * time.Second)
			for !strings.Contains(logs.String(), "Stash listening") && time.Now().Before(deadline) {
				time.Sleep(5 * time.Millisecond)
			}
			cancel()
			waitForServerStop(t, errCh)

			got := logs.String()
			if !strings.Contains(got, "Stash listening") {
				t.Fatalf("server never logged that it was listening:\n%s", got)
			}
			if hasWarning := strings.Contains(got, "level=WARN") && strings.Contains(got, "no password"); hasWarning != tt.wantWarn {
				t.Fatalf("no-password warning logged = %v, want %v\n%s", hasWarning, tt.wantWarn, got)
			}
		})
	}
}

func TestServerRefusesToListenBeyondLoopbackWithoutAPassword(t *testing.T) {
	// The AOF path must stay untouched: the refusal comes before any persistence
	// is opened, so a rejected start has no side effects.
	aofPath := filepath.Join(t.TempDir(), "appendonly.aof")

	for _, host := range []string{"0.0.0.0", ""} {
		t.Run("host "+strconv.Quote(host), func(t *testing.T) {
			cfg := defaultTestConfig()
			cfg.Host = host
			cfg.AOFPath = aofPath

			logs := &syncBuffer{}
			logger := slog.New(slog.NewTextHandler(logs, nil))
			store := storage.NewStore()
			srv := server.New(cfg, logger, store, command.NewExecutor(store, logger))

			// A server that wrongly starts would serve until the context ends.
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			err := srv.ListenAndServe(ctx)
			if err == nil {
				t.Fatalf("ListenAndServe() error = nil, want a refusal\n%s", logs.String())
			}
			for _, want := range []string{"--requirepass", "--allow-open-bind"} {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("ListenAndServe() error = %q, want it to mention %s", err, want)
				}
			}
			if strings.Contains(logs.String(), "Stash listening") {
				t.Fatalf("server logged that it was listening despite refusing:\n%s", logs.String())
			}
			if _, statErr := os.Stat(aofPath); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("Stat(%q) error = %v, want no AOF created by a refused start", aofPath, statErr)
			}
		})
	}
}
