package test

import (
	"bytes"
	"context"
	"log/slog"
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
	tests := []struct {
		name     string
		host     string
		password string
		wantWarn bool
	}{
		{name: "loopback without a password", host: "127.0.0.1"},
		{name: "all interfaces without a password", host: "0.0.0.0", wantWarn: true},
		{name: "all interfaces with a password", host: "0.0.0.0", password: "s3cret"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := defaultTestConfig()
			cfg.Host = tt.host
			cfg.RequirePass = tt.password

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
