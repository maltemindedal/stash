package server

import (
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/maltemindedal/stash/internal/config"
	"github.com/maltemindedal/stash/internal/storage"
)

func TestCheckBindSafety(t *testing.T) {
	tests := []struct {
		name          string
		host          string
		password      string
		allowOpenBind bool
		wantRefused   bool
	}{
		{name: "IPv4 loopback", host: "127.0.0.1"},
		{name: "IPv6 loopback", host: "::1"},
		{name: "IPv6 loopback in brackets", host: "[::1]"},
		{name: "localhost", host: "localhost"},
		{name: "all IPv4 interfaces", host: "0.0.0.0", wantRefused: true},
		{name: "all interfaces by empty host", host: "", wantRefused: true},
		{name: "all IPv6 interfaces", host: "::", wantRefused: true},
		{name: "all IPv6 interfaces in brackets", host: "[::]", wantRefused: true},
		{name: "a routable address", host: "10.1.2.3", wantRefused: true},
		{name: "all interfaces with a password", host: "0.0.0.0", password: "s3cret"},
		{name: "all interfaces, explicitly allowed", host: "0.0.0.0", allowOpenBind: true},
		{name: "an address that does not resolve is left to net.Listen", host: "no such host.invalid"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := config.Default()
			cfg.Host = tt.host
			cfg.RequirePass = tt.password
			cfg.AllowOpenBind = tt.allowOpenBind
			srv := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), storage.NewStore(), stubExecutor{})

			err := srv.checkBindSafety()
			if refused := err != nil; refused != tt.wantRefused {
				t.Fatalf("checkBindSafety() error = %v, want refused = %v", err, tt.wantRefused)
			}
			if err != nil && !strings.Contains(err.Error(), "--allow-open-bind") {
				t.Fatalf("checkBindSafety() error = %q, want it to name the override flag", err)
			}
		})
	}
}
