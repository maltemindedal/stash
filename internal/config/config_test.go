package config

import (
	"flag"
	"io"
	"testing"
	"time"
)

func TestParseFlagsRejectsInvalidValues(t *testing.T) {
	cases := map[string][]string{
		"negative maxclients": {"--maxclients=-1"},
		"port too large":      {"--port=70000"},
		"negative port":       {"--port=-1"},
		"negative maxmemory":  {"--maxmemory=-1"},
	}
	for name, args := range cases {
		t.Run(name, func(t *testing.T) {
			fs := flag.NewFlagSet("stash-test", flag.ContinueOnError)
			fs.SetOutput(io.Discard)
			if _, err := parseFlags(fs, args); err == nil {
				t.Fatalf("parseFlags(%v) error = nil, want validation failure", args)
			}
		})
	}
}

func TestParseFlagsAcceptsMaxClients(t *testing.T) {
	fs := flag.NewFlagSet("stash-test", flag.ContinueOnError)
	cfg, err := parseFlags(fs, []string{"--maxclients=50", "--port=0"})
	if err != nil {
		t.Fatalf("parseFlags() error = %v", err)
	}
	if cfg.MaxClients != 50 {
		t.Fatalf("MaxClients = %d, want 50", cfg.MaxClients)
	}
	if cfg.Port != 0 {
		t.Fatalf("Port = %d, want 0 (ephemeral allowed)", cfg.Port)
	}
}

func TestDefaultIncludesShutdownSnapshotPath(t *testing.T) {
	cfg := Default()

	if cfg.DumpPath != "dump.rdb" {
		t.Fatalf("DumpPath = %q, want %q", cfg.DumpPath, "dump.rdb")
	}
	if cfg.MasterAuth != "" {
		t.Fatalf("MasterAuth = %q, want empty string", cfg.MasterAuth)
	}
	if cfg.AppendFsync != "everysec" {
		t.Fatalf("AppendFsync = %q, want %q", cfg.AppendFsync, "everysec")
	}
	if cfg.MaxMemory != 0 {
		t.Fatalf("MaxMemory = %d, want 0", cfg.MaxMemory)
	}
	if cfg.SlowlogLogSlowerThan != 10*time.Millisecond {
		t.Fatalf("SlowlogLogSlowerThan = %v, want %v", cfg.SlowlogLogSlowerThan, 10*time.Millisecond)
	}
}

func TestParseFlags(t *testing.T) {
	t.Run("parses AOF flags", func(t *testing.T) {
		fs := flag.NewFlagSet("stash-test", flag.ContinueOnError)
		fs.SetOutput(io.Discard)

		cfg, err := parseFlags(fs, []string{"--aof", "appendonly.aof", "--appendfsync", "always", "--dump", "snapshot.rdb", "--maxmemory", "2048"})
		if err != nil {
			t.Fatalf("parseFlags() error = %v", err)
		}
		if cfg.AOFPath != "appendonly.aof" {
			t.Fatalf("AOFPath = %q, want %q", cfg.AOFPath, "appendonly.aof")
		}
		if cfg.AppendFsync != "always" {
			t.Fatalf("AppendFsync = %q, want %q", cfg.AppendFsync, "always")
		}
		if cfg.DumpPath != "snapshot.rdb" {
			t.Fatalf("DumpPath = %q, want %q", cfg.DumpPath, "snapshot.rdb")
		}
		if cfg.MaxMemory != 2048 {
			t.Fatalf("MaxMemory = %d, want 2048", cfg.MaxMemory)
		}
	})

	t.Run("parses slowlog threshold as microseconds", func(t *testing.T) {
		fs := flag.NewFlagSet("stash-test", flag.ContinueOnError)
		fs.SetOutput(io.Discard)

		cfg, err := parseFlags(fs, []string{"--slowlog-log-slower-than", "2500"})
		if err != nil {
			t.Fatalf("parseFlags() error = %v", err)
		}
		if cfg.SlowlogLogSlowerThan != 2500*time.Microsecond {
			t.Fatalf("SlowlogLogSlowerThan = %v, want %v", cfg.SlowlogLogSlowerThan, 2500*time.Microsecond)
		}
	})

	t.Run("accepts negative slowlog threshold to disable", func(t *testing.T) {
		fs := flag.NewFlagSet("stash-test", flag.ContinueOnError)
		fs.SetOutput(io.Discard)

		cfg, err := parseFlags(fs, []string{"--slowlog-log-slower-than", "-1"})
		if err != nil {
			t.Fatalf("parseFlags() error = %v", err)
		}
		if cfg.SlowlogLogSlowerThan >= 0 {
			t.Fatalf("SlowlogLogSlowerThan = %v, want disabled negative duration", cfg.SlowlogLogSlowerThan)
		}
	})

	t.Run("rejects non-integer slowlog threshold", func(t *testing.T) {
		fs := flag.NewFlagSet("stash-test", flag.ContinueOnError)
		fs.SetOutput(io.Discard)

		if _, err := parseFlags(fs, []string{"--slowlog-log-slower-than", "10ms"}); err == nil {
			t.Fatal("parseFlags() error = nil, want invalid slowlog threshold failure")
		}
	})

	t.Run("rejects invalid appendfsync policy", func(t *testing.T) {
		fs := flag.NewFlagSet("stash-test", flag.ContinueOnError)
		fs.SetOutput(io.Discard)

		if _, err := parseFlags(fs, []string{"--appendfsync", "sometimes"}); err == nil {
			t.Fatal("parseFlags() error = nil, want invalid appendfsync failure")
		}
	})

	t.Run("rejects negative maxmemory", func(t *testing.T) {
		fs := flag.NewFlagSet("stash-test", flag.ContinueOnError)
		fs.SetOutput(io.Discard)

		if _, err := parseFlags(fs, []string{"--maxmemory", "-1"}); err == nil {
			t.Fatal("parseFlags() error = nil, want negative maxmemory failure")
		}
	})
}

func TestAddress(t *testing.T) {
	cases := []struct {
		host string
		port int
		want string
	}{
		{"127.0.0.1", 6379, "127.0.0.1:6379"},
		{"localhost", 6379, "localhost:6379"},
		{"", 6379, ":6379"},
		{"0.0.0.0", 0, "0.0.0.0:0"},
		// An IPv6 literal works bare or in the brackets an earlier version required.
		{"::1", 6379, "[::1]:6379"},
		{"[::1]", 6379, "[::1]:6379"},
		{"::", 6380, "[::]:6380"},
		{"[::]", 6380, "[::]:6380"},
		{"fe80::1%eth0", 6379, "[fe80::1%eth0]:6379"},
	}
	for _, tc := range cases {
		t.Run(tc.host, func(t *testing.T) {
			got := Config{Host: tc.host, Port: tc.port}.Address()
			if got != tc.want {
				t.Fatalf("Address() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestParseFlagsAllowOpenBind(t *testing.T) {
	parse := func(args ...string) Config {
		t.Helper()

		fs := flag.NewFlagSet("stash-test", flag.ContinueOnError)
		fs.SetOutput(io.Discard)
		cfg, err := parseFlags(fs, args)
		if err != nil {
			t.Fatalf("parseFlags(%v) error = %v", args, err)
		}
		return cfg
	}

	if parse().AllowOpenBind {
		t.Fatal("AllowOpenBind = true by default, want false")
	}
	if !parse("--allow-open-bind").AllowOpenBind {
		t.Fatal("AllowOpenBind = false with --allow-open-bind, want true")
	}
}
