package config

import (
	"flag"
	"io"
	"os"
	"path/filepath"
	"strings"
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

func TestParseFlagsPasswordFiles(t *testing.T) {
	writeFile := func(t *testing.T, content string) string {
		t.Helper()

		path := filepath.Join(t.TempDir(), "secret")
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatalf("WriteFile() error = %v", err)
		}
		return path
	}
	parse := func(args ...string) (Config, error) {
		fs := flag.NewFlagSet("stash-test", flag.ContinueOnError)
		fs.SetOutput(io.Discard)
		return parseFlags(fs, args)
	}

	t.Run("reads the password from the file", func(t *testing.T) {
		cfg, err := parse("--requirepass-file", writeFile(t, "s3cret"), "--masterauth-file", writeFile(t, "upstream"))
		if err != nil {
			t.Fatalf("parseFlags() error = %v", err)
		}
		if cfg.RequirePass != "s3cret" || cfg.MasterAuth != "upstream" {
			t.Fatalf("RequirePass, MasterAuth = %q, %q, want s3cret, upstream", cfg.RequirePass, cfg.MasterAuth)
		}
	})

	t.Run("drops the line ending an editor or echo adds, and nothing else", func(t *testing.T) {
		for content, want := range map[string]string{
			"s3cret\n":     "s3cret",
			"s3cret\r\n":   "s3cret",
			" s3 cret \n":  " s3 cret ",
			"s3cret\n\n":   "s3cret",
			"pass\nword\n": "pass\nword",
		} {
			cfg, err := parse("--requirepass-file", writeFile(t, content))
			if err != nil {
				t.Fatalf("parseFlags(%q) error = %v", content, err)
			}
			if cfg.RequirePass != want {
				t.Fatalf("file %q gave password %q, want %q", content, cfg.RequirePass, want)
			}
		}
	})

	t.Run("a flag and a file for the same password are ambiguous", func(t *testing.T) {
		if _, err := parse("--requirepass", "a", "--requirepass-file", writeFile(t, "b")); err == nil {
			t.Fatal("--requirepass with --requirepass-file: error = nil, want a conflict")
		}
		if _, err := parse("--masterauth", "a", "--masterauth-file", writeFile(t, "b")); err == nil {
			t.Fatal("--masterauth with --masterauth-file: error = nil, want a conflict")
		}
	})

	t.Run("an empty or missing file is an error, never an empty password", func(t *testing.T) {
		// An empty password means "no authentication", so silently accepting an
		// empty file would turn a broken secret mount into an open server.
		for name, args := range map[string][]string{
			"empty requirepass file":          {"--requirepass-file", writeFile(t, "")},
			"newline-only requirepass file":   {"--requirepass-file", writeFile(t, "\n")},
			"missing requirepass file":        {"--requirepass-file", filepath.Join(t.TempDir(), "absent")},
			"empty masterauth file":           {"--masterauth-file", writeFile(t, "")},
			"missing masterauth file":         {"--masterauth-file", filepath.Join(t.TempDir(), "absent")},
			"directory instead of a password": {"--requirepass-file", t.TempDir()},
		} {
			if _, err := parse(args...); err == nil {
				t.Fatalf("%s: error = nil, want an error", name)
			}
		}
	})

	t.Run("the error does not contain the password", func(t *testing.T) {
		_, err := parse("--requirepass", "topsecret", "--requirepass-file", writeFile(t, "other"))
		if err == nil || strings.Contains(err.Error(), "topsecret") || strings.Contains(err.Error(), "other") {
			t.Fatalf("error = %v, want a conflict error that leaks neither password", err)
		}
	})
}

func TestParseFlagsAuthTimeout(t *testing.T) {
	parse := func(args ...string) (Config, error) {
		fs := flag.NewFlagSet("stash-test", flag.ContinueOnError)
		fs.SetOutput(io.Discard)
		return parseFlags(fs, args)
	}

	cfg, err := parse()
	if err != nil || cfg.AuthTimeout != 30*time.Second {
		t.Fatalf("default AuthTimeout = %v (error %v), want 30s", cfg.AuthTimeout, err)
	}
	if cfg, err = parse("--auth-timeout", "5s"); err != nil || cfg.AuthTimeout != 5*time.Second {
		t.Fatalf("--auth-timeout 5s: AuthTimeout = %v (error %v), want 5s", cfg.AuthTimeout, err)
	}
	if cfg, err = parse("--auth-timeout", "0"); err != nil || cfg.AuthTimeout != 0 {
		t.Fatalf("--auth-timeout 0: AuthTimeout = %v (error %v), want the limit disabled", cfg.AuthTimeout, err)
	}
	if _, err = parse("--auth-timeout", "-1s"); err == nil {
		t.Fatal("--auth-timeout -1s: error = nil, want a validation failure")
	}
}
