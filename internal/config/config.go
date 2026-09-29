package config

import (
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config contains the runtime settings for the Stash server.
type Config struct {
	Host                 string
	Port                 int
	LogLevel             string
	EvictionInterval     time.Duration
	EvictionSampleSize   int
	RDBPath              string
	DumpPath             string
	AOFPath              string
	AppendFsync          string
	ReplicaOf            string
	MasterAuth           string
	RequirePass          string
	MaxMemory            int64
	MaxClients           int
	SlowlogLogSlowerThan time.Duration
	EventLoop            bool
	// AllowOpenBind lets the server listen beyond loopback with no
	// --requirepass; without it that combination is refused at startup.
	AllowOpenBind bool
	// AuthTimeout is how long a client may stay connected without authenticating
	// on a server that requires a password; zero disables the limit.
	AuthTimeout time.Duration
}

// Default returns the default runtime configuration. The listener binds to
// loopback (127.0.0.1) so an out-of-the-box server with no --requirepass is not
// reachable from the network. Set --host explicitly (e.g. 0.0.0.0 or "") to bind
// other interfaces; that requires --requirepass unless --allow-open-bind says an
// unauthenticated datastore is intended.
func Default() Config {
	return Config{
		Host:                 "127.0.0.1",
		Port:                 6379,
		LogLevel:             "info",
		EvictionInterval:     100 * time.Millisecond,
		EvictionSampleSize:   20,
		RDBPath:              "",
		DumpPath:             "dump.rdb",
		AOFPath:              "",
		AppendFsync:          "everysec",
		ReplicaOf:            "",
		MasterAuth:           "",
		MaxMemory:            0,
		MaxClients:           10000,
		SlowlogLogSlowerThan: 10 * time.Millisecond,
		EventLoop:            false,
		AuthTimeout:          30 * time.Second,
	}
}

// ParseFlags parses command-line flags into a Config value.
func ParseFlags() Config {
	cfg, err := parseFlags(flag.CommandLine, os.Args[1:])
	if err == nil {
		return cfg
	}

	_, _ = fmt.Fprintf(os.Stderr, "failed to parse flags: %v\n", err)
	os.Exit(2)
	return Config{}
}

func parseFlags(fs *flag.FlagSet, args []string) (Config, error) {
	cfg := Default()

	fs.StringVar(&cfg.Host, "host", cfg.Host, "host interface to bind the TCP listener to")
	fs.IntVar(&cfg.Port, "port", cfg.Port, "TCP port to listen on")
	fs.StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "log level: debug, info, warn, error")
	fs.DurationVar(&cfg.EvictionInterval, "eviction-interval", cfg.EvictionInterval, "interval for active TTL eviction")
	fs.IntVar(&cfg.EvictionSampleSize, "eviction-sample-size", cfg.EvictionSampleSize, "number of keys to sample on each eviction pass")
	fs.StringVar(&cfg.RDBPath, "rdb", cfg.RDBPath, "optional path to an RDB file to load before accepting TCP connections")
	fs.StringVar(&cfg.DumpPath, "dump", cfg.DumpPath, "path to write an RDB snapshot during graceful shutdown")
	fs.StringVar(&cfg.AOFPath, "aof", cfg.AOFPath, "optional path to an append-only file used for durable command logging")
	fs.Int64Var(&cfg.MaxMemory, "maxmemory", cfg.MaxMemory, "approximate keyspace memory limit in bytes; 0 disables memory pressure eviction")
	fs.IntVar(&cfg.MaxClients, "maxclients", cfg.MaxClients, "maximum number of concurrent client connections; 0 disables the limit")
	fs.StringVar(&cfg.ReplicaOf, "replicaof", cfg.ReplicaOf, "optional master address in host:port form for replica mode")
	fs.StringVar(&cfg.MasterAuth, "masterauth", cfg.MasterAuth, "optional password used by replica mode to AUTH against a protected master")
	fs.StringVar(&cfg.RequirePass, "requirepass", cfg.RequirePass, "optional password required for AUTH-protected client commands")
	var requirePassFile, masterAuthFile string
	fs.StringVar(&requirePassFile, "requirepass-file", "", "read the --requirepass password from this file instead of the command line, where it would be visible in the process list")
	fs.StringVar(&masterAuthFile, "masterauth-file", "", "read the --masterauth password from this file instead of the command line")
	fs.DurationVar(&cfg.AuthTimeout, "auth-timeout", cfg.AuthTimeout, "how long a client may stay connected without authenticating when --requirepass is set; 0 disables the limit")
	fs.BoolVar(&cfg.AllowOpenBind, "allow-open-bind", cfg.AllowOpenBind, "allow listening on a non-loopback address with no --requirepass; without this flag the server refuses to start in that configuration")
	fs.BoolVar(&cfg.EventLoop, "event-loop", cfg.EventLoop, "serve clients through an OS I/O multiplexing event loop; supported on Linux (epoll) and macOS (kqueue), other platforms fall back to one goroutine per connection")
	fs.Func("slowlog-log-slower-than", "slow query threshold in microseconds; 0 logs all commands and negative disables slowlog", func(value string) error {
		threshold, err := parseSlowlogThreshold(value)
		if err != nil {
			return err
		}

		cfg.SlowlogLogSlowerThan = threshold
		return nil
	})
	fs.Func("appendfsync", "appendfsync policy: always, everysec, no", func(value string) error {
		normalized, err := normalizeAppendFsync(value)
		if err != nil {
			return err
		}

		cfg.AppendFsync = normalized
		return nil
	})

	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}
	if err := readPasswordFile(&cfg.RequirePass, requirePassFile, "requirepass"); err != nil {
		return Config{}, err
	}
	if err := readPasswordFile(&cfg.MasterAuth, masterAuthFile, "masterauth"); err != nil {
		return Config{}, err
	}
	if cfg.MaxMemory < 0 {
		return Config{}, fmt.Errorf("invalid maxmemory %d: expected non-negative bytes", cfg.MaxMemory)
	}
	if cfg.AuthTimeout < 0 {
		return Config{}, fmt.Errorf("invalid auth-timeout %v: expected a non-negative duration", cfg.AuthTimeout)
	}
	if cfg.MaxClients < 0 {
		return Config{}, fmt.Errorf("invalid maxclients %d: expected a non-negative count", cfg.MaxClients)
	}
	// Port 0 is allowed and asks the OS to choose an ephemeral port.
	if cfg.Port < 0 || cfg.Port > 65535 {
		return Config{}, fmt.Errorf("invalid port %d: expected 0-65535", cfg.Port)
	}

	return cfg, nil
}

// readPasswordFile sets *password from path, when one is given. A password on the
// command line is visible to every local user in the process list; a file is not.
// The line ending that echo or an editor adds is dropped. An empty password would
// mean "no authentication", so an empty file is an error rather than a silent
// downgrade, and giving both the flag and the file is refused as ambiguous.
func readPasswordFile(password *string, path string, name string) error {
	if path == "" {
		return nil
	}
	if *password != "" {
		return fmt.Errorf("--%s and --%s-file are mutually exclusive", name, name)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read --%s-file: %w", name, err)
	}
	value := strings.TrimRight(string(data), "\r\n")
	if value == "" {
		return fmt.Errorf("--%s-file %q is empty", name, path)
	}

	*password = value
	return nil
}

// Address formats the listen address used by net.Listen. An IPv6 literal may be
// given bare (::1) or in brackets ([::1]); both produce the bracketed form.
func (c Config) Address() string {
	host := c.Host
	if strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
		host = host[1 : len(host)-1]
	}

	return net.JoinHostPort(host, strconv.Itoa(c.Port))
}

// IsReplica reports whether the server should connect to an upstream master.
func (c Config) IsReplica() bool {
	return strings.TrimSpace(c.ReplicaOf) != ""
}

// ReplicaAddress validates and normalizes the configured upstream master address.
func (c Config) ReplicaAddress() (string, error) {
	address := strings.TrimSpace(c.ReplicaOf)
	if address == "" {
		return "", nil
	}

	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return "", fmt.Errorf("invalid replicaof address %q: %w", address, err)
	}
	if host == "" || port == "" {
		return "", fmt.Errorf("invalid replicaof address %q: expected host:port", address)
	}

	return net.JoinHostPort(host, port), nil
}

func normalizeAppendFsync(value string) (string, error) {
	normalized := strings.ToLower(strings.TrimSpace(value))
	switch normalized {
	case "always", "everysec", "no":
		return normalized, nil
	default:
		return "", fmt.Errorf("invalid appendfsync policy %q: expected always, everysec, or no", value)
	}
}

func parseSlowlogThreshold(value string) (time.Duration, error) {
	micros, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid slowlog-log-slower-than %q: expected integer microseconds", value)
	}
	if micros < 0 {
		return -1, nil
	}

	return time.Duration(micros) * time.Microsecond, nil
}
