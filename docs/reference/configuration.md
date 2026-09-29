# Configuration reference

Stash is configured entirely through command-line flags. There is no configuration file and no environment-variable support.

[`internal/config/config.go`](../../internal/config/config.go) is the source of truth.

## Flags

| Flag | Type | Default | Effect |
| --- | --- | --- | --- |
| `--host` | string | `127.0.0.1` | Interface the TCP listener binds to. An empty value binds all interfaces. |
| `--port` | int | `6379` | TCP port to listen on. Accepts `0`–`65535`; `0` asks the OS for an ephemeral port. |
| `--log-level` | string | `info` | Log level: `debug`, `info`, `warn`, or `error`. |
| `--eviction-interval` | duration | `100ms` | Interval between active TTL eviction passes. |
| `--eviction-sample-size` | int | `20` | Number of keys sampled on each eviction pass. |
| `--rdb` | string | *(empty)* | Path to an RDB file to load before the listener opens. Empty disables startup RDB loading. |
| `--dump` | string | `dump.rdb` | Path to write an RDB snapshot to during graceful shutdown. |
| `--aof` | string | *(empty)* | Path to an append-only file for durable command logging. Empty disables AOF. |
| `--appendfsync` | string | `everysec` | Fsync policy: `always`, `everysec`, or `no`. Rejected at startup if the value is anything else. |
| `--maxmemory` | int64 | `0` | Approximate keyspace memory limit in bytes. `0` disables memory-pressure eviction. Negative values are rejected. |
| `--maxclients` | int | `10000` | Maximum concurrent client connections. `0` disables the limit. Negative values are rejected. |
| `--slowlog-log-slower-than` | int | `10000` | Slowlog threshold in **microseconds**. `0` logs every command; any negative value disables the slowlog. See the [note below](#--slowlog-log-slower-than). |
| `--replicaof` | string | *(empty)* | Upstream master address in `host:port` form. Setting it puts the server in replica mode. |
| `--masterauth` | string | *(empty)* | Password the replica uses to `AUTH` against a password-protected master. |
| `--requirepass` | string | *(empty)* | Password clients must supply via `AUTH`. Empty disables authentication. |
| `--event-loop` | bool | `false` | Serve all clients from one event-loop goroutine using OS readiness notifications. |

## Notes on individual flags

### `--host`

An IPv6 address can be written bare or in brackets, so `--host ::1` and `--host "[::1]"` are equivalent.

The default binds loopback deliberately. A server with no `--requirepass` is not reachable from the network unless you bind another interface. Doing so without a password exposes the datastore without authentication. See [Securing a server](../guides/securing-a-server.md).

### `--slowlog-log-slower-than`

| | |
| --- | --- |
| Type | integer **microseconds** |
| Default | `10000` (10ms) |

The unit is microseconds, not milliseconds. `0` logs every command; any negative value disables the slowlog entirely.

```bash
# Log commands slower than 5ms
go run ./cmd/stash --slowlog-log-slower-than 5000
```

### `--appendfsync`

| Value | Behavior |
| --- | --- |
| `always` | Fsync after every write. Strongest durability, slowest. |
| `everysec` | Fsync once per second. Default. |
| `no` | Leave flushing to the OS. |

The value is case-insensitive and trimmed. It only takes effect when `--aof` is also set.

### `--maxmemory`

Accounting is approximate keyspace accounting, not process RSS. When usage passes the limit, the store evicts sampled least-recently-used candidates. See [Memory limits and eviction](../guides/memory-and-eviction.md).

### `--event-loop`

Supported on Linux (`epoll`) and macOS (`kqueue`). Every other platform, including Windows, logs a warning at startup and falls back to goroutine-per-connection, so the flag is safe to set anywhere.

In event-loop mode, commands execute inline on the loop goroutine. `BLPOP` on an empty list and `WAIT` for pending replica acknowledgements return an explicit error instead of stalling every connection. Immediately satisfiable forms of both commands still succeed.

## Startup validation

The server exits with status `2` and writes to stderr when:

- `--maxmemory` is negative
- `--maxclients` is negative
- `--port` is outside `0`–`65535`
- `--appendfsync` is not `always`, `everysec`, or `no`
- `--slowlog-log-slower-than` is not an integer

`--replicaof` is validated when the replica connects, not at flag-parse time; it must be in `host:port` form with both parts non-empty.
