# Configuration reference

Stash is configured entirely through command-line flags. There is no configuration file and no environment-variable support.

[`internal/config/config.go`](../../internal/config/config.go) is the source of truth.

## Flags

| Flag | Type | Default | Effect |
| --- | --- | --- | --- |
| `--host` | string | `127.0.0.1` | Interface the TCP listener binds to. An empty value binds all interfaces. Binding beyond loopback requires `--requirepass` or `--allow-open-bind`. |
| `--allow-open-bind` | bool | `false` | Allow listening on a non-loopback address with no `--requirepass`. Without it the server refuses to start in that configuration. |
| `--port` | int | `6379` | TCP port to listen on. Accepts `0`–`65535`; `0` asks the OS for an ephemeral port. |
| `--log-level` | string | `info` | Log level: `debug`, `info`, `warn`, or `error`. |
| `--eviction-interval` | duration | `100ms` | Interval between active TTL eviction passes. |
| `--eviction-sample-size` | int | `20` | Number of keys in each eviction sample. A pass samples again while over a quarter of the last sample had expired, for up to a quarter of `--eviction-interval`. |
| `--rdb` | string | *(empty)* | Path to an RDB file to load before the listener opens. Empty disables startup RDB loading. With `--aof`, it is loaded only while the AOF is missing or empty, and the keys it loads are then written into the AOF; see [AOF/RDB precedence](../guides/persistence.md#understand-the-aofrdb-precedence). |
| `--dump` | string | `dump.rdb` | Path to write an RDB snapshot to during graceful shutdown. |
| `--aof` | string | *(empty)* | Path to an append-only file for durable command logging. Empty disables AOF. When `--rdb` loads keys into a missing or empty AOF, the path must be a regular file or a symbolic link to one: `/dev/null`, or any other path that is not a regular file, then stops startup. See [AOF/RDB precedence](../guides/persistence.md#understand-the-aofrdb-precedence). |
| `--appendfsync` | string | `everysec` | Fsync policy: `always`, `everysec`, or `no`. Rejected at startup if the value is anything else. |
| `--maxmemory` | int64 | `0` | Approximate keyspace memory limit in bytes. `0` disables memory-pressure eviction. Negative values are rejected. |
| `--maxclients` | int | `10000` | Maximum concurrent client connections. `0` disables the limit. Negative values are rejected. |
| `--slowlog-log-slower-than` | int | `10000` | Slowlog threshold in **microseconds**. `0` logs every command; any negative value disables the slowlog. See the [note below](#--slowlog-log-slower-than). |
| `--replicaof` | string | *(empty)* | Upstream master address in `host:port` form. Setting it puts the server in replica mode. The replica reconnects, with a growing wait of 1 to 30 seconds, if the link drops. |
| `--masterauth` | string | *(empty)* | Password the replica uses to `AUTH` against a password-protected master. |
| `--masterauth-file` | string | *(empty)* | Read the `--masterauth` password from this file. See [the note below](#--requirepass-file-and---masterauth-file). |
| `--requirepass` | string | *(empty)* | Password clients must supply via `AUTH`. Empty disables authentication. |
| `--auth-timeout` | duration | `30s` | With `--requirepass`, how long a client may stay connected without authenticating before the server closes the connection. `0` disables the limit. |
| `--requirepass-file` | string | *(empty)* | Read the `--requirepass` password from this file. See [the note below](#--requirepass-file-and---masterauth-file). |
| `--event-loop` | bool | `false` | Serve all clients from one event-loop goroutine using OS readiness notifications. |

## Notes on individual flags

### `--host`

An IPv6 address can be written bare or in brackets, so `--host ::1` and `--host "[::1]"` are equivalent.

The default binds loopback deliberately. Binding any other address (including `0.0.0.0`, `::` and an empty host) without `--requirepass` is refused at startup, before any persistence is opened, unless you also pass `--allow-open-bind`. With that flag the server starts and logs a warning. See [Securing a server](../guides/securing-a-server.md).

### `--allow-open-bind`

Says that an unauthenticated server reachable from other machines is intended, for example on an isolated network or behind a firewall that does the access control. It has no effect when `--requirepass` is set or the server listens on loopback only.

### `--requirepass-file` and `--masterauth-file`

A password given as `--requirepass` or `--masterauth` is visible to every local user in the process list. These flags read it from a file instead, which suits a Docker or Kubernetes secret mount:

```bash
go run ./cmd/stash --requirepass-file /run/secrets/stash_password
```

The file holds the password and nothing else; the line ending that `echo` or an editor adds is dropped. Giving both the flag and its file is an error, and so is an empty or unreadable file: an empty password would mean "no authentication", so an empty secret mount fails startup instead of opening the server.

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
