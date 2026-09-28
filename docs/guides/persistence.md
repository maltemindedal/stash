# Persistence

Stash offers two persistence mechanisms with different coverage. Choose based on which data types you need to survive a restart.

| | Append-only file (AOF) | RDB snapshot |
| --- | --- | --- |
| Data types covered | All supported types | **String keys only** |
| Written | On every durable command | At graceful shutdown |
| Read | At startup | At startup, DB `0` only |
| Flags | `--aof`, `--appendfsync` | `--dump`, `--rdb` |

The AOF preserves all supported data types. RDB provides a fast string-only snapshot for startup and the replication handshake. It omits lists, hashes, sets, sorted sets, and streams, and logs the number of skipped keys.

## Enable the append-only file

```bash
go run ./cmd/stash --port 6379 --aof appendonly.aof --appendfsync everysec
```

Every successful mutating command is appended as a RESP frame. On the next startup, Stash replays the file before opening the listener, so no client can observe a partially restored keyspace. If the file ends in a command that was only partly written, as after a crash in the middle of an append, Stash logs a warning, cuts the file back to the last complete command, and carries on, so commands appended afterwards are never mistaken for the rest of the torn one.

## Choose a fsync policy

| `--appendfsync` | Behavior | Trade-off |
| --- | --- | --- |
| `always` | Fsync after every write | Strongest durability, slowest |
| `everysec` | Fsync once per second (default) | Loses at most ~1s of writes |
| `no` | Leave flushing to the OS | Fastest, weakest guarantee |

## Compact the file

The AOF grows without bound as commands accumulate. `BGREWRITEAOF` compacts it:

```
127.0.0.1:6379> BGREWRITEAOF
Background append only file rewriting started
```

The rewrite runs in the background. It snapshots live durable state, writes the smallest equivalent command stream, and atomically swaps the new file into place. Writes continue during the rewrite. A list, set, hash, or sorted set with more than 1,024 values is written as several commands, so the rewritten file stays loadable however large the collection grows.

## Understand the AOF/RDB precedence

When both `--aof` and `--rdb` are set and the AOF file exists and is non-empty, the AOF wins and RDB loading is skipped. The server logs this decision:

```
level=INFO msg="AOF detected, skipping RDB startup load" aof_path=appendonly.aof rdb_path=dump.rdb
```

This avoids replaying a stale snapshot over a newer command log.

## Configure RDB snapshots

A shutdown snapshot is written by default to `dump.rdb`:

```bash
# Write the snapshot elsewhere
go run ./cmd/stash --dump /var/lib/stash/dump.rdb

# Load a snapshot at startup
go run ./cmd/stash --rdb /var/lib/stash/dump.rdb

# Disable shutdown snapshots
go run ./cmd/stash --dump ""
```

Stash writes a snapshot only during a **graceful** shutdown triggered by `SIGINT` or `SIGTERM`. A `SIGKILL` or crash produces no snapshot. Use `--aof` when writes since the last startup must survive either event.

## What TTLs do across a restart

Relative expirations (`SET key value EX 60`) are rewritten to an absolute `PXAT` frame before being written to the AOF. Replay therefore anchors the TTL to the original clock rather than restarting the countdown, and keys that expired while the server was down are dropped instead of being resurrected with a fresh lease.

## Related

- [Configuration reference](../reference/configuration.md) lists flag defaults and validation rules.
- [Replication](replication.md) explains how replicas use the RDB snapshot during the handshake.
