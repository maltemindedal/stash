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

Every successful mutating command is appended as a RESP frame. On the next startup, Stash replays the file before opening the listener, so no client can observe a partially restored keyspace. If the file ends in a command that was only partly written, as after a crash in the middle of an append, Stash logs a warning, cuts the file back to the last complete command, and carries on, so commands appended afterwards are never mistaken for the rest of the torn one. Invalid data anywhere before the end of the file is different: see [Repairing a corrupt append-only file](#repairing-a-corrupt-append-only-file).

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

Stash writes a snapshot only during a **graceful** shutdown triggered by `SIGINT` or `SIGTERM`. A second signal during shutdown ends the process at once, so it writes no snapshot either. A `SIGKILL` or crash produces no snapshot. Use `--aof` when writes since the last startup must survive either event.

Loading is stricter than writing. A snapshot write skips the keys it cannot encode and logs how many, because it can still enumerate them. A load cannot skip a value it cannot parse, so startup fails, rather than loading part of the file, when the file selects a database other than `0`, holds a non-string value, or uses an unsupported opcode.

## Repairing a corrupt append-only file

An unfinished command at the very end of the file is normal after a crash and is cut off automatically. Invalid data with more file behind it is not: replaying only what precedes it would silently drop every later command, and new commands would be appended after the damage. Stash therefore refuses to start and leaves the file untouched:

```
server: load aof "appendonly.aof": aof: "appendonly.aof" is corrupt: the command after 1204 complete commands, starting at byte 88213, is not valid RESP: protocol: line missing CRLF terminator; the file was not modified, see "Repairing a corrupt append-only file" in docs/guides/persistence.md
```

The first `N` bytes (88213 here) are 1204 intact commands. To start again from them:

```bash
cp appendonly.aof appendonly.aof.damaged   # keep the original
truncate -s 88213 appendonly.aof           # drop the damaged command and everything after it
```

That loses every write from the damaged command onward. If they matter, repair the copy by hand instead, or restore from a backup. Do not delete the file to get past the error: an empty or missing append-only file starts an empty server (or one loaded from `--rdb`).

## Watching for write failures

Under `everysec` and `no` a command is acknowledged before it reaches the disk, so a full or failing disk does not make the command fail. `INFO persistence` reports `aof_last_write_status:err` from the first failed write or fsync until one succeeds again; see [Observability](observability.md).

## What TTLs do across a restart

Relative expirations (`SET key value EX 60`) are rewritten to an absolute `PXAT` frame before being written to the AOF. Replay therefore anchors the TTL to the original clock rather than restarting the countdown, and keys that expired while the server was down are dropped instead of being resurrected with a fresh lease.

This also holds after `BGREWRITEAOF`, which writes each key with a TTL as `SET key value PXAT <deadline>`, the same absolute deadline the key had. A rewrite refuses, rather than drops, the TTL of any key that is not a string: it fails, logs the error, and keeps the old file. No command can give such a key a TTL today.

## Related

- [Configuration reference](../reference/configuration.md) lists flag defaults and validation rules.
- [Replication](replication.md) explains how replicas use the RDB snapshot during the handshake.
