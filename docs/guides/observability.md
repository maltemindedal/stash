# Observability

Stash provides `INFO` for current statistics, `SLOWLOG` for slow command history, and `MONITOR` for a live command stream.

## Inspect server state with INFO

`INFO` accepts at most one section name. `default` and `all` return every section.

```bash
redis-cli -p 6379 INFO memory
```

| Section | Contents |
| --- | --- |
| `memory` | `used_memory`, `maxmemory`, Go heap stats, key counts per value kind |
| `replication` | `role`, replication IDs and offsets, connected replicas |
| `clients` | `connected_clients`, `monitoring_clients`, `total_commands_processed` |
| `persistence` | `aof_enabled`, `aof_last_write_status` |
| `default` / `all` | All of the above |

An unrecognized section name returns an error rather than an empty response.

### Reading the persistence section

```
# Persistence
aof_enabled:1
aof_last_write_status:ok
```

`aof_enabled` is `1` when `--aof` is set. `aof_last_write_status` is `err` from a failed write of the append-only file until a later write succeeds, and from a failed fsync until `BGREWRITEAOF` replaces the file. A `BGREWRITEAOF` whose old file fails to close and whose rename then fails also leaves it `err` until a later rewrite replaces the file, and a rewritten file whose directory cannot be synced leaves it `err` until a sync of the directory succeeds. It is `ok` otherwise (always `ok` without an AOF). [Watching for write failures](persistence.md#watching-for-write-failures) lists every case. Under `--appendfsync everysec` and `no` a command is acknowledged before it is on disk, so a full or failing disk does not fail the command; the failure appears only here and as a `WARN` in the log. Commands a failed write did not get into the file are kept in memory and written with a later write, and are lost if the process stops first. Alert on `err`. Under `--appendfsync always` a failed write or fsync is returned to the client as an error instead. The command stays applied in memory and is never sent to the replicas. It is also cut back out of the file, unless that cut fails too, in which case it stays in the file until a later cut succeeds. A later `BGREWRITEAOF` or a replica's full resync can still copy it from memory. After either failure that only `BGREWRITEAOF` clears, every write under `always` gets that error until a rewrite replaces the file, and while a rewritten file's directory cannot be synced every write gets it until a sync of the directory succeeds.

### Reading the memory section

```
# Memory
used_memory:1048576
used_memory_human:1.00M
maxmemory:104857600
maxmemory_human:100.00M
mem_fragmentation_ratio:1.00
go_heap_alloc:4194304
go_heap_sys:8388608
go_heap_idle:2097152
key_count:1024
key_count_string:900
key_count_list:50
key_count_hash:40
key_count_set:20
key_count_zset:10
key_count_stream:4
```

Two caveats:

- `used_memory` is **approximate keyspace accounting**, not process RSS. It tracks the store's own estimate of key and value sizes, so it will not match what the OS reports for the process.
- `mem_fragmentation_ratio` is a hardcoded placeholder of `1.00`. It is not measured.

Use `go_heap_alloc` and `go_heap_sys` for actual Go runtime memory.

## Find slow commands with SLOWLOG

Commands slower than `--slowlog-log-slower-than` are recorded in an in-memory ring buffer that holds 128 entries, matching Redis' default length.

The threshold is in **microseconds** and defaults to `10000` (10ms):

```bash
# Record commands slower than 1ms
go run ./cmd/stash --slowlog-log-slower-than 1000

# Record every command
go run ./cmd/stash --slowlog-log-slower-than 0

# Disable the slowlog
go run ./cmd/stash --slowlog-log-slower-than -1
```

Query it:

```
127.0.0.1:6379> SLOWLOG LEN
(integer) 3
127.0.0.1:6379> SLOWLOG GET 2
127.0.0.1:6379> SLOWLOG RESET
OK
```

`SLOWLOG GET` takes an optional non-negative count; without one it returns all buffered entries. Each entry is a six-element array, matching the Redis reply shape: an ID, a Unix timestamp, a duration in microseconds, the command arguments, the client address, and a client name. Stash does not implement `CLIENT SETNAME`, so the client-name field is always an empty string.

`AUTH` arguments are redacted before storage. Each entry keeps at most 32 tokens, the command name included, and at most 128 bytes of each token, as Redis does. A longer token is stored as its first 128 bytes followed by `... (N more bytes)`. A command with more than 32 tokens is stored as its first 31 followed by `... (N more arguments)`.

## Stream live commands with MONITOR

```bash
redis-cli -p 6379 MONITOR
```

The connection switches into monitoring mode and receives every request that clients send to the server, before it runs. A monitoring client may issue only `PING`. It must reconnect before running other commands.

Two operational notes:

- Monitor delivery uses a short write deadline. A monitor that stops draining its socket is disconnected rather than allowed to consume server memory.
- In `--event-loop` mode, buffered output is capped per connection and slow consumers are disconnected on that cap instead of on a per-write deadline.

`MONITOR` shows every command argument except `AUTH` passwords, which it redacts, so treat its output as sensitive.

## Server logs

Logging uses Go's `log/slog` text handler writing to stdout. Set the level with `--log-level` (`debug`, `info`, `warn`, `error`):

```bash
go run ./cmd/stash --log-level debug
```

At `info`, Stash logs listener startup, AOF and RDB load decisions, replica handshakes and disconnects, shutdown snapshots, and startup eviction.

## Related

- [Configuration reference](../reference/configuration.md)
- [Memory limits and eviction](memory-and-eviction.md)
- [Command reference](../reference/commands.md)
