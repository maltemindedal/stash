# Getting started

This tutorial takes you from a fresh clone to a running Stash server with data in it. It should take under 15 minutes.

## Prerequisites

- **Go 1.21 or newer.** The minimum is the `go` directive in [`go.mod`](../go.mod). Check yours with `go version`.
- **A RESP client.** `redis-cli` is the easiest. Any RESP-capable TCP client works.

Stash needs no database or package downloads and has no `go.sum`.

## 1. Clone and build

```bash
git clone https://github.com/maltemindedal/stash.git
cd stash
go build ./cmd/stash
```

The build produces a `stash` binary (`stash.exe` on Windows) in the repository root.

## 2. Start the server

```bash
go run ./cmd/stash --port 6379
```

You should see structured log output confirming the listener is up:

```
time=2026-07-23T12:00:00.000+02:00 level=INFO msg="Stash listening" address=127.0.0.1:6379
```

The server binds `127.0.0.1` by default, so it is not reachable from the network until you set `--host` explicitly. Leave it running and open a second terminal.

## 3. Connect and store something

```bash
redis-cli -p 6379
```

Work through these to confirm the basics:

```
127.0.0.1:6379> PING
PONG
127.0.0.1:6379> SET name Stash
OK
127.0.0.1:6379> GET name
"Stash"
127.0.0.1:6379> INCR counter
(integer) 1
127.0.0.1:6379> INCR counter
(integer) 2
127.0.0.1:6379> DEL name
(integer) 1
```

## 4. Watch a key expire

```
127.0.0.1:6379> SET temp 1 PX 100
OK
```

Wait a moment, then read it back:

```
127.0.0.1:6379> GET temp
(nil)
```

TTLs are enforced both passively (on read) and actively (by a background loop running every `--eviction-interval`).

## 5. Try a few data types

```
127.0.0.1:6379> LPUSH tasks write-docs
(integer) 1
127.0.0.1:6379> RPUSH tasks ship-it
(integer) 2
127.0.0.1:6379> LRANGE tasks 0 -1
1) "write-docs"
2) "ship-it"
127.0.0.1:6379> HSET user:1 name Ada role engineer
(integer) 2
127.0.0.1:6379> HGETALL user:1
1) "name"
2) "Ada"
3) "role"
4) "engineer"
127.0.0.1:6379> ZADD scores 10 alice 20 bob
(integer) 2
127.0.0.1:6379> ZRANGE scores 0 -1 WITHSCORES
1) "alice"
2) "10"
3) "bob"
4) "20"
```

## 6. See how errors behave

Stash returns Redis-compatible error prefixes rather than silently accepting bad input:

```
127.0.0.1:6379> SET bad hello
OK
127.0.0.1:6379> INCR bad
(error) ERR value is not an integer or out of range
127.0.0.1:6379> LPUSH bad item
(error) WRONGTYPE Operation against a key holding the wrong kind of value
```

## 7. Shut down

Press `Ctrl+C` in the server terminal. The server stops accepting connections, drains active clients, and writes an RDB snapshot to `dump.rdb` (the `--dump` default) on the way out.

Restart it with that snapshot to confirm your data survived:

```bash
go run ./cmd/stash --port 6379 --rdb dump.rdb
```

> RDB support covers string keys only, but reading and writing differ in strictness. The shutdown snapshot exports string values and logs a warning for each key of another type that it skips. Startup restores string keys from DB `0`. It refuses to start if the file selects another database, contains a non-string value, or uses an unsupported opcode. The loader cannot skip an unparseable value without losing the rest of the stream, so it rejects an incompatible Redis dump instead of silently dropping data. For durability across all supported data types, use the [append-only file](guides/persistence.md).

## Where to go next

- [Persistence](guides/persistence.md) explains how to choose between AOF and RDB and configure durability.
- [Securing a server](guides/securing-a-server.md) explains what to do before binding outside loopback.
- [Configuration reference](reference/configuration.md) lists every flag and its default.
- [Command reference](reference/commands.md) lists every implemented command.
- [Architecture overview](architecture/overview.md) explains how the components fit together.
