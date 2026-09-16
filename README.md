# Stash

A Redis-compatible key-value store written from scratch in Go, for people who want to read a database implementation rather than depend on one.

Stash speaks RESP over TCP, so `redis-cli` and other RESP clients connect without modification. It implements strings, bitmaps, HyperLogLog, hashes, lists, sets, sorted sets, geospatial queries, and streams on a sharded in-memory store. It also supports append-only-file durability, RDB snapshots, leader/follower replication, transactions, and pub/sub. The entire server builds from the Go standard library, with no external dependencies.

## Quick start

You need Go 1.21 or newer.

```bash
git clone https://github.com/maltemindedal/stash.git
cd stash
go run ./cmd/stash --port 6379
```

```
time=2026-07-23T12:00:00.000+02:00 level=INFO msg="Stash listening" address=127.0.0.1:6379
```

The server binds `127.0.0.1` by default and requires no password, so it is not reachable from the network until you set `--host`. Read [Securing a server](docs/guides/securing-a-server.md) before you do.

## Usage

Connect with `redis-cli` or any RESP-capable client:

```
$ redis-cli -p 6379
127.0.0.1:6379> SET name Stash
OK
127.0.0.1:6379> GET name
"Stash"
127.0.0.1:6379> SET session token EX 60
OK
127.0.0.1:6379> ZADD scores 10 alice 20 bob
(integer) 2
127.0.0.1:6379> ZRANGE scores 0 -1 WITHSCORES
1) "alice"
2) "10"
3) "bob"
4) "20"
```

Run with durability and a password:

```bash
go run ./cmd/stash --port 6379 \
  --aof appendonly.aof --appendfsync everysec \
  --requirepass "$STASH_PASSWORD"
```

## Documentation

| | |
| --- | --- |
| [Getting started](docs/getting-started.md) | Zero to a running server with data in it |
| [Guides](docs/README.md#how-to-guides) | Persistence, replication, security, memory, observability |
| [Configuration](docs/reference/configuration.md) | Every flag, default, and validation rule |
| [Commands](docs/reference/commands.md) | Every implemented command |
| [Architecture](docs/architecture/overview.md) | How the packages fit together, and why |

The full index is at [`docs/README.md`](docs/README.md).

## Project structure

```
cmd/stash/         application entrypoint
internal/
  config/           runtime configuration and flag parsing
  logger/           shared log/slog setup
  protocol/         RESP parsing and encoding
  storage/          sharded key/value store, TTL and memory eviction
  command/          command decoding, validation, and dispatch
  server/           TCP listener, connection lifecycle, replication
  aof/              append-only-file replay, writing, and rewrite
  rdb/              RDB loading and snapshot writing
test/               end-to-end integration tests over real TCP
docs/               project documentation
```

## Current boundaries

Stash is an implementation exercise, not a production datastore. The notable gaps:

- **No TLS and no ACLs.** A single `--requirepass` password, sent in plaintext.
- **RESP2-centric.** RESP3 support exists in the protocol layer (booleans, nulls) but command behavior is RESP2.
- **RDB covers string keys only**, in both directions. Use `--aof` for full-fidelity durability.
- **Replication is partial.** Stash supports `REPLCONF`, `PSYNC`, and `WAIT`, but not partial resync, chaining, or failover.
- **`--maxmemory` uses approximate keyspace accounting**, not process RSS.
- **The `--event-loop` mode is opt-in** and supported on Linux (`epoll`) and macOS (`kqueue`); other platforms fall back to goroutine-per-connection with a startup warning.

Per-command deviations from Redis are noted in the [command reference](docs/reference/commands.md).

## Contributing

See [`docs/contributing.md`](docs/contributing.md) for dev setup and the checks CI runs.

## License

[MIT](LICENSE).
