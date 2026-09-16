# Stash context

Stash is a Go implementation of a Redis-compatible TCP key/value server. The project is intentionally implementation-led. Each feature should preserve Redis wire compatibility where supported while keeping package boundaries small and explicit.

## Domain glossary

- Stash: the server binary and codebase for the Redis-compatible store.
- RESP: the Redis Serialization Protocol used on client, persistence, and replication command streams.
- Command executor: the `internal/command` component that decodes RESP arrays, validates commands, applies connection state rules, executes handlers, and returns RESP values.
- Store: the `internal/storage` in-memory key/value engine.
- Shard: one fixed partition of the store guarded by a shard-local lock.
- Value kind: the logical Redis data type stored for a key: string, hash, list, set, sorted set, or stream.
- TTL: an expiry timestamp stored in Unix milliseconds and enforced by passive reads and active eviction. An active eviction is published as a `DEL` to replicas, the AOF, and `WATCH`; a passive one stays local to the server whose read noticed the deadline.
- AOF: append-only-file durability, stored as replayable RESP command frames.
- AOF rewrite: background compaction that snapshots live durable state into a replacement append-only file.
- RDB snapshot: Redis database file support used for startup loading and graceful-shutdown snapshots.
- Client state: per-connection state for authentication, transactions, pub/sub mode, monitor mode, and replication role.
- Connection state machine: the non-blocking per-connection component that buffers readable bytes, parses complete RESP requests, executes commands, and flushes buffered responses incrementally without a dedicated blocking goroutine.
- Event loop: the opt-in networking mode (`--event-loop`) that serves all client connections from one goroutine backed by OS readiness notifications (`epoll` on Linux, `kqueue` on macOS), dispatching readable and writable sockets through connection state machines.
- Transaction: Redis-style `MULTI` / `EXEC` command queueing with `WATCH`-based optimistic invalidation.
- Pub/sub registry: exact-channel subscription bookkeeping used by `SUBSCRIBE`, `UNSUBSCRIBE`, and `PUBLISH`.
- Replica: a downstream server connected through the supported replication handshake.
- Master: an upstream server that accepts replica handshakes and forwards propagated command frames.
- Slowlog: bounded in-memory command timing log used by `SLOWLOG`.
- Monitor: command-event fan-out mode used by `MONITOR`.
- Maxmemory: approximate keyspace memory pressure limit that triggers probabilistic LRU eviction.
- HyperLogLog: fixed-size approximate cardinality register set stored as a string value and used by `PFADD` and `PFCOUNT`.
- Geohash score: a 52-bit interleaved longitude/latitude encoding stored as a sorted-set score and used by `GEOADD`, `GEODIST`, and `GEORADIUS`.
- Score-range scan: the sorted-set storage operation that returns members whose scores fall in given intervals under a single lock acquisition. `GEORADIUS` uses it to scan only the geohash cells that cover a query radius instead of the whole set.

## Current capabilities

- RESP2-centric TCP server with one goroutine per client by default, an opt-in OS I/O multiplexing event loop, and graceful signal-driven shutdown.
- Thread-safe sharded in-memory storage for strings, hashes, lists, sets, sorted sets, streams, and bitmap and HyperLogLog operations over string values.
- Geospatial commands (`GEOADD`, `GEODIST`, `GEORADIUS`) backed by geohash scores in regular sorted sets, with `GEORADIUS` pruned to score-range scans of the geohash cells covering the query radius.
- TTL handling, append-only durability, startup RDB loading, shutdown RDB snapshots, and background AOF rewrite.
- Authentication, transactions, pub/sub, basic replication, memory-pressure eviction, slowlog, monitor, and `INFO` visibility.
- Unit, integration, race, vet, lint, and benchmark coverage are part of the expected verification workflow.

## Boundaries

- RESP3 support is limited to protocol-layer boolean/null parsing, encoding, and coercion; command behavior remains RESP2-centric.
- RDB loading supports string keys in DB `0` only. Startup fails instead of partially loading a file that selects another database, contains a non-string value, or uses an unsupported opcode. Snapshot *writing* behaves differently. It warns and skips unsupported keys because it can enumerate values that it cannot encode. The loader cannot skip a value whose encoding it cannot parse.
- Memory accounting is approximate keyspace accounting, not exact process RSS accounting.
- Replication supports `REPLCONF`, `PSYNC`, and `WAIT`, but it is not a complete Redis replication implementation.
- Redis compatibility is scoped to commands explicitly implemented in `internal/command`; unsupported modifiers should fail explicitly rather than being silently accepted.
- The event loop is opt-in and supported on Linux (`epoll`) and macOS (`kqueue`); other platforms, including Windows, fall back to goroutine-per-connection networking with a startup warning.
- In event-loop mode, commands execute inline on the loop goroutine, so a command that would block (`BLPOP` on an empty list, `WAIT` that must wait for replica acknowledgements) fails with an error instead of blocking; the immediately satisfiable forms still succeed.
- Event-loop connections cap buffered response and push output per connection and disconnect consumers that stop draining their socket, in place of the per-write deadlines the goroutine path uses.

## Documentation rules

- Use the glossary terms above when naming issues, tests, refactors, and architecture notes.
- Update this file when a new domain concept becomes stable in the codebase.
- Use `docs/architecture/overview.md` for package responsibility details and design rationale.
