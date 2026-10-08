# Command reference

Every command Stash implements. Anything not listed here is unsupported and returns an unknown-command error.

The `commandSpecs` table in [`internal/command/types.go`](../../internal/command/types.go) is the source of truth.

## Conventions

- Command names are case-insensitive on the wire.
- Type-specific commands return a Redis-style `WRONGTYPE` error when the key holds a different value kind.
- **Replicated** marks commands forwarded to connected replicas.
- **Durable** marks commands written to the append-only file when `--aof` is set.

## Connection and authentication

| Command | Replicated | Durable |
| --- | --- | --- |
| `PING [message]` | – | – |
| `AUTH <password>` | – | – |
| `ECHO <message>` | – | – |

`PING` accepts at most one payload and returns it as a bulk string. Unauthenticated clients on a password-protected server may still use `PING`.

`SLOWLOG` redacts `AUTH` arguments and truncates long commands (32 tokens of 128 bytes each) before storing command metadata.

## Strings

| Command | Replicated | Durable |
| --- | --- | --- |
| `SET <key> <value> [EX seconds \| PX milliseconds \| PXAT unix-ms]` | yes | yes |
| `GET <key>` | – | – |
| `DEL <key> [key ...]` | yes | yes |
| `INCR <key>` | yes | yes |

- Expiration takes exactly one option/value pair or none. The value must be a positive integer; `0` and negatives return an invalid-expire-time error, as does an `EX` or `PX` whose deadline does not fit in a 64-bit Unix-millisecond timestamp, and an unrecognized option returns a syntax error.
- Relative `EX`/`PX` expirations are rewritten to an absolute `PXAT` frame before replication and AOF logging, so replicas and AOF replay anchor the TTL to the master's clock instead of restarting it.
- `GET` on a missing key returns a null bulk string.
- `DEL` ignores missing keys and returns the number of keys removed.
- `INCR` initializes a missing key to `1` and operates on base-10 signed 64-bit integer strings.

## Bitmaps

| Command | Replicated | Durable |
| --- | --- | --- |
| `SETBIT <key> <offset> <0\|1>` | yes | yes |
| `GETBIT <key> <offset>` | – | – |
| `BITCOUNT <key> [start end]` | – | – |

Bitmap commands operate on string values and enforce Redis-compatible offsets up to `2^32 - 1`. `BITCOUNT` takes either no range or both range bounds.

## HyperLogLog

| Command | Replicated | Durable |
| --- | --- | --- |
| `PFADD <key> [element ...]` | yes | yes |
| `PFCOUNT <key> [key ...]` | – | – |

Registers are a fixed-size approximate cardinality structure stored as a string value.

## Hashes

| Command | Replicated | Durable |
| --- | --- | --- |
| `HSET <key> <field> <value> [field value ...]` | yes | yes |
| `HGET <key> <field>` | – | – |
| `HDEL <key> <field> [field ...]` | yes | yes |
| `HGETALL <key>` | – | – |

`HSET` requires an odd total argument count (key plus field/value pairs). Small hashes use a compact internal encoding.

## Lists

| Command | Replicated | Durable |
| --- | --- | --- |
| `LPUSH <key> <element> [element ...]` | yes | yes |
| `RPUSH <key> <element> [element ...]` | yes | yes |
| `LPOP <key> [count]` | yes | yes |
| `RPOP <key> [count]` | yes | yes |
| `LRANGE <key> <start> <stop>` | – | – |
| `BLPOP <key>` | yes (as `LPOP`) | yes (as `LPOP`) |

- `LPOP`/`RPOP` accept an optional count, which must be non-negative; a negative count returns `ERR value is out of range, must be positive`.
- `BLPOP` takes a key and **no timeout argument**. Redis requires the timeout. A push of n elements serves up to n clients blocked on the key, longest-waiting first, as in Redis: each served client's pop wakes the next while elements remain. Unlike Redis, which serves them before it runs the next command, another client's command can run between two of those pops: an `LPOP` there takes an element ahead of the next blocked client. In `--event-loop` mode, a `BLPOP` that must block returns an error rather than waiting. While a `BLPOP` waits, the server checks every 100 ms that the client is still connected (on Linux, macOS and FreeBSD), and drops it if the client has closed or half-closed its side. It checks again just before the pop when the client is woken, whether by a push or by the pop of the client served before it: a client that has gone pops nothing, and the element goes to the next waiting client or stays in the list, so no element is consumed on behalf of a client that is gone. A client that half-closes its sending side right after sending `BLPOP` and still expects the reply no longer gets it; keep the connection open until the reply arrives, as Redis requires.

## Sets

| Command | Replicated | Durable |
| --- | --- | --- |
| `SADD <key> <member> [member ...]` | yes | yes |
| `SISMEMBER <key> <member>` | – | – |
| `SREM <key> <member> [member ...]` | yes | yes |
| `SMEMBERS <key>` | – | – |

Integer-only sets use a compact sorted-slice encoding until they reach 512 members or gain a non-integer member.

## Sorted sets

| Command | Replicated | Durable |
| --- | --- | --- |
| `ZADD <key> <score> <member> [score member ...]` | yes | yes |
| `ZRANGE <key> <start> <stop> [WITHSCORES]` | – | – |

`WITHSCORES` is the only accepted modifier; anything else returns a syntax error. Small sorted sets use a compact internal encoding.

## Geospatial

| Command | Replicated | Durable |
| --- | --- | --- |
| `GEOADD <key> <longitude> <latitude> <member> [longitude latitude member ...]` | yes | yes |
| `GEODIST <key> <member1> <member2> [m\|km\|ft\|mi]` | – | – |
| `GEORADIUS <key> <longitude> <latitude> <radius> <m\|km\|ft\|mi>` | – | – |

Positions are stored as 52-bit interleaved geohash scores in a regular sorted set, so geospatial keys are readable with `ZRANGE`. `GEODIST` defaults to metres when no unit is given. `GEORADIUS` takes exactly five arguments. Optional Redis modifiers such as `WITHCOORD`, `COUNT`, and `ASC` are not supported and return a syntax error.

## Streams

| Command | Replicated | Durable |
| --- | --- | --- |
| `XADD <key> <id\|*> <field> <value> [field value ...]` | – | yes |
| `XREAD STREAMS <key> <id>` | – | – |

`XADD` is written to the AOF but not forwarded to replicas. An auto-generated ID (`*`) is written to the AOF as the ID that was generated, so entries keep the IDs clients were given across a restart. `XREAD` supports exactly one key and one ID, and requires the literal `STREAMS` keyword first; `BLOCK` and `COUNT` are not supported.

## Transactions

| Command | Replicated | Durable |
| --- | --- | --- |
| `MULTI` | – | – |
| `EXEC` | – | – |
| `DISCARD` | – | – |
| `WATCH <key> [key ...]` | – | – |

Queued commands propagate and persist individually when `EXEC` runs them. `WATCH` provides optimistic invalidation. If a watched key changes before `EXEC`, the transaction aborts.

`EXEC` runs alone: no other client's command executes while it does, so a watched key cannot change between `EXEC`'s check and its queued commands, and no other client can observe the transaction half way through. Because of that, a blocking command queued in a transaction does not wait: `BLPOP` on an empty list returns a null array, and `WAIT` returns the number of replicas that have acknowledged so far, as in Redis.

## Pub/sub

| Command | Replicated | Durable |
| --- | --- | --- |
| `SUBSCRIBE <channel> [channel ...]` | – | – |
| `UNSUBSCRIBE [channel ...]` | – | – |
| `PUBLISH <channel> <message>` | yes | – |

Subscriptions match exact channel names; there is no pattern subscription (`PSUBSCRIBE`). Empty channel names return a syntax error. A subscribed client may only issue `PING`, `SUBSCRIBE`, and `UNSUBSCRIBE`.

`PUBLISH` is forwarded to replicas but never written to the AOF, since it mutates no durable state.

## Replication

| Command | Replicated | Durable |
| --- | --- | --- |
| `REPLCONF <subcommand> <arg>` | – | – |
| `PSYNC ? -1` | – | – |
| `WAIT <numreplicas> <timeout>` | – | – |

`REPLCONF` supports the `LISTENING-PORT`, `GETACK`, and `ACK` subcommands. `WAIT` takes a replica count and a timeout in milliseconds, both non-negative; a timeout of `0` returns the current acknowledgement count immediately, and a timeout longer than about 292 years (the most a duration can hold) waits as long as that. On a password-protected master, replicas must authenticate before `REPLCONF` or `PSYNC`.

See [Setting up replication](../guides/replication.md).

## Persistence and observability

| Command | Replicated | Durable |
| --- | --- | --- |
| `BGREWRITEAOF` | – | – |
| `INFO [default\|all\|memory\|replication\|clients\|persistence]` | – | – |
| `SLOWLOG GET [count]` | – | – |
| `SLOWLOG LEN` | – | – |
| `SLOWLOG RESET` | – | – |
| `MONITOR` | – | – |

`INFO` accepts at most one section name; an unrecognized section returns an error. A monitoring client may only issue `PING`.

See [Observability](../guides/observability.md) and [Persistence](../guides/persistence.md).
