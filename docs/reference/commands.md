# Command reference

Every command Stash implements. Anything not listed here is unsupported and returns an unknown-command error.

The `commandSpecs` table in [`internal/command/types.go`](../../internal/command/types.go) is the source of truth.

## Conventions

- Command names are case-insensitive on the wire.
- Type-specific commands return a Redis-style `WRONGTYPE` error when the key holds a different value kind.
- **Replicated** marks commands forwarded to connected replicas.
- **Durable** marks commands written to the append-only file when `--aof` is set.

## Protocol errors

A request that cannot be parsed is answered with one error, and the connection is closed, in both networking modes. The error text is Stash's own and differs from Redis's. Redis replies `ERR Protocol error: ` and its wording (`invalid bulk length`, `invalid multibulk length`, `too big mbulk count string`); Stash replies `ERR protocol: ` and a description of its own, such as `bulk string length 536870913 exceeds 536870912 byte limit`. Inside a command, the reply also names the element that failed, so an oversized third argument reads `ERR protocol: parse array element 2: protocol: bulk string length 536870913 exceeds 536870912 byte limit`. Do not match on the text; treat any `ERR protocol:` reply as the end of the connection.

What Stash refuses, as soon as the part of the request that shows it has arrived:

- A bulk string header that declares more than 512 MiB, or an array header of more than 1,048,576 elements.
- A line (a header, simple string or integer) of more than 65,536 bytes after its type byte, CRLF included, or one that has gone 65,536 bytes without an LF.
- Arrays nested more than 128 levels deep.
- On a server with `--requirepass`, before the client authenticates: an array of more than 10 elements or a bulk string of more than 16 KiB. The text ends `for a client that has not authenticated`; see [Securing a server](../guides/securing-a-server.md).
- An inline command, which Redis accepts and Stash does not: a request that does not start with a RESP type byte gets `ERR protocol: unsupported frame prefix "G"` (here for `GET key`).

`--event-loop` mode also bounds one request at 512 MiB of buffered input, headers and line endings included, and answers a larger one with `ERR protocol: frame exceeds 536870912 byte read-buffer limit`. A request can be within the 512 MiB bulk limit and still over this one: a lone bulk string header declaring 536,870,899 to 536,870,912 bytes is refused in that mode, and waits for its payload in the default mode, which bounds each bulk string and array but not the request as a whole.

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

`REPLCONF` supports the `LISTENING-PORT`, `GETACK`, and `ACK` subcommands. `WAIT` takes a replica count and a timeout in milliseconds, both non-negative; a timeout of `0` returns the current acknowledgement count immediately, and a timeout longer than about 292 years (the most a duration can hold) waits as long as that. On a password-protected master, replicas must authenticate before `REPLCONF` or `PSYNC`. `PSYNC` inside `MULTI` is refused when it is queued, with `ERR Command not allowed inside a transaction`, and `EXEC` then aborts the transaction, as in Redis.

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
