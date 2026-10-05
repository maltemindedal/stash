# Stash

Stash is a Redis-compatible TCP key/value server written in Go. It is implementation-led: each feature keeps Redis wire compatibility where Stash supports it.

## Language

### Protocol and commands

**RESP**:
The Redis Serialization Protocol. It carries client requests and replies, and the command streams written to the **AOF** and sent to **Replicas**.

**Command executor**:
The component that validates each request against the **Client state**, runs it, and returns its RESP reply.

**Client state**:
What the server knows about one connection: whether it has authenticated, and whether it is in a **Transaction**, pub/sub mode, **Monitor** mode, or a replication role.

**Transaction**:
A `MULTI` / `EXEC` block of queued commands that runs as one unit, and is aborted if a key the client `WATCH`ed changes before `EXEC`.

**Pub/sub registry**:
The record of which clients subscribe to which exact channels, used by `SUBSCRIBE`, `UNSUBSCRIBE`, and `PUBLISH`.

**Monitor**:
The mode, entered with `MONITOR`, in which a client receives every command the server runs.

**Slowlog**:
The bounded in-memory log of commands that ran longer than a threshold, read with `SLOWLOG`.

### Networking

**Event loop**:
The opt-in networking mode in which one loop serves every client connection from OS readiness notifications, instead of a goroutine for each.

**Connection state machine**:
One client connection as the **Event loop** drives it: its unparsed input and unsent replies, so the connection needs no goroutine of its own.

### Storage

**Store**:
The in-memory key/value engine that holds every key and its value.

**Shard**:
One of the fixed partitions the **Store** divides keys between, each with its own lock.

**Value kind**:
The Redis data type of a key's value: string, hash, list, set, sorted set, or stream.

**TTL**:
The deadline after which a key expires. An expired key is removed passively, when a read finds it, or actively, by a sweep of expired keys; only active removals are published to **Replicas**, the **AOF**, and `WATCH`.

**Maxmemory**:
The approximate limit on keyspace memory past which the **Store** evicts keys, sampling for the least recently used.

**HyperLogLog**:
A fixed-size approximate cardinality structure stored as a string value, used by `PFADD` and `PFCOUNT`.

**Geohash score**:
A longitude/latitude position encoded as a sorted-set score, used by `GEOADD`, `GEODIST`, and `GEORADIUS`.

**Score-range scan**:
A single consistent read of the members of a sorted set whose scores fall within given intervals.

### Persistence

**AOF**:
The append-only file: the durable log of writes, stored as replayable RESP commands.

**AOF rewrite**:
The background compaction that replaces the **AOF** with a file recreating only the current live data.

**RDB snapshot**:
A Redis database file of the keyspace, loaded at startup, written at graceful shutdown, and sent to a **Replica** on full resync.

### Replication

**Master**:
A server that accepts **Replicas** and forwards the writes it applies to them.
_Avoid_: leader

**Replica**:
A server that has completed the replication handshake with a **Master** and applies the writes it forwards.
_Avoid_: follower
