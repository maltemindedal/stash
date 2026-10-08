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
The mode, entered with `MONITOR`, in which a client receives every request that clients send to the server.

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
The deadline after which a key expires.

**Eviction**:
The server removing keys on its own rather than at a client's request: a **TTL eviction** or a **Memory-pressure eviction**. Every eviction except a passive TTL eviction is published to **Replicas**, the **AOF**, and `WATCH`. The **Memory-pressure eviction** at startup runs before any client or **Replica** connects, so it is published to the **AOF** only.

**TTL eviction**:
The removal of a key whose **TTL** has passed: passive when a command touches the key, active when a sweep of expired keys removes it.

**Maxmemory**:
The approximate limit on keyspace memory above which **Memory-pressure eviction** runs.

**Memory-pressure eviction**:
The removal of live keys, sampled for the least recently used, while keyspace memory is over **Maxmemory**.
_Avoid_: LRU eviction, memory eviction

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

**AOF seed**:
The startup step that writes the keys an **RDB snapshot** loaded into a missing or empty **AOF**, the way an **AOF rewrite** writes its file, because every later start loads the **AOF** and skips the snapshot.

**RDB snapshot**:
A Redis database file holding the string keys of database `0`. Stash can load one at startup, writes one at graceful shutdown, and sends one to a **Replica** on full resync.

### Replication

**Master**:
A server that accepts **Replicas** and forwards to them the commands it propagates: most writes, and `PUBLISH`.
_Avoid_: leader

**Replica**:
A server that has completed the replication handshake with a **Master** and applies the writes it forwards.
_Avoid_: follower
