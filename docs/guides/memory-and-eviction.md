# Memory limits and eviction

Stash has two independent eviction mechanisms: **TTL eviction**, which removes keys whose expiry has passed, and **memory-pressure eviction**, which removes live keys when the keyspace exceeds `--maxmemory`.

## TTL eviction

TTLs are stored internally as absolute Unix millisecond timestamps. Expired keys are removed two ways:

- **Passively**, when a read touches an expired key. The read behaves as though the key is gone.
- **Actively**, by a background loop that samples keys on a fixed interval.

Both are always on; there is no flag to disable them. With `--maxmemory` set there is a third: every accounted write re-measures the whole keyspace and drops the expired keys it passes, which reclaims them faster than sampling alone would.

```bash
# Sample 50 keys every 250ms instead of the default 20 every 100ms
go run ./cmd/stash --eviction-interval 250ms --eviction-sample-size 50
```

| Flag | Default | Effect |
| --- | --- | --- |
| `--eviction-interval` | `100ms` | Time between active eviction passes |
| `--eviction-sample-size` | `20` | Keys sampled per pass |

The active loop samples rather than scanning the full keyspace, so expired keys that are never read are removed probabilistically rather than immediately. A larger sample size reclaims memory sooner at the cost of more work per pass. A non-positive sample size falls back to the built-in default.

The server publishes removals from the background loop and the accounted-write recalculation like other writes. See [Evictions are replicated and made durable](#evictions-are-replicated-and-made-durable). It does not publish a passive removal because each server holding the key can read the same expiry timestamp.

## Memory-pressure eviction

Set `--maxmemory` to a byte count to enable approximate keyspace accounting and LRU eviction:

```bash
# 100 MB limit
go run ./cmd/stash --maxmemory 104857600
```

`0`, the default, disables the feature entirely and skips the accounting overhead. Negative values are rejected at startup.

When a write pushes the keyspace over the limit, the store evicts sampled least-recently-used candidates until usage is back at or below the limit. The server also enforces the limit once at startup after loading an AOF or RDB file. That pass logs:

```
level=INFO msg="applied startup maxmemory eviction" evicted_keys=42 used_memory=104857000 maxmemory=104857600
```

### Accounting is approximate

`used_memory` is the store's own estimate, built from per-entry overhead constants plus key and value byte lengths. It is **not** process RSS and will not match what the OS reports. Go runtime overhead, the RESP buffers, connection state, and the AOF write buffer are all outside this number.

Set `--maxmemory` below the process memory limit to leave room for Go runtime overhead, RESP buffers, connection state, and the AOF write buffer. Use `go_heap_sys` from `INFO memory` to monitor the Go heap.

### Watching it work

```bash
redis-cli -p 6379 INFO memory
```

```
used_memory:104857000
used_memory_human:100.00M
maxmemory:104857600
maxmemory_human:100.00M
```

`mem_fragmentation_ratio` in that output is a hardcoded placeholder, not a measurement. See [Observability](observability.md).

## Evictions are replicated and made durable

An eviction is a keyspace mutation, so it is not confined to the server that performed it. Whenever memory pressure or a TTL sweep removes keys, the server synthesises a `DEL` frame naming them and treats it like any other write:

- it is **propagated** to attached replicas, so a replica does not keep serving a key the master has dropped;
- it is **appended to the AOF**, so replaying the log does not resurrect the key;
- it **invalidates `WATCH`** on the evicted keys, so a transaction guarding one aborts.

A replica runs its own `--maxmemory` enforcement and active TTL loop, so it can drop keys without an instruction from the master. If an eviction occurs while the replica applies a replicated command, the replica records the eviction in its own AOF but does not send it upstream or to other replicas. An eviction triggered by the replica itself is a local write. Set `--maxmemory` no lower on a replica than on its master, or the replica may drop keys that the master still holds.

## Compact encodings

Small collections use compact internal encodings that store entries in a flat structure rather than a map, reducing per-key overhead for the common case:

| Type | Compact encoding | Promotes to a map when |
| --- | --- | --- |
| Hash | Flat entry slice | It outgrows the small-hash threshold |
| Sorted set | Flat entry slice | It outgrows the small-zset threshold |
| Set | Sorted `int64` slice (`IntSet`) | A non-integer member is added, or it passes 512 entries |

The 512-entry `IntSet` limit matches Redis' default for `set-max-intset-entries`. Beyond that size, O(n) inserts into a sorted slice cost more than map inserts. Promotion is automatic, one-way, and not configurable.

## Related

- [Configuration reference](../reference/configuration.md)
- [Observability](observability.md) explains how to read `INFO memory`.
- [Architecture overview](../architecture/overview.md) describes the sharding and lock strategy.
