# Setting up replication

Stash supports master/replica replication over the standard Redis handshake (`REPLCONF`, `PSYNC`). A replica connects to a master, receives an RDB snapshot of current state, then applies a live stream of propagated commands. As in Redis, a full resync *replaces* the replica's dataset: whatever the replica loaded from its own AOF or RDB is discarded in favour of the master's snapshot.

> Replication supports `REPLCONF`, `PSYNC`, and `WAIT`. It does not support partial resynchronization, replica chaining, or automatic failover.

> The full-resync snapshot uses the same RDB encoder as shutdown snapshots, so it carries only string keys from DB `0`. The handshake does not transfer existing hashes, lists, sets, sorted sets, or streams. The master logs how many keys it skipped. Writes propagated after the handshake cover every supported type. To synchronize collection keys, write them after the replica attaches.

## Start a master and a replica

Run the master on the default port:

```bash
go run ./cmd/stash --port 6379
```

In a second terminal, start a replica pointed at it:

```bash
go run ./cmd/stash --port 6380 --replicaof 127.0.0.1:6379
```

The replica logs a completed handshake:

```
level=INFO msg="replica handshake completed" master_addr=127.0.0.1:6379 listening_port=6380
```

## Verify it works

Write to the master:

```bash
redis-cli -p 6379 SET greeting hello
```

Read from the replica:

```bash
redis-cli -p 6380 GET greeting
```

Check roles and connected replicas with `INFO replication`:

```bash
redis-cli -p 6379 INFO replication
```

```
# Replication
role:master
master_replid:...
master_repl_offset:31
slave_repl_offset:0
connected_slaves:1
slave0:id=1,port=6380,offset=31
```

> TODO: The field names above come from `appendInfoReplication` in `internal/command/info.go`, but the values are examples rather than output from a live master and replica. Run the two-server setup above and replace this block with its output.

## How the stream reaches a replica, and when a replica is dropped

Writes on the master are queued for each replica and sent by a goroutine of that replica's own, in the order of their replication offsets, which `WAIT` relies on, and several at a time. A slow or stalled replica therefore never holds up clients writing to the master, and one healthy replica costs the master little (with one replica attached, a benchmark of pipelined `SET` went from about 127,000 to about 220,000 operations per second on the same machine).

A replica's queue starts when the master copies the snapshot for its full resync, so a write made while the snapshot is still being sent is queued and sent right after it: every write reaches the replica exactly once, in the snapshot or in the stream. Writes on the master wait while it copies the snapshot; reads keep running then, but pause briefly before the copy starts, while the master waits for the writes already in progress, as they do for `EXEC`. Encoding and sending the snapshot come after. With `--event-loop`, commands run one at a time on the loop, so every client also waits for the encoding. The replica counts in `INFO replication`'s `connected_slaves` from that moment, as in Redis.

The queue for a replica is limited to 256 MiB, including the writes queued while its snapshot is sent, as Redis counts a replica's output buffer, and a replica that will not accept 1 MiB of the stream within 30 seconds is considered stalled. In either case the master logs `dropping a replica` and closes the connection; a replica dropped while its snapshot is still being sent is closed once the snapshot has gone. With `--event-loop`, the connection's own 256 MiB limit on output waiting for a replica also counts the part of the snapshot reply that has not reached the socket yet, so a replica whose snapshot is 256 MiB or more is dropped as soon as the first write is sent to it. On a graceful shutdown the master gives the replicas up to a second to take what is queued before it closes their sockets.

## When the link to the master drops

A replica that loses its master, or cannot reach it at startup, waits one second and tries again, doubling the wait up to 30 seconds after each failure and starting over once a link has completed its handshake. Every attempt begins with a full resynchronisation, which replaces the replica's dataset with the master's snapshot, so a reconnected replica never keeps data from before the drop. Only an invalid `--replicaof` address or the server stopping stops the retries, including when the server stops because it can no longer accept connections.

## Replicate against a protected master

A password-protected master requires the replica to authenticate before `REPLCONF` or `PSYNC`. Supply the password with `--masterauth` (or keep it out of the process list with `--masterauth-file`, see [Securing a server](securing-a-server.md)):

```bash
# Master
go run ./cmd/stash --port 6379 --requirepass secret

# Replica
go run ./cmd/stash --port 6380 --replicaof 127.0.0.1:6379 --masterauth secret
```

An unauthenticated replica handshake against a protected master is rejected.

## Wait for acknowledgements

`WAIT` blocks until a given number of replicas have acknowledged the connection's last write, or until a timeout in milliseconds elapses:

```
127.0.0.1:6379> SET k v
OK
127.0.0.1:6379> WAIT 1 1000
(integer) 1
```

The reply is the number of replicas that acknowledged, which may be lower than requested if the timeout fires first. A timeout of `0` returns the current count immediately without waiting. A replica that attached after earlier writes counts once it has acknowledged everything the master sent it since.

In `--event-loop` mode, `WAIT` returns an error if it would have to block. Commands execute inline on the loop goroutine, so waiting would stall every connection. The command still succeeds when enough replicas have already acknowledged the write or when the timeout is `0`.

## What gets replicated

Commands that mutate state are forwarded to replicas; read commands are not. Two cases are deliberately asymmetric:

- **`PUBLISH` is replicated but not persisted.** It mutates no durable state, but subscribers connected to a replica should still receive the message.
- **`XADD` is persisted but not replicated.** Stream writes reach the AOF but are not currently forwarded.

The full per-command breakdown is in the [command reference](../reference/commands.md).

## Related

- [Configuration reference](../reference/configuration.md) documents `--replicaof`, `--masterauth`, and `--requirepass`.
- [Securing a server](securing-a-server.md)
- [Architecture overview](../architecture/overview.md) explains why persistence and replication use separate paths.
