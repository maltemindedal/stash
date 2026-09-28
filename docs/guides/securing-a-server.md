# Securing a server

By default, Stash binds to loopback and has no password. Configure authentication before making the server reachable from another machine.

## The default binding

`--host` defaults to `127.0.0.1`, so a server started with no flags accepts connections only from the local machine. Stash has no authentication until you set `--requirepass`. Binding a public interface without a password exposes the datastore without authentication.

## Require a password

Set `--requirepass` and clients must authenticate before issuing commands:

```bash
go run ./cmd/stash --port 6379 --requirepass "$STASH_PASSWORD"
```

```
127.0.0.1:6379> GET key
(error) NOAUTH Authentication required.
127.0.0.1:6379> AUTH secret
OK
127.0.0.1:6379> GET key
(nil)
```

`AUTH` takes exactly one argument, the password. Stash has no usernames or ACLs.

Unauthenticated clients may still issue `PING`. Every other command, including the replication handshake, requires authentication first.

## Bind another interface

Only after setting a password:

```bash
go run ./cmd/stash --host 0.0.0.0 --port 6379 --requirepass "$STASH_PASSWORD"
```

An empty `--host` also binds all interfaces.

## Authenticate replicas

A replica of a protected master needs `--masterauth` to complete its handshake:

```bash
go run ./cmd/stash --port 6380 --replicaof 127.0.0.1:6379 --masterauth "$STASH_PASSWORD"
```

See [Setting up replication](replication.md).

## Cap concurrent connections

`--maxclients` bounds concurrent client connections and defaults to `10000`. Setting it to `0` removes the limit.

```bash
go run ./cmd/stash --maxclients 1000
```

## What the slowlog stores

`SLOWLOG` and `MONITOR` redact `AUTH` arguments, so passwords do not appear in the slow query log or in a monitor stream, including for failed attempts. Both show other command arguments verbatim. Any other secret passed as an argument can appear in `SLOWLOG GET` output and `MONITOR` streams.

## Known limitations

These are properties of the current implementation, not configuration mistakes:

- **No TLS.** All traffic, including `AUTH` passwords, crosses the network in plaintext. Run Stash on a trusted network or behind a TLS-terminating proxy.
- **No ACLs or users.** A single shared password grants full command access.
- **No rename-command or command-level restrictions.** Any authenticated client can issue any implemented command, including `MONITOR` and `BGREWRITEAOF`.

## Related

- [Configuration reference](../reference/configuration.md)
- [Observability](observability.md) explains what `MONITOR` and `SLOWLOG` expose.
