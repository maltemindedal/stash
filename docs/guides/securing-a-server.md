# Securing a server

By default, Stash binds to loopback and has no password. Configure authentication before making the server reachable from another machine.

## The default binding

`--host` defaults to `127.0.0.1`, so a server started with no flags accepts connections only from the local machine. Stash has no authentication until you set `--requirepass`. Binding a public interface without a password would expose the datastore without authentication, so Stash refuses to start in that configuration unless you pass `--allow-open-bind`. With the flag it starts and logs a warning.

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

A password on the command line is visible to every local user in the process list (`ps`). Keep it out of there by reading it from a file, mode `0600` and owned by the user that runs Stash:

```bash
go run ./cmd/stash --requirepass-file /etc/stash/password
```

For a replica, use `--masterauth-file` the same way. Startup fails if the file is empty or unreadable, and if you give both `--requirepass` and `--requirepass-file`.

Unauthenticated clients may still issue `PING`. Every other command, including the replication handshake, requires authentication first.

Authentication fails closed. Whether a client may run a command is decided by its own connection's Client state alone: on a server started with `--requirepass` every connection starts unauthenticated, and only a successful `AUTH` on that connection changes that. Should the password not reach the code that checks `AUTH`, clients are locked out (`AUTH` answers that no password is configured) rather than let in, and a client request that arrives without its Client state is refused. Besides a client's `AUTH` and `PING`, only two kinds of request run without authenticating: the writes a replica receives from its master and the commands replayed from the append-only file at startup, because they come from no client. One of those that carries a client's state is refused rather than let it borrow their rights.

Until a client authenticates it is also held to small frames, the same limits Redis applies: an array of at most 10 elements and a bulk string of at most 16 KiB. Anything larger is answered with one protocol error and the connection is closed, before the server has read the rest of the frame, so someone who can reach the port cannot make it buffer large requests. `AUTH` and `PING` fit easily. The limits end once the client authenticates, and apply only when `--requirepass` is set; the frame that follows an `AUTH` in the same pipeline is judged after the `AUTH` has run.

A client also has a limited time to authenticate. With `--requirepass` set, a connection that has not sent a successful `AUTH` within `--auth-timeout` (30 seconds by default) is closed, including one that is half way through sending its `AUTH`; `--auth-timeout 0` turns this off. Once a client has authenticated, an idle connection is left alone.

## Bind another interface

Only after setting a password:

```bash
go run ./cmd/stash --host 0.0.0.0 --port 6379 --requirepass "$STASH_PASSWORD"
```

An empty `--host` also binds all interfaces. Without `--requirepass` these addresses are refused (see [`--allow-open-bind`](../reference/configuration.md#--allow-open-bind)); a hostname counts as loopback only if it resolves to a loopback address.

## Authenticate replicas

A replica of a protected master needs `--masterauth` (or `--masterauth-file`) to complete its handshake:

```bash
go run ./cmd/stash --port 6380 --replicaof 127.0.0.1:6379 --masterauth "$STASH_PASSWORD"
```

See [Setting up replication](replication.md).

## Cap concurrent connections

`--maxclients` bounds concurrent client connections and defaults to `10000`. Setting it to `0` removes the limit.

```bash
go run ./cmd/stash --maxclients 1000
```

## File permissions

The append-only file and snapshots are created with mode `0600`. Parent directories that Stash has to create for them get `0750`, subject to the process umask; directories that already exist are left as they are.

## What the slowlog stores

`SLOWLOG` and `MONITOR` redact `AUTH` arguments, so passwords do not appear in the slow query log or in a monitor stream, including for failed attempts. `MONITOR` shows other command arguments verbatim; `SLOWLOG` shows the first 128 bytes of each of the first 31 tokens (the command name counts as one). Any other secret passed as an argument can appear in `SLOWLOG GET` output and `MONITOR` streams.

## Known limitations

These are properties of the current implementation, not configuration mistakes:

- **No TLS.** All traffic, including `AUTH` passwords, crosses the network in plaintext. Run Stash on a trusted network or behind a TLS-terminating proxy.
- **No ACLs or users.** A single shared password grants full command access.
- **No rename-command or command-level restrictions.** Any authenticated client can issue any implemented command, including `MONITOR` and `BGREWRITEAOF`.

## Related

- [Configuration reference](../reference/configuration.md)
- [Observability](observability.md) explains what `MONITOR` and `SLOWLOG` expose.

## Reporting a vulnerability

See [`SECURITY.md`](../../SECURITY.md) for how to report a vulnerability privately.
