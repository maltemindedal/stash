# Strings over RESP

A Redis client connects over TCP and reads and writes string keys. Unsupported commands fail explicitly with Redis's error text.

## Sub-features

- `ping` returns `PONG` for `PING`.
- `set-get` returns the stored value for `GET` after `SET`.
- `incr` returns 1 for `INCR` on a new key.
- `unknown` returns `ERR unknown command` for an unsupported command.

## How to get to it (user POV)

- Run `redis-cli -p <port>`, or any RESP client, against a running server.

## Driving it with verify.sh

Preconditions:

- `verify.sh launch` printed `PASS server ready`.

- **Drive.** Run `verify.sh drive`. Every reply matches, then `RESULT: PASS (drive)`.
- **Proof.** `ping`, `set`, `get`, `incr` and `unknown` files (`.cmd`, `.stdout`, `.stderr`, `.exit`) in the artifacts directory.

## Gotchas

- `drive` reuses the run's server. INCR returns 1 only on a fresh AOF, so run it once per run, or `stop` and use a new `STASH_VERIFY_RUN`.
