# Stash verification map

The maintained source for verifying Stash's user-facing behavior. Read this index first, then use the feature file as the recipe.

## Baseline preconditions

- Go on `PATH` (1.21 or newer), `python3`, and for `gate` `golangci-lint` v2.13.2.
- `verify.sh doctor` exits 0.
- Only drive a server that this run launched (`server.pid` in its artifacts directory).

## Driving conventions

- Run every step through `.agents/skills/verify-stash/scripts/verify.sh`. Send ad-hoc commands with `scripts/resp.py`.
- Use one `STASH_VERIFY_RUN` id per verification.
- Prefix keys with `verify:`.

## Proof and skip reporting

- Proof is the command, the RESP reply, and for writes a second read or the file on disk.
- Report a SKIP as unverified.

## Feature entry contract

Each feature file has an H1, a paragraph, then `Sub-features`, `How to get to it (user POV)`, `Driving it with verify.sh`, `Gotchas`.

## Features

- [Test and gate](./test-and-gate.md) covers `go test ./...` and `scripts/gate.sh`.
- [Strings over RESP](./strings.md) covers PING, SET, GET, INCR and unknown commands.
- [AOF persistence](./aof-persistence.md) covers a write surviving a restart.
- [Event-loop mode](./event-loop.md) covers the same journey under `--event-loop`.
