---
name: verify-stash
description: Drive Stash (Redis-compatible TCP key/value server in Go) the way a Redis client does and capture proof. It builds the server, runs go test and scripts/gate.sh, launches its own instance on a free loopback port, sends real RESP commands, and checks AOF persistence across a restart. Use before claiming a Stash change works.
---

# Verify Stash

Users touch Stash over TCP with RESP (`redis-cli` or any client). Contributors touch `go test` and `scripts/gate.sh`. This skill drives both, through one helper:

```sh
.agents/skills/verify-stash/scripts/verify.sh <doctor|build|test|gate|launch|drive|persist|stop|cleanup|all>
```

Each step prints `PASS`/`FAIL`/`SKIP` lines and ends with `RESULT: PASS`, `RESULT: PASS with SKIP` or `RESULT: FAIL`. The exit code is 0 unless something failed. A SKIP still means that step is unverified.

## Launch

`verify.sh launch [flags...]` builds `./cmd/stash` into the run's work dir. It then starts the server with `--host 127.0.0.1 --port 0 --dump "" --aof <work>/appendonly.aof`, from inside the work dir, so no `stash`, `dump.rdb` or `*.aof` lands in the repo. Extra flags are passed through, for example `launch --event-loop` or `launch --requirepass secret`. The server is ready when its log shows `msg="Stash listening" address=127.0.0.1:<port>` and `PING` returns `PONG`. The PID goes in `server.pid` and the address in `server.addr`. Each run has its own port and work dir, so runs never share an instance. `verify.sh stop` is the teardown.

## Doctor

`verify.sh doctor` is read-only. It prints the commit, the `go version`, and the `go.mod` floor (1.21, which CI builds on). It checks for `python3`, which the RESP client needs, and for `golangci-lint`, which `gate` needs. It also reports whether this run's server is up. Run it first.

## Drive

- `test` runs `go test ./...` and requires exit 0 with no `FAIL` lines.
- `gate` runs `scripts/gate.sh`, the repo's PR gate: gofmt, build, vet, test, lint, race and cross-OS vet on `GOTOOLCHAIN=go1.21.13`. It SKIPs if `golangci-lint` v2.13.2 is not on `PATH` or in `GOLANGCI_LINT`. It downloads the floor toolchain the first time.
- `drive` sends `PING`, `SET verify:greeting hello`, `GET verify:greeting`, `DEL verify:counter` then `INCR verify:counter`, and an unknown command over real TCP through `scripts/resp.py <host:port> <args...>`. It asserts the replies `PONG`, `OK`, `"hello"`, `(integer) 1` (the DEL makes this hold on a rerun against the same AOF) and `(error) ERR unknown command...`.
- `persist` writes a key, stops the server, checks that the AOF file on disk contains the key, restarts on the same AOF, and requires `GET` to return the value.
- `all` runs doctor, test, gate, launch, drive, stop, persist and cleanup. It stops after doctor if doctor fails.

For a one-off command, run `python3 .agents/skills/verify-stash/scripts/resp.py "$(cat <artifacts>/server.addr)" <args...>`.

Feature recipes are in [`features/README.md`](features/README.md).

## Evidence

Artifacts go to `$STASH_VERIFY_ARTIFACTS`, default `/tmp/stash-verify/<run-id>`. Set `STASH_VERIFY_RUN=<id>` to keep several steps in one directory. Each step writes `<name>.cmd`, `.stdout`, `.stderr` and `.exit`. The server log is `server.log`, and `persist` keeps `appendonly.aof.copy`. Drive the real binary over TCP, never internal packages. Record the reply and the side effect on disk.

## Cleanup

`verify.sh cleanup` stops only the PID in this run's `server.pid` (SIGTERM, then SIGKILL after 5 s), never anything by name. It removes the work dir (binary and AOF) and keeps every artifact.
