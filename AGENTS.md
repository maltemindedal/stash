# AGENTS.md

Stash is a Redis-compatible TCP key/value server built from the Go standard library alone, written to be read. A change has to keep Redis wire behavior where a command is supported and fail explicitly where it is not, keep the AOF and replicas in the order writes were applied, stay available under hostile clients, and leave the docs matching the code.

## Commands

The pre-PR gate mirrors `.github/workflows/ci.yml`. Run all of it from the repo root:

```bash
gofmt -s -l .                        # must print nothing; fix with gofmt -s -w .
go build -o /dev/null ./cmd/stash    # CI's plain `go build ./cmd/stash` leaves a ./stash binary
go vet ./...
go test ./...
golangci-lint run                    # the version pinned in ci.yml
go test -race -shuffle=on ./...
for t in darwin/amd64 darwin/arm64 windows/amd64; do CGO_ENABLED=0 GOOS=${t%/*} GOARCH=${t#*/} go vet ./...; done
```

- CI builds, tests and races on the floor in `go.mod` (Go 1.21). `go build` on a newer toolchain accepts calls to post-1.21 standard-library APIs; `go vet` reports them and CI's floor build rejects them. Run the gate on the floor by prefixing the `go` commands with `GOTOOLCHAIN=go1.21.13` (it downloads that toolchain).
- No Go on the machine: run the same commands in Docker, `docker run --rm -v "$PWD":/src -w /src golang:1.21 sh -c 'go vet ./... && go test ./...'`, and lint with `docker run --rm -v "$PWD":/src -w /src golangci/golangci-lint:v2.13.2 golangci-lint run`. When a check could not run, say so in the commit body and the PR.
- Single test: `go test ./internal/storage -run '^TestStoreActiveEvictionReportsExpiredKeys$' -count=1 -v`. Integration tests are the `./test` package: `go test ./test -run '^TestMasterFullResyncTransfersExistingKeyspace$' -count=1 -v`.
- A failed race run prints its shuffle seed; replay it with `go test -race -shuffle=<seed> ./...`.
- Run the server with `go run ./cmd/stash --port 6379 --dump ""`. The default `--dump dump.rdb` writes `dump.rdb` into the working directory on graceful shutdown, and `--aof <path>` creates its file wherever it points, so keep AOF paths under a temp dir.

## Conventions

- Standard library only, in code and tests: lint (`depguard` in `.golangci.yml`) fails any import outside the standard library and this module. `go.mod` has no `require` block and there is no `go.sum`; lint does not check that, review does. Reach for `syscall` and the standard library instead, as the epoll/kqueue pollers do (PR #36 rejected `golang.org/x/sys`).
- Write Go 1.21. Loop variables are shared across iterations even on new toolchains, so copy one before a closure or goroutine captures it, as `newSequencer` in `internal/command/sequencer.go` does. Raising the floor is its own change, decided by the owner, with no `toolchain` line.
- Skills are installed globally: keep `.agents/skills/` and `skills-lock.json` out of the repo (removed twice, last in f6e2ace). The per-repo settings skills read are `## Agent skills` below and `docs/agents/`.
- Unsupported input fails explicitly: an unrecognized modifier returns `ERR syntax error`. Wire errors use Redis's exact text through the `Err*` sentinels and `newRESPError` in `internal/command/executor.go`. Go errors start with their package (`"aof: ..."`, `"server: ..."`).
- Storage operations go through `readKey`/`writeKey` in `internal/storage/shard.go`, which own locking, expiry and memory accounting. A callback keeps no reference to the value or its memory after it returns: returning internal slices and mutating shared buffers were repeat review findings.
- Before changing any lock, read the header of `internal/command/sequencer.go` and the `readKey`/`writeKey` docs. Locks are taken in the order sequencer gate, write stripes ascending, shards ascending. Nothing waits for another request while holding the gate or a stripe, and no shard lock is held while calling an expiry listener or taking `waiters.mu`.
- Every feature works in both networking modes: goroutine-per-connection (default) and `--event-loop` (Linux and macOS). Event-loop commands run inline on the loop goroutine, so anything that would block returns an error there instead.
- A protocol change touches both RESP decoders: the streaming `Parser` (`internal/protocol/parser.go`, default mode) and the incremental `Decode`/`Decoder` (`decode.go`, event loop). They share `limits.go` and `grammar.go`. A command allowed before AUTH must fit `UnauthenticatedLimits`.

### Adding a command

1. Register it in `commandSpecs` in `internal/command/types.go`. Set `validate` (a test requires it; handlers then skip their own arity checks). For a write, set `propagates`/`durable` and `keys`: left unset, the write is ordered against every other write. Set `rewriteFrame` when the logged frame must be deterministic (SET's relative EX becomes PXAT, XADD `*` becomes the generated ID).
2. Put the handler in its family file (`list.go`, `geo.go`, `hashset.go`, ..., otherwise `commands.go`) and the storage operation in `internal/storage/<kind>.go`.
3. A blocking command is classified by name in `sequenceModeFor` (`sequencer.go`) and returns `blockingNotSupportedError` in event-loop mode, as BLPOP and WAIT do.
4. A new value kind also needs a case in `internal/aof/rewrite.go` (the default case errors), `approximateValueObjectSize` in `storage/memory.go`, `snapshotAllLocked` in `storage/store.go`, `storage/stats.go`, and `command/info.go`.
5. Add the row to `docs/reference/commands.md`; a new family also goes in README.md's feature sentence and the `internal/command` section of `docs/architecture/overview.md`.

### Tests

- Name tests as behavior sentences (`TestConnectionsThatNeverAuthenticateAreClosed`), table-driven with `t.Run`, without `t.Parallel`.
- Unit tests sit beside their package. Wire-visible behavior also gets a test in `test/`, which starts the server in process on `127.0.0.1:0` from `defaultTestConfig()` and checks hand-written Redis replies through the repo's own parser (`assertCommandResponse` in `test/integration_test.go`). Event-loop tests that need a poller carry `//go:build linux || darwin`.
- A bug fix comes with a test that fails on the old code; say so in the commit body.
- Assert timing with ratios, and wait for the state you expect before asserting on it: a wall-clock bound and an early assertion both flaked (33c9517, d762cbd).

### Commits and PRs

- Subject: imperative, sentence case, no `type:` prefix, no trailing period, stating the behavior change ("Refuse to start on an append-only file that is corrupt before its end"). Body: why, and how it was verified, citing other commits by short hash.
- One atomic commit per change, each green on its own. A pure `git mv` gets its own commit so `git log --follow` keeps history.
- Fill in `.github/pull_request_template.md`, and add what you verified and what you deliberately left out.

## Gotchas

- `stash`, `dump.rdb` and `*.aof` are not gitignored. Delete any you created before committing, so `git status --porcelain` shows only your changes.
- `server` cannot import `command`, so `server.New` wires the executor by optional type assertion (`executor.(writeOrderingSetter)` and others in `internal/server/server.go`). Renaming one of those methods still compiles and silently switches the feature off; only integration tests notice.
- CodeQL (enabled in repo settings, not in `.github/workflows`) flags an `int64` to `int` conversion bounded by the `min` builtin as high severity. Write an explicit clamp (6db82b0).
- CI only vets darwin and windows. The kqueue poller (`internal/server/poller_darwin.go`) and the darwin paths in `peer_unix.go` have never run in CI: after changing them, run `go test ./...` natively on macOS, and say in the PR whether you did.

## Docs

The code is the source of truth: when docs and behavior disagree, fix the docs. Update the doc named below in the same commit as the code it describes.

- Adding or changing a command or modifier: `docs/reference/commands.md` (syntax, Replicated, Durable, deviations from Redis).
- Adding or changing a flag in `internal/config/config.go`: `docs/reference/configuration.md`.
- Changing INFO fields (`internal/command/info.go`), SLOWLOG or MONITOR output: `docs/guides/observability.md`.
- Touching `internal/aof`, `internal/rdb` or startup loading: `docs/guides/persistence.md`. The error in `internal/server/aof.go` cites its heading "Repairing a corrupt append-only file"; the test checks only the file name, so keep the two in step by hand.
- Touching replication, auth or bind behavior: `docs/guides/replication.md`, `docs/guides/securing-a-server.md`.
- Touching `internal/server`, the sequencer, or storage locking: `docs/architecture/overview.md`.
- Proposing a feature: README.md "Current boundaries" first; the gaps listed there are deliberate.
- Durability, performance or security work: PR #36's body (`gh pr view 36`) records decisions D1 to D20, the deferred list, and "Measured and rejected (do not redo)".
- Adding a doc: give it a row in `docs/README.md`.

## Agent skills

### Issue tracker

Issues are tracked in GitHub Issues for `maltemindedal/stash`. See `docs/agents/issue-tracker.md`.

### Triage labels

Triage uses the canonical label vocabulary: `needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`. See `docs/agents/triage-labels.md`.

### Domain docs

This repo uses a single-context domain-doc layout. See `docs/agents/domain.md`.

Read `GLOSSARY.md` before exploring the codebase for domain terms, and use its terms, not the synonyms it lists under _Avoid_, in issue titles, tests, refactors, and architecture notes.
