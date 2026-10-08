# Contributing

Everyone taking part in Stash is expected to follow the [Code of Conduct](CODE_OF_CONDUCT.md).

## Development setup

Install **Go 1.21 or newer**. The minimum is the `go` directive in [`go.mod`](go.mod); CI builds and tests on that minimum and on the two latest Go releases. Stash has no external module dependencies.

```bash
git clone https://github.com/maltemindedal/stash.git
cd stash
go build ./cmd/stash
```

For linting locally you also need [`golangci-lint`](https://golangci-lint.run/) **v2.13.2**, the version CI runs.

## Verification commands

Run [`scripts/gate.sh`](scripts/gate.sh) before opening a pull request. It runs the checks CI runs, from the repository root, on the Go version in `go.mod` (the first run downloads Go 1.21.13 through `GOTOOLCHAIN`). It stops at the first failure, names the failed step and exits non-zero; it exits 0 only when every step passed, and it prints the total duration.

The script also fails when:

- a `stash` binary, a `dump.rdb` or an `*.aof` file is untracked or modified in the working tree. Delete it before committing.
- the `golangci-lint` it finds (on `PATH`, or the binary in `GOLANGCI_LINT`) is not the version `.github/workflows/ci.yml` pins. It prints the install command.

Check the script's exit status itself. A pipe into `tail` or `grep` replaces it with the status of the pipe's last command, which hides a failure.

The script runs these commands:

```bash
gofmt -s -l .          # must print nothing
go build -o /dev/null ./cmd/stash
go vet ./...
go test ./...
golangci-lint run
go test -race -shuffle=on ./...
for t in darwin/amd64 darwin/arm64 windows/amd64; do CGO_ENABLED=0 GOOS=${t%/*} GOARCH=${t#*/} go vet ./...; done
```

The CI `race` job runs the tests under the race detector in random order (`-shuffle=on`) to catch tests that depend on each other. A failing run prints the shuffle seed; repeat it with `go test -race -shuffle=<seed> ./...`.

Benchmarks for the parser and store:

```bash
go test -run ^$ -bench . ./internal/protocol ./internal/storage
```

## Lint configuration

[`.golangci.yml`](.golangci.yml) enables `depguard`, `errcheck`, `govet`, `ineffassign`, `staticcheck`, `unused`, `errorlint`, `makezero`, `nilnesserr`, `predeclared`, and `wastedassign` with a 2-minute timeout. `govet` runs every analyzer except `fieldalignment` and `shadow`, and `staticcheck` runs all of its checks. `depguard` keeps Stash on the standard library alone: an import outside the standard library and this module fails lint, in code and in tests. It does not look at `go.mod`, so a `require` block there is still caught in review.

## Test layout

| Location | Scope |
| --- | --- |
| `internal/*/[name]_test.go` | Table-driven unit tests alongside the package under test |
| `test/` | End-to-end integration tests over a real TCP connection |

The integration suite covers AOF replay, RDB loading, replication, event-loop mode, hash and set commands, and multi-client shutdown. A new command with wire-visible behavior should get an integration test, not only a unit test.

## Conventions

- **Use the domain glossary.** [`GLOSSARY.md`](GLOSSARY.md) defines the project's vocabulary. Name issues, tests, and refactors with those terms rather than synonyms, and add a term there once a new domain concept is stable.
- **The code is the source of truth for docs.** When documentation and behavior disagree, fix the documentation.
- **Unsupported input fails explicitly.** Unrecognized command modifiers return a syntax error rather than being silently ignored. This makes the limits of Redis compatibility clear.
- **Keep package seams intact.** Protocol, storage, command dispatch, and networking are deliberately separate; see the [architecture overview](docs/architecture/overview.md).

## Agent-assisted contributions

[`AGENTS.md`](AGENTS.md) holds the commands, conventions and gotchas for LLM coding agents working in this repo, and [`docs/agents/`](docs/agents/) documents the issue tracker and triage label conventions those agents follow.

## Issues and pull requests

Issues are tracked in [GitHub Issues](https://github.com/maltemindedal/stash/issues). Triage uses the five canonical labels described in [`docs/agents/triage-labels.md`](docs/agents/triage-labels.md): `needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, and `wontfix`.

Pull requests run the `validate` job (on the minimum Go version) and the `test-latest` job (on the two latest Go releases, plus a `go vet` for macOS on both architectures and for Windows) on every push. The `race` job runs the tests under the race detector on both.

## Dependency and vulnerability checks

Stash has no module dependencies, so the moving parts are the Go toolchain and the GitHub Actions in `.github/workflows/`. Actions are pinned to commit SHAs; Dependabot proposes bumps weekly, holding back new releases for its default cooldown. A weekly workflow runs `govulncheck` against the latest stable Go. Run it locally with `go run golang.org/x/vuln/cmd/govulncheck@v1.8.0 ./...`.
