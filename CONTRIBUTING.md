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

## Benchmarks

A performance change is judged against the commit it replaces, not by absolute numbers. Run [`scripts/bench.sh`](scripts/bench.sh) on a quiet machine, with nothing else competing for the CPUs (on a shared machine hold a lock around the run, for example with `flock`; the script takes none):

```bash
scripts/bench.sh [-count N] [-threshold PCT] <base-ref> <bench-regex> <package>...
scripts/bench.sh origin/main '^BenchmarkStore$' ./internal/storage
```

Each `<package>` is a path or import path that `go list` resolves from the repository root, whatever your current directory, to exactly one package; `./...` is refused. The package needs tests at `<base-ref>` and at `HEAD`.

For each package the script builds one test binary at `<base-ref>` and one at `HEAD` (uncommitted changes are not measured) on the floor toolchain, then runs `-count` rounds of the base binary followed by the head binary, so drift in the machine reaches both. `-count` defaults to 10 and must be at least 4: with fewer samples a side benchstat cannot reach p<0.05, so no slowdown could fail the bar. [`benchstat`](https://pkg.go.dev/golang.org/x/perf/cmd/benchstat) compares the results; its version is pinned in the script and nothing is added to `go.mod`. The first run downloads Go 1.21.13 and the Go 1.26 that benchstat needs.

The script prints a header to paste into the commit body (nproc, Go version, CPU model, count, base and head commits). It exits 1 and names the rows when a benchmark is more than `-threshold` percent (default 5) slower in sec/op at p<0.05, when allocs/op rises, or when a benchmark was measured on one side only (renamed or removed at `HEAD`, or new), because nothing can be compared. It exits 2 when the results cannot be judged, for example a row without a p-value. Use `-count 20` or a larger threshold only where the issue allows it. The pass or fail logic is [`scripts/benchcheck`](scripts/benchcheck), a standard-library Go program with its own tests.

For a quick look at the raw numbers of the parser and store, without a comparison:

```bash
go test -run ^$ -bench . ./internal/protocol ./internal/storage
```

The two RESP decoders in `internal/protocol` (the `Parser` and the `Decoder`) are tested against each other in `differential_test.go`. `go test` runs a fixed corpus of 2,500 mutated inputs, which takes about a second under `-race`; `-short` skips it. To run the first n inputs of the same corpus, or to fuzz for new ones:

```bash
STASH_DECODER_FUZZ_CASES=1000000 go test ./internal/protocol -run '^TestParserMatchesDecoderOnMutatedFrames$'
go test ./internal/protocol -run '^$' -fuzz '^FuzzParserMatchesDecoder$' -fuzztime 60s -fuzzminimizetime 0s
```

Without `-fuzzminimizetime 0s` the fuzzer spends most of the minute minimizing the inputs it finds. A failure prints its whole input; add it to `differentialSeeds` once it is fixed.

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

Two more tools are pinned outside `go.mod`, both to versions at least a week old when chosen (the rule in PR #36's dependency table): `golangci-lint` at the version `.github/workflows/ci.yml` names, and `benchstat`, which `scripts/bench.sh` runs as `golang.org/x/perf/cmd/benchstat@v0.0.0-20260929162123-406019bb8b68` (golang.org/x/perf has no tags, so this is a pseudo-version; the script's `benchstat_version` is the one place to bump it, together with this line).
