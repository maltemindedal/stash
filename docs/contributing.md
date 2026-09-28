# Contributing

## Development setup

Install **Go 1.21 or newer**. The minimum is the `go` directive in [`go.mod`](../go.mod); CI builds and tests on that minimum and on the two latest Go releases. Stash has no external module dependencies.

```bash
git clone https://github.com/maltemindedal/stash.git
cd stash
go build ./cmd/stash
```

For linting locally you also need [`golangci-lint`](https://golangci-lint.run/) **v2.13.2**, the version CI runs.

## Verification commands

These are the checks CI runs, in order. Run them before opening a pull request.

```bash
gofmt -s -l .          # must print nothing
go build ./cmd/stash
go vet ./...
go test ./...
golangci-lint run
```

Race tests run in CI on pushes to `main` only, but run them locally when touching concurrency:

```bash
go test -race ./...
```

Benchmarks for the parser and store:

```bash
go test -run ^$ -bench . ./internal/protocol ./internal/storage
```

## Lint configuration

[`.golangci.yml`](../.golangci.yml) enables `errcheck`, `govet`, `ineffassign`, `staticcheck`, and `unused` with a 2-minute timeout.

## Test layout

| Location | Scope |
| --- | --- |
| `internal/*/[name]_test.go` | Table-driven unit tests alongside the package under test |
| `test/` | End-to-end integration tests over a real TCP connection |

The integration suite covers AOF replay, RDB loading, replication, event-loop mode, hash and set commands, and multi-client shutdown. A new command with wire-visible behavior should get an integration test, not only a unit test.

## Conventions

- **Use the domain glossary.** [`CONTEXT.md`](../CONTEXT.md) defines the project's vocabulary. Name issues, tests, and refactors with those terms rather than synonyms.
- **The code is the source of truth for docs.** When documentation and behavior disagree, fix the documentation.
- **Unsupported input fails explicitly.** Unrecognized command modifiers return a syntax error rather than being silently ignored. This makes the limits of Redis compatibility clear.
- **Keep package seams intact.** Protocol, storage, command dispatch, and networking are deliberately separate; see the [architecture overview](architecture/overview.md).

## Agent-assisted contributions

[`AGENTS.md`](../AGENTS.md) holds behavioral guidelines for LLM coding agents working in this repo, and [`docs/agents/`](agents/) documents the issue tracker and triage label conventions those agents follow.

## Issues and pull requests

Issues are tracked in [GitHub Issues](https://github.com/maltemindedal/stash/issues). Triage uses the five canonical labels described in [`docs/agents/triage-labels.md`](agents/triage-labels.md): `needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, and `wontfix`.

Pull requests run the `validate` job (on the minimum Go version) and the `test-latest` job (on the two latest Go releases, plus a `go vet` for macOS and Windows) on every push. Pushes to `main` also run race tests.
