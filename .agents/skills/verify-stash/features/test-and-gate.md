# Test and gate

Contributors prove a change with `go test ./...` and the full PR gate `scripts/gate.sh`, which mirrors CI on the Go 1.21 floor.

## Sub-features

- `test` runs `go test ./...`, which exits 0.
- `gate` runs `scripts/gate.sh`, which exits 0.

## How to get to it (user POV)

- Run `go test ./...` or `scripts/gate.sh` at the repo root.

## Driving it with verify.sh

Preconditions:

- `doctor` passes. For `gate`, `golangci-lint` v2.13.2 is installed.

- **Test.** Run `verify.sh test`. The output shows `PASS go test exits 0` and `N packages ok`.
- **Gate.** Run `verify.sh gate`. The output shows `PASS scripts/gate.sh exits 0`.

## Gotchas

- Read the exit code, not piped output. AGENTS.md records two commits that went in on a failed gate that way.
- The first `gate` run downloads go1.21.13.
