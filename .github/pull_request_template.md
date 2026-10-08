## What changed and why

<!-- Describe the change and the reason for it. -->

## Linked issue

Closes #

## Checks

These are the checks CI runs, from [CONTRIBUTING.md](https://github.com/maltemindedal/stash/blob/main/CONTRIBUTING.md#verification-commands). I ran them before opening this PR:

- [ ] `scripts/gate.sh` exits 0 (runs every check below, then the race tests and the macOS and Windows vet)
- [ ] `gofmt -s -l .` prints nothing
- [ ] `go build ./cmd/stash`
- [ ] `go vet ./...`
- [ ] `go test ./...`
- [ ] `golangci-lint run`
