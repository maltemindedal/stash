#!/usr/bin/env bash
set -euo pipefail

repo="$(git rev-parse --show-toplevel 2>/dev/null || true)"
if [ -z "$repo" ] || [ ! -f "$repo/go.mod" ]; then
  echo "install: run from the stash repository" >&2
  exit 1
fi
cd "$repo"

for cmd in go python3 gcc curl git; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "install: required command not on PATH: $cmd" >&2
    exit 1
  fi
done

# shellcheck source=scripts/pins.sh
. scripts/pins.sh
pins_prefix=install
go_honours_gotoolchain
lint_version="$(pinned_lint_version)"
floor="$(pinned_floor)"

want="${lint_version#v}"
have=""
if command -v golangci-lint >/dev/null 2>&1; then
  have="$(golangci-lint version --short 2>/dev/null || true)"
fi
if [ "$have" != "$want" ]; then
  dest="$(mktemp -d)"
  curl -sSfL "$(lint_installer_url "$lint_version")" | sh -s -- -b "$dest" "$lint_version"
  sudo install -m 0755 "$dest/golangci-lint" /usr/local/bin/golangci-lint
  rm -rf "$dest"
  hash -r
fi

resolved="$(command -v golangci-lint)"
have="$("$resolved" version --short)"
if [ "$have" != "$want" ]; then
  echo "install: $resolved reports golangci-lint $have, want $want (a different binary is ahead of /usr/local/bin on PATH)" >&2
  exit 1
fi

# GOTOOLCHAIN downloads the floor into the module cache on first use. Check that the
# floor actually ran, not the go on PATH.
got="$(GOTOOLCHAIN="$floor" go env GOVERSION)"
if [ "$got" != "$floor" ]; then
  echo "install: GOTOOLCHAIN=$floor go env GOVERSION reported '$got'; the floor toolchain is not cached" >&2
  exit 1
fi

echo "install: golangci-lint $have at $resolved"
echo "install: floor toolchain $floor cached"
