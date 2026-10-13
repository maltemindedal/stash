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

lint_version="$(sed -n '/golangci\/golangci-lint-action/,/^ *- /s/^ *version: *\(v[0-9][0-9.]*\).*$/\1/p' .github/workflows/ci.yml)"
case "$lint_version" in
  v[0-9]*.[0-9]*.[0-9]*) ;;
  *)
    echo "install: no single golangci-lint version under golangci-lint-action in .github/workflows/ci.yml" >&2
    exit 1
    ;;
esac

floor="$(sed -n 's/^floor=//p' scripts/gate.sh)"
case "$floor" in
  go[0-9]*.[0-9]*.[0-9]*) ;;
  *)
    echo "install: no Go floor assignment in scripts/gate.sh" >&2
    exit 1
    ;;
esac

want="${lint_version#v}"
have=""
if command -v golangci-lint >/dev/null 2>&1; then
  have="$(golangci-lint version --short 2>/dev/null || true)"
fi
if [ "$have" != "$want" ]; then
  dest="$(mktemp -d)"
  curl -sSfL https://golangci-lint.run/install.sh | sh -s -- -b "$dest" "$lint_version"
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

GOTOOLCHAIN="$floor" go version

echo "install: golangci-lint $have at $resolved"
echo "install: floor toolchain $floor cached"
