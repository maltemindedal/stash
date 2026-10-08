#!/usr/bin/env bash
# The pre-PR gate: every check in AGENTS.md "Commands", from the repo root, on the Go
# floor toolchain, stopping at the first failure and naming the step that failed.
#
#   scripts/gate.sh                                    # exit 0 only when every step passed
#   GOLANGCI_LINT=/path/to/golangci-lint scripts/gate.sh
#
# Read the exit status directly or redirect the output to a file. A pipe into tail or
# grep reports the pipe's last command instead (two PR #36 commits went in on a failed gate that way).
set -euo pipefail

# The go.mod floor, as a toolchain. CI's validate and race jobs build on it; the go
# commands below run on it through GOTOOLCHAIN, which downloads it on first use.
floor=go1.21.13

cd "$(dirname "${BASH_SOURCE[0]}")/.."

current=preflight
finish() {
  local status=$?
  trap - EXIT
  if [ "$status" -eq 0 ]; then
    printf '\nGATE PASSED in %ss\n' "$SECONDS"
  else
    printf '\nGATE FAILED at step "%s" (exit %s) after %ss\n' "$current" "$status" "$SECONDS" >&2
  fi
  exit "$status"
}
trap finish EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# step NAME COMMAND...: names the step for the failure report, then runs it. Under
# set -e a failing command ends the script, so no later step runs.
step() {
  current=$1
  shift
  printf '\n== %s\n' "$current"
  "$@"
}

say() {
  printf 'gate: %s\n' "$*" >&2
}

# The declared floor and the toolchain this script pins must agree, or the gate would
# test a different Go than CI does.
check_floor() {
  local declared
  declared="$(awk '$1 == "go" { print $2; exit }' go.mod)"
  case "$floor" in
    go"$declared".*) ;;
    *)
      say "go.mod declares go $declared but gate.sh pins $floor; update floor in scripts/gate.sh"
      return 1
      ;;
  esac
}

# The golangci-lint version ci.yml pins, so ci.yml stays the one place that says it.
pinned_lint_version() {
  local want
  want="$(sed -n '/golangci\/golangci-lint-action/,/^ *- /s/^ *version: *\(v[0-9][0-9.]*\).*$/\1/p' .github/workflows/ci.yml)"
  case "$want" in
    *$'\n'*)
      say "more than one golangci-lint version in .github/workflows/ci.yml: $want"
      return 1
      ;;
    v[0-9]*.[0-9]*.[0-9]*) printf '%s\n' "$want" ;;
    *)
      say "no golangci-lint version found under golangci-lint-action in .github/workflows/ci.yml"
      return 1
      ;;
  esac
}

# Fails when a stash binary, dump.rdb or *.aof is untracked or modified (AGENTS.md
# "Gotchas"): commits must hold only the change.
check_stray_artifacts() {
  local status_out line path stray=""
  status_out="$(git status --porcelain --untracked-files=all)"
  while IFS= read -r line; do
    [ -n "$line" ] || continue
    path="${line:3}"
    path="${path##* -> }"
    path="${path#\"}"
    path="${path%\"}"
    case "${path##*/}" in
      stash | dump.rdb | *.aof) stray="$stray $path" ;;
    esac
  done <<<"$status_out"
  if [ -n "$stray" ]; then
    say "delete before committing:$stray"
    return 1
  fi
}

# Fails unless the golangci-lint found reports exactly the version ci.yml pins.
check_lint_version() {
  local want lint have
  want="$(pinned_lint_version)"
  lint="${GOLANGCI_LINT:-golangci-lint}"
  if ! command -v "$lint" >/dev/null 2>&1; then
    say "golangci-lint ${want#v} is required and '$lint' was not found"
    lint_install_hint "$want"
    return 1
  fi
  have="$("$lint" version --short 2>/dev/null)" || have="unknown"
  if [ "$have" != "${want#v}" ]; then
    say "golangci-lint ${want#v} is required (pinned in .github/workflows/ci.yml); $(command -v "$lint") reports $have"
    lint_install_hint "$want"
    return 1
  fi
  printf 'golangci-lint %s at %s\n' "$have" "$(command -v "$lint")"
}

lint_install_hint() {
  printf '  install: curl -sSfL https://golangci-lint.run/install.sh | sh -s -- -b "$(go env GOPATH)/bin" %s\n' "$1" >&2
  printf '  or set GOLANGCI_LINT to a %s binary\n' "$1" >&2
}

# gofmt must come from the floor toolchain too: the binary on PATH belongs to whichever
# Go is installed.
check_gofmt() {
  local unformatted
  unformatted="$("$(go env GOROOT)/bin/gofmt" -s -l .)"
  if [ -n "$unformatted" ]; then
    printf '%s\n' "$unformatted" >&2
    say "the files above need gofmt -s; fix with: gofmt -s -w ."
    return 1
  fi
}

step "floor toolchain matches go.mod" check_floor
export GOTOOLCHAIN="$floor"
step "go version" go version
step "stray artifacts" check_stray_artifacts
step "golangci-lint version" check_lint_version
lint="${GOLANGCI_LINT:-golangci-lint}"

step "gofmt" check_gofmt
step "build" go build -o /dev/null ./cmd/stash
step "vet" go vet ./...
step "test" go test ./...
step "lint" "$lint" run
step "race" go test -race -shuffle=on ./...
for target in darwin/amd64 darwin/arm64 windows/amd64; do
  step "vet $target" env CGO_ENABLED=0 GOOS="${target%/*}" GOARCH="${target#*/}" go vet ./...
done
step "stray artifacts after the run" check_stray_artifacts
