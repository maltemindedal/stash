# shellcheck shell=bash
# Sourced by scripts/gate.sh and .cursor/install.sh so both read the pins the same
# way. Run from the repo root. Each function prints the value, or names the problem
# on stderr and returns 1.

pins_say() {
  printf '%s: %s\n' "${pins_prefix:-pins}" "$*" >&2
}

# The golangci-lint version ci.yml pins, so ci.yml stays the one place that says it.
pinned_lint_version() {
  local want
  want="$(sed -n '/golangci\/golangci-lint-action/,/^ *- /s/^ *version: *\(v[0-9][0-9.]*\).*$/\1/p' .github/workflows/ci.yml)"
  case "$want" in
    *$'\n'*)
      pins_say "more than one golangci-lint version in .github/workflows/ci.yml: $want"
      return 1
      ;;
    v[0-9]*.[0-9]*.[0-9]*) printf '%s\n' "$want" ;;
    *)
      pins_say "no golangci-lint version found under golangci-lint-action in .github/workflows/ci.yml"
      return 1
      ;;
  esac
}

# The floor toolchain from the one `floor=goX.Y.Z` line in scripts/gate.sh. The line
# must be exactly that: a trailing comment or a second assignment is rejected.
pinned_floor() {
  local lines
  lines="$(grep -E '^floor=' scripts/gate.sh || true)"
  case "$lines" in
    *$'\n'*)
      pins_say "more than one floor= line in scripts/gate.sh"
      return 1
      ;;
  esac
  if ! printf '%s\n' "$lines" | grep -qE '^floor=go[0-9]+\.[0-9]+\.[0-9]+$'; then
    pins_say "scripts/gate.sh needs one line floor=goX.Y.Z with nothing after it; found '${lines}'"
    return 1
  fi
  printf '%s\n' "${lines#floor=}"
}

# Exit 0 when the go on PATH is 1.21 or newer. Older go ignores GOTOOLCHAIN, so it
# would silently run itself instead of the floor.
go_honours_gotoolchain() {
  local v major minor
  v="$(GOTOOLCHAIN=local go env GOVERSION 2>/dev/null)" || v=""
  v="${v#go}"
  major="${v%%.*}"
  minor="${v#*.}"
  minor="${minor%%[!0-9]*}"
  case "$major$minor" in
    '' | *[!0-9]*)
      pins_say "cannot read the go version from 'go env GOVERSION' (got '$v')"
      return 1
      ;;
  esac
  if [ "$major" -lt 1 ] || { [ "$major" -eq 1 ] && [ "$minor" -lt 21 ]; }; then
    pins_say "go $v is on PATH; Go >= 1.21 is required because older go ignores GOTOOLCHAIN"
    return 1
  fi
}

# The pinned upstream installer for a golangci-lint tag. It verifies the release
# archive against the release checksums.
lint_installer_url() {
  printf 'https://raw.githubusercontent.com/golangci/golangci-lint/%s/install.sh\n' "$1"
}
