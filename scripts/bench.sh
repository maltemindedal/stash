#!/usr/bin/env bash
# Compares benchmarks on HEAD with a base ref against the bar that gates merges: benchstat,
# -count 10, interleaved, one machine. A commit fails the bar when a benchmark it touches
# is more than 5% slower in sec/op at p<0.05, or allocates more per op.
#
#   scripts/bench.sh [-count N] [-threshold PCT] <base-ref> <bench-regex> <package>...
#
#   scripts/bench.sh origin/main '^BenchmarkEncode$' ./internal/protocol
#   scripts/bench.sh -count 20 HEAD~1 '^BenchmarkStore$' ./internal/storage
#
# -count N is the number of rounds: default 10, minimum 4. With fewer samples a side benchstat
# cannot reach p<0.05, so no slowdown could ever fail the bar. Each <package> is a path or
# import path that `go list` resolves from the repo root, whatever the current directory, to
# exactly one package ('./...' is refused). That package needs tests at <base-ref> and at HEAD.
#
# For each package it builds one test binary at <base-ref> and one at HEAD, each in a
# temporary git worktree that is removed on exit (uncommitted changes are not measured), on
# the go.mod floor toolchain. It then runs N rounds. A round runs the base binary and then
# the head binary once per package, with -test.run '^$' -test.benchmem -test.count 1, so
# drift in the machine reaches both sides alike. benchstat compares the two sets of results.
#
# Exit 0: the bar is met. Exit 1: the bar failed, and the failing rows are named; a benchmark
# measured on one side only (renamed or removed at HEAD, or new) fails it, because nothing can
# be compared. Exit 2: bad usage, a step of the run failed, or the results cannot be judged.
#
# Run it on a quiet machine, with nothing else competing for the CPUs. The script takes no
# lock; on a shared machine hold one around it (flock). Paste the header it prints into the
# commit body.
set -Eeuo pipefail
trap 'exit 2' ERR

# The newest golang.org/x/perf version that was at least 7 days old when it was chosen, the
# age rule PR #36 gives for its govulncheck pin ("at least a week old", in its dependency
# table). x/perf has no tags, so this is a pseudo-version: commit 406019bb8b68 of 2026-09-29,
# chosen on 2026-10-08. It needs Go 1.26, which GOTOOLCHAIN=auto fetches for benchstat alone;
# the benchmarks build on the floor toolchain. Nothing is added to go.mod. CONTRIBUTING.md
# lists this pin beside the other pinned tools; change both together.
benchstat_version=v0.0.0-20260929162123-406019bb8b68

say() {
  printf 'bench: %s\n' "$*" >&2
}

die() {
  say "$*"
  exit 2
}

usage() {
  say "usage: scripts/bench.sh [-count N] [-threshold PCT] <base-ref> <bench-regex> <package>..."
  say "  -count N         rounds, one base run then one head run each (default 10, at least 4)"
  say "  -threshold PCT   percent slower in sec/op, at p<0.05, that fails the bar (default 5)"
  say "  <package>        a path or import path that go list resolves from the repo root to exactly one package"
  exit 2
}

count=10
threshold=5
while [ $# -gt 0 ]; do
  case "$1" in
    -count)
      [ $# -ge 2 ] || usage
      count=$2
      shift 2
      ;;
    -threshold)
      [ $# -ge 2 ] || usage
      threshold=$2
      shift 2
      ;;
    -h | -help | --help) usage ;;
    --)
      shift
      break
      ;;
    -*) die "unknown flag $1" ;;
    *) break ;;
  esac
done
[ $# -ge 3 ] || usage
if ! [[ $count =~ ^[1-9][0-9]*$ ]] || [ "$count" -lt 4 ]; then
  die "-count must be an integer of at least 4, got '$count': with fewer samples a side benchstat cannot reach p<0.05"
fi
[[ $threshold =~ ^[0-9]+(\.[0-9]+)?$ ]] || die "-threshold must be a non-negative number, got '$threshold'"
base_ref=$1
regex=$2
shift 2
patterns=("$@")

cd "$(dirname "${BASH_SOURCE[0]}")/.."

# The floor toolchain is named once, in gate.sh.
floor="$(sed -n 's/^floor=\(go[0-9][0-9.]*\)$/\1/p' scripts/gate.sh)"
[ -n "$floor" ] || die "cannot read the floor toolchain from scripts/gate.sh"
export GOTOOLCHAIN="$floor"

if ! base_commit="$(git rev-parse --verify --quiet "$base_ref^{commit}")"; then
  if [ "$(git rev-parse --is-shallow-repository)" = true ]; then
    die "unknown base ref '$base_ref' in a shallow clone: fetch more history (git fetch --deepen=N, or git fetch --unshallow) or name a ref it has"
  fi
  die "unknown base ref '$base_ref'"
fi
head_commit="$(git rev-parse HEAD)"
if [ -n "$(git status --porcelain --untracked-files=no)" ]; then
  say "warning: uncommitted changes are not measured; HEAD is $(git log -1 --format='%h %s' HEAD)"
fi

tmp="$(mktemp -d "${TMPDIR:-/tmp}/stash-bench.XXXXXX")"
cleanup() {
  local status=$?
  trap - EXIT ERR
  git worktree remove --force "$tmp/base" >/dev/null 2>&1 || true
  git worktree remove --force "$tmp/head" >/dev/null 2>&1 || true
  rm -rf "$tmp"
  git worktree prune >/dev/null 2>&1 || true
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

nproc_count() {
  nproc 2>/dev/null || getconf _NPROCESSORS_ONLN 2>/dev/null || sysctl -n hw.logicalcpu 2>/dev/null || echo unknown
}

cpu_model() {
  local model=""
  case "$(uname -s)" in
    Linux) model="$(awk -F': *' '/^model name/ { print $2; exit }' /proc/cpuinfo 2>/dev/null)" || true ;;
    Darwin) model="$(sysctl -n machdep.cpu.brand_string 2>/dev/null)" || true ;;
  esac
  printf '%s\n' "${model:-unknown}"
}

# Builds the test binary of import path $2 at worktree $1 into $3.
build_test_binary() {
  local label=$1 importpath=$2 out=$3
  if ! (cd "$tmp/$label" && go test -c -o "$out" "$importpath"); then
    die "cannot build the tests of $importpath at the $label commit"
  fi
  [ -x "$out" ] || die "$importpath has no tests at the $label commit"
}

# Runs test binary $2 from package directory $3, appending its results to $tmp/$1.txt.
run_benchmarks() {
  local label=$1 binary=$2 dir=$3
  if ! (cd "$dir" && "$binary" -test.run '^$' -test.bench "$regex" -test.benchmem -test.count 1 >>"$tmp/$label.txt"); then
    say "the $label benchmarks failed; their last output:"
    tail -n 20 "$tmp/$label.txt" >&2
    exit 2
  fi
}

git worktree add --detach --quiet "$tmp/base" "$base_commit"
git worktree add --detach --quiet "$tmp/head" "$head_commit"
mkdir "$tmp/bin"

modpath="$(cd "$tmp/head" && go list -m)"
packages=()
for pattern in "${patterns[@]}"; do
  resolved="$(cd "$tmp/head" && go list "$pattern")"
  case "$resolved" in
    '' | *$'\n'*) die "'$pattern' must name exactly one package" ;;
  esac
  packages+=("$resolved")
done

for i in "${!packages[@]}"; do
  say "building ${packages[$i]} at base and head"
  build_test_binary base "${packages[$i]}" "$tmp/bin/base-$i.test"
  build_test_binary head "${packages[$i]}" "$tmp/bin/head-$i.test"
done

# The test binaries read testdata relative to their package directory.
package_dir() {
  local label=$1 importpath=$2 rel
  rel="${importpath#"$modpath"}"
  printf '%s\n' "$tmp/$label/${rel#/}"
}

: >"$tmp/base.txt"
: >"$tmp/head.txt"
for ((round = 1; round <= count; round++)); do
  say "round $round of $count"
  for i in "${!packages[@]}"; do
    run_benchmarks base "$tmp/bin/base-$i.test" "$(package_dir base "${packages[$i]}")"
    run_benchmarks head "$tmp/bin/head-$i.test" "$(package_dir head "${packages[$i]}")"
  done
done
grep -q '^Benchmark' "$tmp/head.txt" || die "no benchmark matched '$regex' in ${patterns[*]}"

# benchstat needs a newer Go than the floor; it only reads the two result files.
benchstat() {
  (cd "$tmp" && env GOTOOLCHAIN=auto go run "golang.org/x/perf/cmd/benchstat@$benchstat_version" "$@")
}

benchstat -format csv base=base.txt head=head.txt >"$tmp/results.csv" 2>"$tmp/benchstat.err" || {
  cat "$tmp/benchstat.err" >&2
  die "benchstat $benchstat_version failed"
}
go build -o "$tmp/benchcheck" ./scripts/benchcheck

goversion="$(go version)"
printf '%s\n' \
  "Benchmarks: scripts/bench.sh, -count $count, base then head interleaved in each round, one machine" \
  "  nproc: $(nproc_count)" \
  "  go: ${goversion#go version }" \
  "  cpu: $(cpu_model)" \
  "  base: $(git log -1 --format='%h %s' "$base_commit") ($base_ref)" \
  "  head: $(git log -1 --format='%h %s' "$head_commit")" \
  "  run: -test.bench '$regex' -test.benchmem in ${patterns[*]}" \
  "  compare: benchstat $benchstat_version; bar: sec/op more than $threshold% slower at p<0.05, any rise in allocs/op, or a benchmark measured on one side only" \
  ""
benchstat base=base.txt head=head.txt
echo

status=0
"$tmp/benchcheck" -threshold "$threshold" "$tmp/results.csv" || status=$?
exit "$status"
