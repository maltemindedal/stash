#!/usr/bin/env bash
# Drive Stash the way a Redis client does and keep the evidence.
# Usage: verify.sh <doctor|build|test|gate|launch|drive|persist|stop|cleanup|all>
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(git -C "$HERE" rev-parse --show-toplevel)"
RUN_ID="${STASH_VERIFY_RUN:-$(date +%Y%m%d-%H%M%S)}"
ART="${STASH_VERIFY_ARTIFACTS:-/tmp/stash-verify/$RUN_ID}"
WORK="$ART/work"
BIN="$WORK/stash"
mkdir -p "$ART" "$WORK"
cd "$ROOT" || exit 1

FAILED=0
SKIPPED=0

run() {
  local name="$1"; shift
  printf '%q ' "$@" > "$ART/$name.cmd"; echo >> "$ART/$name.cmd"
  "$@" > "$ART/$name.stdout" 2> "$ART/$name.stderr"
  local rc=$?
  echo "$rc" > "$ART/$name.exit"
  echo "[$name] exit=$rc  ($ART/$name.*)"
  return "$rc"
}

expect() {
  local desc="$1" file="$2" pattern="$3"
  if grep -qE -- "$pattern" "$file"; then echo "  PASS $desc"; else echo "  FAIL $desc (no /$pattern/ in $file)"; FAILED=1; fi
}

addr() { cat "$ART/server.addr" 2>/dev/null; }

# cmd <name> <arg>...: one RESP command against the running server, reply saved as <name>.stdout
cmd() { local name="$1"; shift; run "$name" python3 "$HERE/resp.py" "$(addr)" "$@"; }

doctor() {
  echo "artifacts: $ART"
  echo "repo: $ROOT @ $(git rev-parse --short HEAD) ($(git rev-parse --abbrev-ref HEAD))"
  command -v go >/dev/null || { echo "  FAIL go not on PATH"; return 1; }
  echo "go: $(go version)"
  local floor; floor="$(sed -n 's/^go //p' go.mod)"
  echo "go.mod floor: $floor (CI builds on it; scripts/gate.sh pins GOTOOLCHAIN=go1.21.13)"
  command -v python3 >/dev/null && echo "  PASS python3 present (RESP client)" || { echo "  FAIL python3 missing: drive needs it"; return 1; }
  if command -v golangci-lint >/dev/null || [ -n "${GOLANGCI_LINT:-}" ]; then
    echo "  PASS golangci-lint available for gate"
  else
    echo "  WARN golangci-lint missing: gate will SKIP"
  fi
  if [ -s "$ART/server.pid" ] && kill -0 "$(cat "$ART/server.pid")" 2>/dev/null; then
    echo "  PASS server from this run is up: pid $(cat "$ART/server.pid") at $(addr)"
  else
    echo "  INFO no server from this run is running"
  fi
}

build() { run build go build -o "$BIN" ./cmd/stash; }

test_go() {
  run test go test ./... || FAILED=1
  expect "go test exits 0" "$ART/test.exit" "^0$"
  if grep -E "^(FAIL|--- FAIL)" "$ART/test.stdout" > "$ART/test.failures"; then echo "  FAIL $(wc -l < "$ART/test.failures") failing lines in test.stdout"; FAILED=1; fi
  echo "  $(grep -c '^ok ' "$ART/test.stdout") packages ok"
}

gate() {
  if ! command -v golangci-lint >/dev/null && [ -z "${GOLANGCI_LINT:-}" ]; then
    echo "  SKIP gate: golangci-lint not installed (scripts/gate.sh prints the pinned install command)"; SKIPPED=1; return
  fi
  run gate scripts/gate.sh || FAILED=1
  expect "scripts/gate.sh exits 0" "$ART/gate.exit" "^0$"
}

# launch [extra flags...]: start our own server on a free loopback port
launch() {
  if [ -s "$ART/server.pid" ] && kill -0 "$(cat "$ART/server.pid")" 2>/dev/null; then echo "  FAIL a server from this run is already up; run stop first"; FAILED=1; return 1; fi
  [ -x "$BIN" ] || build || { FAILED=1; return 1; }
  (cd "$WORK" && exec "$BIN" --host 127.0.0.1 --port 0 --dump "" --aof "$WORK/appendonly.aof" "$@" > "$ART/server.log" 2>&1) &
  echo $! > "$ART/server.pid"
  local i
  for i in $(seq 1 50); do
    if grep -q 'msg="Stash listening"' "$ART/server.log" 2>/dev/null; then
      sed -n 's/.*msg="Stash listening" address=\([^ ]*\).*/\1/p' "$ART/server.log" | head -1 > "$ART/server.addr"
      break
    fi
    sleep 0.1
  done
  if [ -s "$ART/server.addr" ] && [ "$(python3 "$HERE/resp.py" "$(addr)" PING 2>/dev/null)" = PONG ]; then
    echo "  PASS server ready at $(addr) (pid $(cat "$ART/server.pid"), PING -> PONG)"
  else
    echo "  FAIL server not ready (see $ART/server.log)"; FAILED=1; return 1
  fi
}

stop() {
  [ -s "$ART/server.pid" ] || return 0
  local pid; pid="$(cat "$ART/server.pid")"
  kill "$pid" 2>/dev/null
  local i; for i in $(seq 1 50); do kill -0 "$pid" 2>/dev/null || break; sleep 0.1; done
  kill -0 "$pid" 2>/dev/null && kill -9 "$pid" 2>/dev/null
  rm -f "$ART/server.pid" "$ART/server.addr"
  echo "  stopped pid $pid"
}

drive() {
  [ -s "$ART/server.addr" ] || { echo "  FAIL no server; run launch first"; FAILED=1; return; }
  cmd ping PING; expect "PING replies PONG" "$ART/ping.stdout" "^PONG$"
  cmd set SET verify:greeting hello; expect "SET replies OK" "$ART/set.stdout" "^OK$"
  cmd get GET verify:greeting; expect "GET returns the stored value" "$ART/get.stdout" '^"hello"$'
  cmd incr-reset DEL verify:counter; expect "DEL of the counter replies with an integer" "$ART/incr-reset.stdout" '^\(integer\) [01]$'
  cmd incr INCR verify:counter; expect "INCR on a just-deleted key returns 1" "$ART/incr.stdout" '^\(integer\) 1$'
  cmd unknown NOSUCHCOMMAND; expect "unknown command is an explicit error" "$ART/unknown.stdout" "^\(error\) ERR unknown command"
}

persist() {
  [ -s "$ART/server.addr" ] || launch || return
  cmd persist-set SET verify:durable kept; expect "SET before restart replies OK" "$ART/persist-set.stdout" "^OK$"
  stop
  cp "$WORK/appendonly.aof" "$ART/appendonly.aof.copy" 2>/dev/null
  expect "AOF on disk records the SET" "$WORK/appendonly.aof" "verify:durable"
  launch || return
  cmd persist-get GET verify:durable; expect "GET after restart returns the AOF value" "$ART/persist-get.stdout" '^"kept"$'
  stop
}

cleanup() {
  stop
  rm -rf "$WORK"
  echo "cleanup done; evidence kept in $ART"
}

STEP="${1:-}"
case "$STEP" in
  doctor) doctor || FAILED=1 ;;
  build) build || FAILED=1 ;;
  test) test_go ;;
  gate) gate ;;
  launch) shift; launch "$@" ;;
  drive) drive ;;
  persist) persist ;;
  stop) stop ;;
  cleanup) cleanup ;;
  all)
    if doctor; then test_go; gate; launch && { drive; stop; persist; }; else FAILED=1; echo "  doctor failed: not driving this checkout"; fi
    cleanup ;;
  *) sed -n '2,3p' "$0"; exit 2 ;;
esac
if [ "$FAILED" != 0 ]; then echo "RESULT: FAIL ($STEP)"; elif [ "$SKIPPED" != 0 ]; then echo "RESULT: PASS with SKIP ($STEP): skipped steps are unverified"; else echo "RESULT: PASS ($STEP)"; fi
exit "$FAILED"
