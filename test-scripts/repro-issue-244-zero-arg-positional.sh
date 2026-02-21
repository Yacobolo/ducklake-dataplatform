#!/usr/bin/env bash
set -euo pipefail
MODE="${1:-report}" # report|expect-bug|expect-fixed
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$ROOT_DIR/bin/duck"
TEST_HOME="/tmp/duckcli-issue-244"

mkdir -p "$ROOT_DIR/bin"
go build -o "$BIN" "$ROOT_DIR/cmd/cli"

rm -rf "$TEST_HOME"
mkdir -p "$TEST_HOME"
export HOME="$TEST_HOME"
"$BIN" config set-profile --name default --host http://127.0.0.1:65535 >/dev/null

run_case() {
  local name="$1"; shift
  local out rc
  set +e
  out=$("$@" 2>&1)
  rc=$?
  set -e
  echo "CASE: $name"
  echo "EXIT: $rc"
  echo "$out"
  echo "---"

  if [[ "$MODE" == "expect-bug" ]]; then
    [[ $rc -eq 0 ]] || { echo "expected buggy success for '$name'"; return 1; }
  elif [[ "$MODE" == "expect-fixed" ]]; then
    [[ $rc -ne 0 ]] || { echo "expected non-zero for '$name'"; return 1; }
  fi
}

run_case "version extra" "$BIN" version extra
run_case "commands extra" "$BIN" commands extra
run_case "config show extra" "$BIN" config show extra
run_case "commands --output json extra" "$BIN" commands --output json extra
run_case "config set-profile ... extra" "$BIN" config set-profile --name p --host http://127.0.0.1:65535 extra

echo "done mode=$MODE"