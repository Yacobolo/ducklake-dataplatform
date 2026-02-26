#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

mkdir -p bin

go build -o bin/server ./cmd/server

out_dir="test-results"
mkdir -p "$out_dir"
out_file="$out_dir/repro-263-server-help.out"
err_file="$out_dir/repro-263-server-help.err"

set +e
timeout 3 ./bin/server --help >"$out_file" 2>"$err_file"
rc=$?
set -e

echo "exit_code=$rc"
if [[ $rc -eq 124 ]]; then
  echo "FAIL: command timed out (server likely started instead of showing help)"
  exit 1
fi

if ! grep -qi "usage" "$out_file" && ! grep -qi "usage" "$err_file"; then
  echo "FAIL: expected usage/help text in output"
  echo "--- stdout ---"
  cat "$out_file"
  echo "--- stderr ---"
  cat "$err_file"
  exit 1
fi

if grep -q "HTTP API listening" "$err_file"; then
  echo "FAIL: server startup log found while requesting --help"
  exit 1
fi

echo "PASS: --help printed usage and did not start server"
