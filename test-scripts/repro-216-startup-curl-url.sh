#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG="${TMPDIR:-/tmp}/repro-216-startup-curl.log"
: > "$LOG"

go build -o "$ROOT/bin/server" "$ROOT/cmd/server"

if ! timeout 5s env LISTEN_ADDR="127.0.0.1:18080" JWT_SECRET="dev-secret-change-in-production" \
  "$ROOT/bin/server" >"$LOG" 2>&1; then
  # timeout exits non-zero by design; ignore if process was killed by timeout.
  true
fi

line="$(grep -m1 "try\"" "$LOG" || true)"
if [[ -z "$line" ]]; then
  echo "FAIL: no startup 'try curl' line found"
  echo "--- log ---"
  sed -n '1,80p' "$LOG"
  exit 1
fi

echo "startup_line=$line"

if grep -q "http://localhost127.0.0.1:18080/v1/principals" <<<"$line"; then
  echo "BUG_PRESENT: malformed URL detected"
  exit 2
fi

if grep -q "http://127.0.0.1:18080/v1/principals" <<<"$line" || grep -q "http://localhost:18080/v1/principals" <<<"$line"; then
  echo "PASS: startup curl URL is valid"
  exit 0
fi

echo "FAIL: startup line did not contain expected URL format"
exit 1
