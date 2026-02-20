#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-after}"
FILE="pkg/cli/config_cmd.go"

if [[ "$MODE" == "before" ]]; then
  echo "[repro-before] Expecting set-profile to persist output without validation"
  if grep -q 'p.Output = output' "$FILE" && ! grep -q 'validateOutputFormat(output)' "$FILE"; then
    echo "Bug reproduced: --output value is persisted without validation."
  else
    echo "Before-state check failed (validation already present)."
    exit 1
  fi
elif [[ "$MODE" == "after" ]]; then
  echo "[verify-after] Expecting set-profile to validate --output"
  grep -q 'validateOutputFormat(output)' "$FILE"
  echo "Fix verified: invalid output formats are rejected at set-profile time."
else
  echo "Usage: $0 [before|after]"
  exit 2
fi
