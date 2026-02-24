#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="${1:-$(pwd)}"
TMP_HOME="$(mktemp -d)"
trap 'rm -rf "$TMP_HOME"' EXIT

cd "$REPO_DIR"
CLI_BIN="./cli"
if [[ ! -x "$CLI_BIN" ]]; then
  echo "missing executable $CLI_BIN" >&2
  exit 1
fi

HOME="$TMP_HOME" "$CLI_BIN" config set-profile --name p1 --host http://localhost:9999 >/dev/null
HOME="$TMP_HOME" "$CLI_BIN" config show --output table
