#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

files=(
  "docs/reference/generated/api/index.md"
  "docs/reference/generated/api/features.md"
  "docs/reference/generated/declarative/index.md"
  "docs/public/llms.txt"
  "docs/public/llms-full.txt"
  "docs/public/agent-index.json"
  "pkg/cli/gen/discovery_index.gen.go"
)

tmp_before="$(mktemp)"
tmp_after="$(mktemp)"
trap 'rm -f "$tmp_before" "$tmp_after"' EXIT

snapshot() {
  local out="$1"
  : >"$out"
  for file in "${files[@]}"; do
    if [[ ! -f "$file" ]]; then
      echo "missing  $file" >>"$out"
      continue
    fi
    shasum -a 256 "$file" >>"$out"
  done
}

snapshot "$tmp_before"
task docs:generate

for file in "${files[@]}"; do
  test -f "$file"
done

snapshot "$tmp_after"

if ! diff -u "$tmp_before" "$tmp_after"; then
  echo "generated docs artifacts changed after regeneration" >&2
  exit 1
fi
