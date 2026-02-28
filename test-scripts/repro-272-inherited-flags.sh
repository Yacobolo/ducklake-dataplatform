#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

go test ./pkg/cli -run TestCommands_IncludesInheritedFlags -count=1
