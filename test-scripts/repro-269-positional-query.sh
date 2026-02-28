#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

go test ./pkg/cli -run TestQueryOverride/SQL_from_positional_arg -count=1
