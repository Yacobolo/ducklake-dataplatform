#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
META_DB="${META_DB:-$ROOT_DIR/ducklake_meta.sqlite}"
API_KEY="${API_KEY:-showcase-local-admin-key}"
ADMIN_PRINCIPAL="${ADMIN_PRINCIPAL:-ml_admin}"

if ! command -v sqlite3 >/dev/null 2>&1; then
  echo "sqlite3 is required" >&2
  exit 1
fi

if ! command -v shasum >/dev/null 2>&1; then
  echo "shasum is required" >&2
  exit 1
fi

if [[ ! -f "$META_DB" ]]; then
  echo "metadata database not found: $META_DB" >&2
  echo "start the server once to initialize migrations" >&2
  exit 1
fi

KEY_HASH="$(printf "%s" "$API_KEY" | shasum -a 256 | awk '{print $1}')"

sqlite3 "$META_DB" <<SQL
INSERT OR IGNORE INTO principals(id, name, type, is_admin)
VALUES ('showcase-admin-id', '$ADMIN_PRINCIPAL', 'user', 1);

INSERT OR IGNORE INTO api_keys(id, key_hash, principal_id, name, key_prefix)
VALUES (
  'showcase-admin-key-id',
  '$KEY_HASH',
  (SELECT id FROM principals WHERE name='$ADMIN_PRINCIPAL'),
  'showcase-admin',
  'showcase'
);
SQL

echo "Admin API key bootstrapped for principal '$ADMIN_PRINCIPAL'"
echo "Export this before running showcase commands:"
echo "  export API_KEY=$API_KEY"
