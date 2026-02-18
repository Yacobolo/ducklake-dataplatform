#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
DUCK_BIN="${DUCK_BIN:-$ROOT_DIR/bin/duck}"
CATALOG_NAME="${CATALOG_NAME:-lake}"
SCHEMA_NAME="${SCHEMA_NAME:-main}"
API_KEY="${API_KEY:-}"
DUCK_HOST="${DUCK_HOST:-http://localhost:8080}"
LANDING_DIR="$ROOT_DIR/ducklake_data/showcase_ingest"

if [[ -z "$API_KEY" ]]; then
  echo "API_KEY is required" >&2
  exit 1
fi

if [[ ! -x "$DUCK_BIN" ]]; then
  echo "duck CLI not found or not executable: $DUCK_BIN" >&2
  echo "run: task build-cli" >&2
  exit 1
fi

if ! command -v duckdb >/dev/null 2>&1; then
  echo "duckdb CLI is required to generate parquet seed files" >&2
  exit 1
fi

mkdir -p "$LANDING_DIR"

duck() {
	"$DUCK_BIN" --host "$DUCK_HOST" --token '' --api-key "$API_KEY" "$@"
}

echo "Preparing deterministic parquet files in $LANDING_DIR"
duckdb ":memory:" "COPY (SELECT CAST(movieId AS BIGINT) AS movie_id, title, genres FROM read_csv_auto('$ROOT_DIR/examples/showcase-movielens/data/movies.csv', header=true)) TO '$LANDING_DIR/movies.parquet' (FORMAT PARQUET);"
duckdb ":memory:" "COPY (SELECT CAST(userId AS BIGINT) AS user_id, age_group, region FROM read_csv_auto('$ROOT_DIR/examples/showcase-movielens/data/users.csv', header=true)) TO '$LANDING_DIR/users.parquet' (FORMAT PARQUET);"
duckdb ":memory:" "COPY (SELECT CAST(userId AS BIGINT) AS user_id, CAST(movieId AS BIGINT) AS movie_id, CAST(rating AS DOUBLE) AS rating, CAST(timestamp AS BIGINT) AS rating_ts FROM read_csv_auto('$ROOT_DIR/examples/showcase-movielens/data/ratings.csv', header=true)) TO '$LANDING_DIR/ratings.parquet' (FORMAT PARQUET);"

echo "Registering files through ingestion API"
duck ingestion load "$SCHEMA_NAME" raw_movies --catalog-name "$CATALOG_NAME" --paths showcase_ingest/movies.parquet
duck ingestion load "$SCHEMA_NAME" raw_users --catalog-name "$CATALOG_NAME" --paths showcase_ingest/users.parquet
duck ingestion load "$SCHEMA_NAME" raw_ratings --catalog-name "$CATALOG_NAME" --paths showcase_ingest/ratings.parquet

echo "Ingestion complete"
