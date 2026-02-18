#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
DUCK_BIN="${DUCK_BIN:-$ROOT_DIR/bin/duck}"
API_KEY="${API_KEY:-}"
DUCK_HOST="${DUCK_HOST:-http://localhost:8080}"
CONFIG_DIR="examples/showcase-movielens/config"

if [[ -z "$API_KEY" ]]; then
  echo "API_KEY is required" >&2
  exit 1
fi

if [[ ! -x "$DUCK_BIN" ]]; then
  echo "duck CLI not found or not executable: $DUCK_BIN" >&2
  echo "run: task build-cli" >&2
  exit 1
fi

duck() {
	"$DUCK_BIN" --host "$DUCK_HOST" --token '' --api-key "$API_KEY" "$@"
}

echo "Validating and applying declarative showcase"
duck validate --config-dir "$CONFIG_DIR"
duck plan --config-dir "$CONFIG_DIR" || true
duck apply --config-dir "$CONFIG_DIR" --auto-approve

echo "Loading raw data through ingestion API"
DUCK_HOST="$DUCK_HOST" "$ROOT_DIR/examples/showcase-movielens/scripts/ingest_seed_data.sh"

echo "Running transformation models"
MODEL_NAMES="bronze_movies,bronze_users,bronze_ratings,silver_movies,silver_users,silver_ratings_enriched,gold_movie_scores,gold_user_engagement"
if command -v jq >/dev/null 2>&1; then
  RUN_JSON="$(duck --output json models model-runs trigger-model-run --project-name movielens --model-names "$MODEL_NAMES" --target-catalog lake --target-schema main)"
  RUN_ID="$(printf "%s" "$RUN_JSON" | jq -r '.id')"
  if [[ -z "$RUN_ID" || "$RUN_ID" == "null" ]]; then
    echo "failed to parse model run id" >&2
    exit 1
  fi

  STATUS="PENDING"
  for _ in {1..40}; do
    STATUS="$(duck --output json models model-runs get "$RUN_ID" | jq -r '.status')"
    if [[ "$STATUS" == "SUCCESS" || "$STATUS" == "FAILED" || "$STATUS" == "CANCELLED" ]]; then
      break
    fi
    sleep 1
  done

  if [[ "$STATUS" != "SUCCESS" ]]; then
    echo "model run failed with status: $STATUS" >&2
    duck --output json models steps list "$RUN_ID"
    exit 1
  fi

  FRESHNESS_JSON="$(duck --output json models freshness check-model-freshness movielens gold_user_engagement)"
  IS_FRESH="$(printf "%s" "$FRESHNESS_JSON" | jq -r '.is_fresh')"
  if [[ "$IS_FRESH" != "true" ]]; then
    echo "freshness check failed for movielens.gold_user_engagement" >&2
    printf "%s\n" "$FRESHNESS_JSON" >&2
    exit 1
  fi
else
  duck models model-runs trigger-model-run --project-name movielens --model-names "$MODEL_NAMES" --target-catalog lake --target-schema main
  sleep 3
fi

echo "Triggering pipeline run"
if command -v jq >/dev/null 2>&1; then
  PIPELINE_JSON="$(duck --output json pipelines runs trigger movielens_daily)"
  PIPELINE_RUN_ID="$(printf "%s" "$PIPELINE_JSON" | jq -r '.id')"
  if [[ -z "$PIPELINE_RUN_ID" || "$PIPELINE_RUN_ID" == "null" ]]; then
    echo "failed to parse pipeline run id" >&2
    exit 1
  fi

  PIPELINE_STATUS="PENDING"
  for _ in {1..40}; do
    PIPELINE_STATUS="$(duck --output json pipelines runs get "$PIPELINE_RUN_ID" | jq -r '.status')"
    if [[ "$PIPELINE_STATUS" == "SUCCESS" || "$PIPELINE_STATUS" == "FAILED" || "$PIPELINE_STATUS" == "CANCELLED" ]]; then
      break
    fi
    sleep 1
  done

  if [[ "$PIPELINE_STATUS" != "SUCCESS" ]]; then
    echo "pipeline run failed with status: $PIPELINE_STATUS" >&2
    duck --output json pipelines runs list-job-runs "$PIPELINE_RUN_ID"
    exit 1
  fi
else
  duck pipelines runs trigger movielens_daily
fi

echo "Spot-checking curated outputs"
duck query execute --sql "SELECT COUNT(*) AS total_rows FROM lake.main.gold_movie_scores"
duck query execute --sql "SELECT * FROM lake.main.gold_movie_scores ORDER BY avg_rating DESC LIMIT 5"

echo "Demo flow complete"
