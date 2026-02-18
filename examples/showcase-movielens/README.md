# MovieLens Showcase

This is the flagship end-to-end showcase for the platform:

1. Load raw data through the ingestion API.
2. Transform it with declarative models (bronze -> silver -> gold).
3. Reuse a declarative macro in transformation SQL.
4. Run a notebook with KPI checks.
5. Schedule the notebook in a pipeline.
6. Demonstrate RBAC, row filters, and column masks on MovieLens tables.

## Directory layout

- `config/`: declarative resources (catalog, tables, security, governance, models, macros, notebooks, pipelines)
- `data/`: tiny deterministic MovieLens CSV seed set
- `scripts/`: helper scripts for local bootstrap and demo execution
- `assertions.yaml`: example lifecycle expectations used by integration tests

## Quickstart (10-15 minutes)

From repository root:

```bash
task build
task build-cli
go run ./cmd/server
```

In a second terminal:

```bash
export API_KEY="showcase-local-admin-key"
examples/showcase-movielens/scripts/bootstrap_admin_key.sh

./bin/duck --token '' --api-key "$API_KEY" validate --config-dir examples/showcase-movielens/config
./bin/duck --token '' --api-key "$API_KEY" plan --config-dir examples/showcase-movielens/config
./bin/duck --token '' --api-key "$API_KEY" apply --config-dir examples/showcase-movielens/config --auto-approve

examples/showcase-movielens/scripts/ingest_seed_data.sh

./bin/duck --token '' --api-key "$API_KEY" models model-runs trigger-model-run \
  --project-name movielens \
  --model-names bronze_movies,bronze_users,bronze_ratings,silver_movies,silver_users,silver_ratings_enriched,gold_movie_scores,gold_user_engagement \
  --target-catalog lake \
  --target-schema main

./bin/duck --token '' --api-key "$API_KEY" pipelines runs trigger movielens_daily
./bin/duck --token '' --api-key "$API_KEY" query execute --sql "SELECT * FROM lake.main.gold_movie_scores ORDER BY avg_rating DESC LIMIT 5"
```

Or run the full happy path in one command:

```bash
API_KEY="showcase-local-admin-key" examples/showcase-movielens/scripts/run_demo_flow.sh
```

## Deep Dive

### Ingestion API

- `scripts/ingest_seed_data.sh` converts seed CSV files to parquet, then calls `ingestion load` for `raw_movies`, `raw_users`, and `raw_ratings`.
- Raw table definitions live under `config/catalogs/lake/schemas/main/tables/raw_*`.

### Models + Macro

- Medallion models are under `config/models/movielens/bronze|silver|gold`.
- The `rating_bucket` macro is declared in `config/macros/rating_bucket.yaml` and used by `silver_ratings_enriched`.

### Notebook + Pipeline

- Notebook: `config/notebooks/01_kpi_walkthrough.yaml`
- Scheduled pipeline: `config/pipelines/movielens_daily.yaml`

### Governance + RBAC

- Grants: `config/security/grants.yaml`
- Row filter + column mask on `raw_users`: `config/catalogs/lake/schemas/main/tables/raw_users/`
- Table tags: `config/governance/tags.yaml`

## Troubleshooting

- If helper scripts fail, ensure the server is running and `./bin/duck` exists (`task build-cli`).
- If bootstrap fails, confirm `ducklake_meta.sqlite` exists in repo root (server startup creates it).
- If ingestion fails with file-path issues, verify `ducklake_data/showcase_ingest/*.parquet` exists after running `ingest_seed_data.sh`.

## Dataset attribution

MovieLens datasets are from GroupLens Research:

- F. Maxwell Harper and Joseph A. Konstan. 2015.
  The MovieLens Datasets: History and Context.
  ACM Transactions on Interactive Intelligent Systems (TiiS) 5, 4, Article 19.

This repository includes only a tiny handcrafted subset for demonstration and testing.
