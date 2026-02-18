# Landing Data

This directory is intentionally kept lightweight in git.

Run `examples/showcase-movielens/scripts/ingest_seed_data.sh` to generate
deterministic parquet files and register them through the ingestion API.

Generated files:

- `movies.parquet`
- `users.parquet`
- `ratings.parquet`
