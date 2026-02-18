# MovieLens Showcase

This showcase demonstrates a layered declarative analytics project:

- Bronze: raw ingested records from MovieLens subset files.
- Silver: conformed entities and cleaned fact rows.
- Gold: business-facing aggregates for analytics consumption.

It also includes governance-as-code resources (tags, row filters, column masks) and RBAC declarations.

## Directory layout

- `config/`: declarative resources (`models`, `macros`, `security`, `governance`, `catalogs`)
- `data/`: small deterministic MovieLens subset used by Bronze model SQL
- `assertions.yaml`: optional expectations used by integration tests

## Run

For local demos, bootstrap a one-time admin API key directly in your metadata SQLite before running declarative apply.

Example bootstrap (replace paths/DB as needed):

```bash
API_KEY="showcase-local-admin-key"
HASH=$(printf "%s" "$API_KEY" | shasum -a 256 | awk '{print $1}')

sqlite3 ducklake_meta.sqlite "
INSERT OR IGNORE INTO principals(id,name,type,is_admin)
VALUES ('showcase-admin-id','ml_admin','user',1);

INSERT OR IGNORE INTO api_keys(id,key_hash,principal_id,name,key_prefix)
VALUES (
  'showcase-admin-key-id',
  '$HASH',
  (SELECT id FROM principals WHERE name='ml_admin'),
  'showcase-admin',
  'showcase'
);
"
```

Use that key for CLI commands (and force empty token so API key auth is used):

```bash
./bin/duck --token '' --api-key "$API_KEY" validate --config-dir examples/showcase-movielens/config
./bin/duck --token '' --api-key "$API_KEY" plan --config-dir examples/showcase-movielens/config
./bin/duck --token '' --api-key "$API_KEY" apply --config-dir examples/showcase-movielens/config --auto-approve
./bin/duck --token '' --api-key "$API_KEY" plan --config-dir examples/showcase-movielens/config
```

## Dataset attribution

MovieLens datasets are from GroupLens Research:

- F. Maxwell Harper and Joseph A. Konstan. 2015.
  The MovieLens Datasets: History and Context.
  ACM Transactions on Interactive Intelligent Systems (TiiS) 5, 4, Article 19.

This repository includes only a tiny handcrafted subset for demonstration and testing.
