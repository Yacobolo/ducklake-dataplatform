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

```bash
./bin/duck validate --config-dir examples/showcase-movielens/config
./bin/duck plan --config-dir examples/showcase-movielens/config
./bin/duck apply --config-dir examples/showcase-movielens/config --auto-approve
./bin/duck plan --config-dir examples/showcase-movielens/config
```

## Dataset attribution

MovieLens datasets are from GroupLens Research:

- F. Maxwell Harper and Joseph A. Konstan. 2015.
  The MovieLens Datasets: History and Context.
  ACM Transactions on Interactive Intelligent Systems (TiiS) 5, 4, Article 19.

This repository includes only a tiny handcrafted subset for demonstration and testing.
