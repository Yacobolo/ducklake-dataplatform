# Examples

This directory contains runnable product showcases.

## Flagship showcase

- `showcase-movielens`: ingestion API -> models (bronze/silver/gold) -> macro reuse -> notebook -> scheduled pipeline -> RBAC/RLS/column masking.

## Prerequisites

- Server running (`go run ./cmd/server`)
- CLI built (`task build-cli`) or run via `go run ./cmd/cli`

## Fastest path

From repository root:

```bash
export API_KEY="showcase-local-admin-key"
examples/showcase-movielens/scripts/bootstrap_admin_key.sh
API_KEY="$API_KEY" examples/showcase-movielens/scripts/run_demo_flow.sh
```

## Declarative workflow

From repository root, run:

```bash
./bin/duck --token '' --api-key "$API_KEY" validate --config-dir examples/showcase-movielens/config
./bin/duck --token '' --api-key "$API_KEY" plan --config-dir examples/showcase-movielens/config
./bin/duck --token '' --api-key "$API_KEY" apply --config-dir examples/showcase-movielens/config --auto-approve
./bin/duck --token '' --api-key "$API_KEY" plan --config-dir examples/showcase-movielens/config
```

See `examples/showcase-movielens/README.md` for full quickstart and feature walkthrough.

After apply, the second `plan` should report no further declarative changes.

## Automated verification

- `task examples:test`: runs integration tests that load examples and verify apply/replan behavior.
- `task examples:validate`: runs offline declarative validation for each example config directory.

Note: examples tasks are intentionally local-only in v1 and are not wired into CI `check` yet.
