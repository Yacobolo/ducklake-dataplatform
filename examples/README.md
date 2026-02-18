# Examples

This directory contains runnable declarative configurations that demonstrate DuckDB Data Platform as code.

## Available examples

- `showcase-movielens`: end-to-end Bronze/Silver/Gold transformation pipeline with macros, RBAC, and governance policies.

## Prerequisites

- Server running (`go run ./cmd/server`)
- CLI built (`task build-cli`) or run via `go run ./cmd/cli`

## Standard workflow

From repository root, run:

```bash
./bin/duck validate --config-dir examples/showcase-movielens/config
./bin/duck plan --config-dir examples/showcase-movielens/config
./bin/duck apply --config-dir examples/showcase-movielens/config --auto-approve
./bin/duck plan --config-dir examples/showcase-movielens/config
```

After apply, the second `plan` should report no further model/macro changes.

## Automated verification

- `task examples:test`: runs integration tests that load examples and verify apply/replan behavior.
- `task examples:validate`: runs offline declarative validation for each example config directory.

Note: examples tasks are intentionally local-only in v1 and are not wired into CI `check` yet.
