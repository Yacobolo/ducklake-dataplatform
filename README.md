# Duck Data Platform

Duck is a secure SQL platform built on DuckDB with RBAC, row-level security, column masking, declarative configuration, and optional remote compute.

## Start With the Docs

Canonical product guidance now lives under [`/site/content`](./site/content).

- Product entrypoint: [`site/content/index.md`](./site/content/index.md)
- Quickstart: [`site/content/start-here/quickstart.md`](./site/content/start-here/quickstart.md)
- Core concepts: [`site/content/core-concepts/index.md`](./site/content/core-concepts/index.md)
- How-to guides: [`site/content/how-to/index.md`](./site/content/how-to/index.md)
- Operations: [`site/content/operations/index.md`](./site/content/operations/index.md)
- Reference: [`site/content/reference/index.md`](./site/content/reference/index.md)

## Local Development

Prerequisites:

- Go `1.25+`
- [Task](https://taskfile.dev/)

Run the local developer workflow:

```bash
task dev
```

Key commands:

```bash
task build
task test
task lint
task build-cli
```

## Docs Workflow

```bash
task site:dev
task site:build
task docs:generate
task site:check
```

The canonical public API contract is `api/gen/openapi.yaml`.
Generated reference under `site/content/reference/generated` is derived from that contract plus the declarative schema artifacts.

## Examples

Start with the MovieLens showcase:

- [`examples/README.md`](./examples/README.md)
- [`examples/showcase-movielens/README.md`](./examples/showcase-movielens/README.md)

## API Surface

- Interactive server docs: `GET /docs`
- OpenAPI spec: `GET /openapi.json`
- Health check: `GET /healthz`

```
cmd/server/             -- HTTP server entry point
cmd/compute-agent/      -- Remote compute agent binary
cmd/cli/                -- CLI client
internal/api/           -- HTTP handlers and APIGen-generated transport glue
internal/service/       -- Business logic (depends on domain interfaces only)
internal/domain/        -- Types, interfaces, errors (zero external deps)
internal/db/repository/ -- Implements domain repository interfaces
internal/db/dbstore/    -- sqlc-generated code (do not edit)
internal/db/migrations/ -- Goose SQL migrations
internal/engine/        -- SecureEngine (DuckDB + RBAC + RLS + column masking)
internal/sqlrewrite/    -- SQL parsing/rewriting via pg_query_go
internal/middleware/    -- JWT, API key, rate limiting, request-ID, CORS
internal/config/        -- Environment-based configuration
extension/duck_access/  -- C++ DuckDB client extension
```

Dependency direction: `api` -> `service` -> `domain` <- `repository`. Never import upward.

APIGen owns the server transport, compatibility API types, and generated CLI metadata from `api/gen/json-ir.json`.
