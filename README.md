# Duck Data Platform

Duck is a secure SQL platform built on DuckDB with RBAC, row-level security, column masking, declarative configuration, and optional remote compute.

## Start With the Docs

Canonical product guidance now lives under [`/docs`](./docs).

- Product entrypoint: [`docs/index.md`](./docs/index.md)
- Quickstart: [`docs/start-here/quickstart.md`](./docs/start-here/quickstart.md)
- Core concepts: [`docs/core-concepts/index.md`](./docs/core-concepts/index.md)
- How-to guides: [`docs/how-to/index.md`](./docs/how-to/index.md)
- Operations: [`docs/operations/index.md`](./docs/operations/index.md)
- Reference: [`docs/reference/index.md`](./docs/reference/index.md)

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
task docs:dev
task docs:build
task docs:generate
task docs:check
```

Generated reference lives under `docs/reference/generated` and should not be edited by hand.

## Examples

Start with the MovieLens showcase:

- [`examples/README.md`](./examples/README.md)
- [`examples/showcase-movielens/README.md`](./examples/showcase-movielens/README.md)

## API Surface

- Interactive server docs: `GET /docs`
- OpenAPI spec: `GET /openapi.json`
- Health check: `GET /healthz`
