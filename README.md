# DuckDB Data Platform

Secure SQL query layer over DuckDB with RBAC, row-level security, and column masking. Uses DuckDB for analytics and SQLite as a metadata/permissions store.

## Features

- **SQL Query Engine** -- Execute SQL queries through a secure proxy that enforces access controls
- **RBAC** -- Role-based access control with principals, groups, and privilege grants
- **Row-Level Security** -- Filter rows per principal with configurable SQL predicates
- **Column Masking** -- Mask sensitive columns with custom expressions per principal
- **Multi-Catalog** -- Register and manage multiple DuckLake catalogs (SQLite or PostgreSQL metastores)
- **Data Governance** -- Tags, classifications, lineage tracking, audit logs, and search
- **Storage Management** -- Storage credentials (S3/Azure/GCS), external locations, and volumes
- **Ingestion** -- Upload and load data into managed tables via presigned URLs
- **Compute Routing** -- Route queries to local or remote DuckDB compute endpoints
- **API Key Auth** -- Create and manage API keys alongside JWT/OIDC authentication
- **DuckDB Extension** -- Client-side DuckDB extension for transparent table virtualization

## Quick Start

### Prerequisites

- Go 1.25+
- [Task](https://taskfile.dev/) (task runner)

### Run Locally

```bash
# Copy and configure environment
cp .env.sample .env

# Build and run
task build
go run ./cmd/server
```

The server starts on `:8080` by default. API docs are available at `http://localhost:8080/docs`.

### Run with Docker

```bash
docker build -t duck-demo .
docker run -p 8080:8080 \
  -e JWT_SECRET=your-secret-here \
  -e ENCRYPTION_KEY=your-64-char-hex-key \
  -v duck-data:/data \
  duck-demo
```

## Configuration

All configuration is via environment variables. See `.env.sample` for a full reference.

| Variable | Default | Description |
|---|---|---|
| `LISTEN_ADDR` | `:8080` | HTTP listen address |
| `META_DB_PATH` | `ducklake_meta.sqlite` | SQLite metadata database path |
| `LOG_LEVEL` | `info` | Log level: debug, info, warn, error |
| `JWT_SECRET` | (insecure default) | HS256 JWT signing secret |
| `ENCRYPTION_KEY` | (insecure default) | 64-char hex AES-256 key for credential encryption |
| `ENV` | `development` | Set to `production` to enforce secure config |
| `RATE_LIMIT_RPS` | `100` | Sustained requests per second |
| `RATE_LIMIT_BURST` | `200` | Maximum burst capacity |

### Production Mode

Set `ENV=production` to enforce secure defaults. In production mode, the server will refuse to start if `JWT_SECRET` or `ENCRYPTION_KEY` are not explicitly set.

### Authentication

The server supports three authentication methods:

1. **OIDC/JWKS** -- Set `AUTH_ISSUER_URL` (and `AUTH_AUDIENCE`) for external identity providers
2. **Shared Secret JWT** -- Set `JWT_SECRET` for HS256 token verification (dev/backward compat)
3. **API Keys** -- Create via the API; sent in the `X-API-Key` header

### S3 Storage (Optional)

Set `KEY_ID`, `SECRET`, `ENDPOINT`, and `REGION` to enable DuckLake catalog and ingestion features.

## Development

```bash
task build          # Build all packages
task test:unit      # Run unit tests
task test           # Run all tests (unit + integration)
task vet            # Run go vet
task lint           # Run all linters
task generate       # Regenerate all code (API types + sqlc + CLI)
task sqlc           # Regenerate DB query code
task generate-api   # Regenerate API types/server from openapi.yaml
```

## Architecture

```
cmd/server/             -- HTTP server entry point
cmd/compute-agent/      -- Remote compute agent binary
cmd/cli/                -- CLI client
internal/api/           -- HTTP handlers (generated StrictServerInterface)
internal/service/       -- Business logic (depends on domain interfaces only)
internal/domain/        -- Types, interfaces, errors (zero external deps)
internal/db/repository/ -- Implements domain repository interfaces
internal/db/dbstore/    -- sqlc-generated code (do not edit)
internal/db/migrations/ -- Goose SQL migrations
internal/engine/        -- SecureEngine (DuckDB + RBAC + RLS + column masking)
internal/sqlrewrite/    -- SQL parsing/rewriting via pg_query_go
internal/middleware/     -- JWT, API key, rate limiting, request-ID, CORS
internal/config/        -- Environment-based configuration
extension/duck_access/  -- C++ DuckDB client extension
```

Dependency direction: `api` -> `service` -> `domain` <- `repository`. Never import upward.

## API Documentation

- Interactive docs: `GET /docs` (Scalar API reference)
- OpenAPI spec: `GET /openapi.json`
- Health check: `GET /healthz`

## License

See LICENSE file for details.
