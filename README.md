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
- **Async Remote Query Lifecycle** -- Remote agents support submit/status/results/cancel APIs for paged result retrieval
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
# or generate a tailored file
duck config init --mode hybrid --env development --output .env

# Build and run
task build
go run ./cmd/server
```

The server starts on `:8080` by default. API docs are available at `http://localhost:8080/docs`.

### Run with Docker

```bash
docker build -t duck-demo .
docker run -p 8080:8080 \
  -e AUTH_ISSUER_URL=https://issuer.example.com \
  -e AUTH_AUDIENCE=your-api-audience \
  -e ENCRYPTION_KEY=your-64-char-hex-key \
  -v duck-data:/data \
  duck-demo
```

### Compose Profiles

- Default/local stack: `docker-compose.yml`
- Continuous staging VPS stack: `deploy/staging/vps/docker-compose.yml`

## Configuration

All configuration is via environment variables. See `.env.sample` for a full reference.
Scenario templates are also available: `.env.local-only.sample`, `.env.hybrid.sample`, `.env.oidc-only.sample`, `.env.prod.sample`.

| Variable | Default | Description |
|---|---|---|
| `LISTEN_ADDR` | `:8080` | HTTP listen address |
| `FLIGHT_SQL_LISTEN_ADDR` | `:32010` | Flight SQL TCP listen address (used when `FEATURE_FLIGHT_SQL=true`) |
| `PG_WIRE_LISTEN_ADDR` | `:5433` | PostgreSQL wire TCP listen address (used when `FEATURE_PG_WIRE=true`) |
| `META_DB_PATH` | `ducklake_meta.sqlite` | SQLite metadata database path |
| `LOG_LEVEL` | `info` | Log level: debug, info, warn, error |
| `AUTH_ISSUER_URL` | `` | OIDC issuer URL for JWT validation |
| `AUTH_JWKS_URL` | `` | Optional JWKS URL override |
| `AUTH_AUDIENCE` | `` | Required audience for issuer-based validation |
| `AUTH_MODE` | `hybrid` | Auth policy: `hybrid`, `oidc_only`, `local_only`, `api_key_only` |
| `JWT_SECRET` | `` | Optional HS256 secret for local JWT auth |
| `AUTH_API_KEY_ENABLED` | `true` | Enable API key authentication |
| `AUTH_WEB_SESSION_IDLE_TTL` | `30m` | UI session idle timeout |
| `AUTH_WEB_SESSION_ABSOLUTE_TTL` | `24h` | UI session absolute max lifetime |
| `AUTH_WEB_SESSION_COOKIE_NAME` | `ui_session` | Opaque UI session cookie name |
| `AUTH_WEB_SESSION_REAPER_INTERVAL` | `5m` | Cleanup cadence for expired/revoked UI sessions |
| `ENCRYPTION_KEY` | (insecure default) | 64-char hex AES-256 key for credential encryption |
| `ENCRYPTION_KEY_FILE` | `` | Read encryption key from file (e.g. Docker/K8s secret mount) |
| `JWT_SECRET_FILE` | `` | Read local JWT secret from file |
| `ENV` | `development` | Set to `production` to enforce secure config |
| `TRUST_DOWNSTREAM_PROXY` | `false` | Allow HTTP listener in production when TLS is terminated by a trusted reverse proxy |
| `RATE_LIMIT_RPS` | `100` | Sustained requests per second |
| `RATE_LIMIT_BURST` | `200` | Maximum burst capacity |
| `FEATURE_INTERNAL_GRPC` | `true` | Enable internal gRPC worker transport (`grpc://`/`grpcs://` endpoint URLs) |
| `FEATURE_FLIGHT_SQL` | `true` | Enable Flight SQL listener |
| `FEATURE_PG_WIRE` | `true` | Enable PostgreSQL wire listener |

### Production Mode

Set `ENV=production` to enforce secure defaults. In production mode, the server will refuse to start unless `ENCRYPTION_KEY` (or `ENCRYPTION_KEY_FILE`) is configured and at least one auth method is enabled (OIDC, local JWT via `JWT_SECRET`, or API keys).

### Authentication

The server supports two authentication methods:

1. **OIDC/JWKS** -- Set `AUTH_ISSUER_URL` (and `AUTH_AUDIENCE`) for external identity providers
2. **API Keys** -- Create via the API; sent in the `X-API-Key` header

`AUTH_MODE` controls policy and precedence:

- `hybrid` (default): OIDC plus optional local JWT/API key support
- `oidc_only`: require OIDC config
- `local_only`: use local JWT (`JWT_SECRET`) and/or API keys
- `api_key_only`: API keys only

### UI Session Revocation

Interactive `/ui` access uses an opaque `ui_session` cookie backed by server-side session state.

- Logging out revokes the current session server-side immediately.
- Session expiry is enforced by both idle TTL and absolute TTL.
- Operators can force logout by revoking sessions for a principal (all current browser sessions become invalid immediately).
- API/CLI auth is separate (`/v1` bearer/API-key); revoking UI sessions does not rotate API keys or JWT signing configuration.

Admin API support:

- `POST /v1/auth/sessions/revoke-all` with `{"principal_id":"<id>"}` revokes all active UI sessions for that principal.
- `GET /v1/auth/sessions/stats` returns session lifecycle counters and current active session count.

### S3 Storage (Optional)

Set `S3_KEY_ID`, `S3_SECRET` (or `S3_SECRET_FILE`), `S3_ENDPOINT`, and `S3_REGION` to enable DuckLake catalog and ingestion features.

## Development

```bash
task build          # Build all packages
task test:unit      # Run unit tests
task test           # Run all tests (unit + integration)
task vet            # Run go vet
task lint           # Run all linters
task generate       # Regenerate all code (TypeSpec/apigen + sqlc)
task generate:declarative-schema # Regenerate declarative JSON Schema artifacts
task sqlc           # Regenerate DB query code
task generate:api   # Regenerate OpenAPI/server/CLI registry from JSON IR
```

Declarative schema artifacts are documented in `docs/declarative-schema.md`.
Distributed compute operations are documented in `docs/distributed-compute.md`.

## Examples

Run `examples/` to see declarative "data platform as code" configurations, including a Bronze/Silver/Gold transformation showcase.

- Example index and run instructions: `examples/README.md`
- Canonical showcase: `examples/showcase-movielens/README.md`

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
