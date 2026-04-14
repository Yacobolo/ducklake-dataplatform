# Staging VPS deployment

This setup deploys `control-plane` + `compute-agent` + `postgres` with:

- `caddy` for HTTPS termination and reverse proxy
- production-mode control-plane with downstream proxy trust enabled (`TRUST_DOWNSTREAM_PROXY=true`)

Deploys are deterministic and CI-driven (no timer-based auto-updater).

The `Deploy Staging VPS` GitHub Actions workflow writes runtime values from GitHub Variables and Secrets into `.env` on the server during each deploy, then runs:

- `docker compose pull`
- `docker compose up -d --remove-orphans`

## Required GitHub repository variables

### SSH / target host

- `VPS_HOST`
- `VPS_PORT`
- `VPS_USER`
- `VPS_KNOWN_HOSTS`
- `VPS_APP_DIR`

### Image references

- images are fixed to `ghcr.io/yacobolo/quackstack-...` and use `IMAGE_TAG`

### Runtime config

- `PUBLIC_DOMAIN`
- `CORS_ALLOWED_ORIGINS` (recommended; defaults to `https://${PUBLIC_DOMAIN}`)
- `IMAGE_TAG` (optional for manual runs; workflow sets `sha-<commit>` automatically)
- `S3_ENDPOINT`
- `S3_REGION`
- `S3_BUCKET`
- `POSTGRES_DB`
- `POSTGRES_USER`
- `AGENT_MAX_MEMORY_GB` (optional; default `2`)
- `AGENT_QUERY_RESULT_TTL` (optional; default `10m`)
- `AGENT_QUERY_CLEANUP_INTERVAL` (optional; default `1m`)

## Required GitHub repository secrets

- `VPS_SSH_KEY`
- `AGENT_TOKEN`
- `JWT_SECRET`
- `ENCRYPTION_KEY`
- `S3_ACCESS_KEY`
- `S3_SECRET_KEY`
- `POSTGRES_PASSWORD`

`GHCR_TOKEN` is optional for public images and required only if you switch to private GHCR packages.

You can use `deploy/staging/vps/.env.example` as the source of truth for runtime values.
