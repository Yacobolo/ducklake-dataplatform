# Staging VPS deployment (GitHub Secrets only)

This setup deploys `control-plane` + `compute-agent` + `postgres` with:

- `caddy` for HTTPS termination and reverse proxy

Deploys are deterministic and CI-driven (no timer-based auto-updater).

The `Deploy Staging VPS` GitHub Actions workflow writes runtime values from GitHub Secrets into `.env` on the server during each deploy, then runs:

- `docker compose pull`
- `docker compose up -d --remove-orphans`

## Required GitHub repository secrets

### SSH / target host

- `VPS_HOST`
- `VPS_PORT`
- `VPS_USER`
- `VPS_SSH_KEY`
- `VPS_KNOWN_HOSTS`
- `VPS_APP_DIR`

### GHCR pull credentials

- `GHCR_USERNAME`
- `GHCR_TOKEN`

### Runtime config

- `PUBLIC_DOMAIN`
- `GHCR_OWNER`
- `GHCR_REPO`
- `AGENT_TOKEN`
- `JWT_SECRET`
- `ENCRYPTION_KEY`
- `S3_ACCESS_KEY`
- `S3_SECRET_KEY`
- `S3_ENDPOINT`
- `S3_REGION`
- `S3_BUCKET`
- `POSTGRES_DB`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- `AGENT_MAX_MEMORY_GB`
- `AGENT_QUERY_RESULT_TTL`
- `AGENT_QUERY_CLEANUP_INTERVAL`

You can use `deploy/staging/vps/.env.example` as the source of truth for runtime values.
