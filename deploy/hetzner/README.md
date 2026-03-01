# Hetzner test deployment (GitHub Secrets only)

This setup deploys `control-plane` + `compute-agent` + `postgres` with:

- `caddy` for HTTPS termination and reverse proxy

Deploys are deterministic and CI-driven (no timer-based auto-updater).

The `Deploy Hetzner` GitHub Actions workflow writes runtime values from GitHub Secrets into `.env` on the server during each deploy, then runs:

- `docker compose pull`
- `docker compose up -d --remove-orphans`

## Required GitHub repository secrets

### SSH / target host

- `HETZNER_HOST`
- `HETZNER_PORT`
- `HETZNER_USER`
- `HETZNER_SSH_KEY`
- `HETZNER_KNOWN_HOSTS`
- `HETZNER_APP_DIR`

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

You can use `deploy/hetzner/.env.example` as the source of truth for runtime values.
