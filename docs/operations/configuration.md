---
title: Platform Settings
description: Use Duck's platform settings safely in shared and production environments.
---

# Platform Settings

Duck is configured through environment variables and deployment settings. Treat the platform configuration surface as part of your operating model.

## Required Production Baseline

Before a production deployment, confirm:

- `ENV=production`
- `ENCRYPTION_KEY` or `ENCRYPTION_KEY_FILE` is configured
- at least one real authentication path is enabled
- listener addresses match your network boundaries

## Important Configuration Areas

### Auth and identity

- `AUTH_MODE`
- `AUTH_ISSUER_URL`
- `AUTH_AUDIENCE`
- `JWT_SECRET` or `JWT_SECRET_FILE`
- `AUTH_API_KEY_ENABLED`

### Runtime and networking

- `LISTEN_ADDR`
- `FLIGHT_SQL_LISTEN_ADDR`
- `PG_WIRE_LISTEN_ADDR`
- `TRUST_DOWNSTREAM_PROXY`

### Security and encryption

- `ENCRYPTION_KEY`
- `ENCRYPTION_KEY_FILE`
- rate-limit settings

### Feature gates and compute

- `FEATURE_INTERNAL_GRPC`
- `FEATURE_FLIGHT_SQL`
- `FEATURE_PG_WIRE`
- remote compute feature flags as needed

## Recommended Practice

- use deployment templates as a starting point, not as production truth
- prefer file-based secret injection in managed environments
- keep environment definitions versioned alongside deployment artifacts

## Next Steps

- [Security Checklist](/operations/security-checklist)
- [Ways to Access Duck](/start-here/deployment-modes)

## Related Reference

- [Get Started](/start-here/)
- [Distributed Compute](/operations/distributed-compute)
