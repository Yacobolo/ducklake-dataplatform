---
title: Platform Settings
description: Configure identity, runtime, networking, storage, and compute with a production-first baseline.
---

# Platform Settings

Duck is configured through environment variables and deployment settings. Treat configuration as part of the product contract for the platform itself.

## Required Production Baseline

Before a production deployment, confirm:

- `ENV=production`
- `ENCRYPTION_KEY` or `ENCRYPTION_KEY_FILE` is configured
- at least one real authentication path is enabled
- listener addresses match your network boundaries
- storage credentials and external locations are intentionally managed

## Important Configuration Areas

### Auth and identity

- `AUTH_MODE`
- `AUTH_ISSUER_URL`
- `AUTH_AUDIENCE`
- `JWT_SECRET` or `JWT_SECRET_FILE`
- `AUTH_API_KEY_ENABLED`

### Runtime, networking, and storage

- `LISTEN_ADDR`
- `FLIGHT_SQL_LISTEN_ADDR`
- `PG_WIRE_LISTEN_ADDR`
- `TRUST_DOWNSTREAM_PROXY`
- storage and external location settings that match your deployment

### Security and encryption

- `ENCRYPTION_KEY`
- `ENCRYPTION_KEY_FILE`
- rate-limit settings

### Feature gates and compute

- `FEATURE_INTERNAL_GRPC`
- `FEATURE_FLIGHT_SQL`
- `FEATURE_PG_WIRE`
- remote compute feature flags as needed
- integration and Git sync controls where enabled

## Recommended Practice

- use deployment templates as a starting point, not as production truth
- prefer file-based secret injection in managed environments
- keep environment definitions versioned alongside deployment artifacts
- document the meaning of every environment-specific override for operators on call

## Next Steps

- [Security Checklist](/operations/security-checklist)
- [Ways to Access Duck](/start-here/deployment-modes)

## Related Reference

- [First Operator Setup](/start-here/first-operator-setup)
- [Distributed Compute](/operations/distributed-compute)
