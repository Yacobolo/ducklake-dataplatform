---
title: Platform Settings
description: Configure identity, runtime, networking, storage, and compute with a production-first baseline.
---

# Platform Settings

QuackStack is configured through environment variables and deployment settings. Treat configuration as part of the product contract for the platform itself.

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

<div class="site-card-grid">
  <a class="site-card" href="/operations/security-checklist" style="position: relative; display: flex; align-items: center; gap: 0.875rem; padding: 1rem 1.25rem; border: 1px solid var(--borderColor-default); background: transparent; color: inherit; text-decoration: none;"><span aria-hidden="true" style="display: inline-flex; height: 3rem; width: 3rem; align-items: center; justify-content: center; border-radius: 1rem; background: color-mix(in srgb, var(--fgColor-accent) 12%, transparent); color: var(--fgColor-accent);"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M12 3l7 3v5c0 5-3 8-7 10-4-2-7-5-7-10V6l7-3z"></path></svg></span><span style="flex: 1; min-width: 0; text-align: left;"><strong style="display: block; color: var(--fgColor-default); font-size: 0.98rem;">Security Checklist</strong><span style="display: block; margin-top: 0.2rem; color: var(--fgColor-muted); font-size: 0.875rem; line-height: 1.35;">Validate the hardening baseline.</span></span></a>
  <a class="site-card" href="/start-here/deployment-modes" style="position: relative; display: flex; align-items: center; gap: 0.875rem; padding: 1rem 1.25rem; border: 1px solid var(--borderColor-default); background: transparent; color: inherit; text-decoration: none;"><span aria-hidden="true" style="display: inline-flex; height: 3rem; width: 3rem; align-items: center; justify-content: center; border-radius: 1rem; background: color-mix(in srgb, var(--fgColor-accent) 12%, transparent); color: var(--fgColor-accent);"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M4 7h16"></path><path d="M7 12h10"></path><path d="M9 17h6"></path></svg></span><span style="flex: 1; min-width: 0; text-align: left;"><strong style="display: block; color: var(--fgColor-default); font-size: 0.98rem;">Ways to Access QuackStack</strong><span style="display: block; margin-top: 0.2rem; color: var(--fgColor-muted); font-size: 0.875rem; line-height: 1.35;">See the supported access modes.</span></span></a>
</div>

## Related Reference

- [First Operator Setup](/start-here/first-operator-setup)
- [Distributed Compute](/operations/distributed-compute)
