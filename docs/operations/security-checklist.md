---
title: Security Checklist
description: Review the minimum controls to run Duck in a shared or production environment.
---

# Security Checklist

Use this as a release gate before exposing the platform to shared users or sensitive data.

## Identity and auth

- OIDC is configured for user-facing environments, or the non-OIDC choice is explicitly justified
- API keys are limited to service or automation use cases
- local-only auth conveniences are disabled outside development

## Secrets and encryption

- `ENCRYPTION_KEY` is set from a managed secret source
- local JWT secrets are not reused in production
- secret values are not committed into config files or example artifacts

## Access policy

- grants follow least privilege
- row filters are in place where tenant or geography scoping is required
- column masks cover sensitive fields that should not be returned verbatim

## Runtime posture

- the correct listener addresses are exposed
- rate limits are configured for the intended load profile
- proxy trust is enabled only when a trusted reverse proxy is actually in front of the service

## Operations

- health and metrics endpoints are monitored
- distributed compute rollout uses fallback intentionally
- upgrade and drift procedures are documented for the team

## Warning Signs

- a shared environment depends on development auth shortcuts
- one admin API key is reused broadly across people and systems
- policy changes are applied without query-path verification

## Next Steps

- [Manage Access](/how-to/access-control)
- [Distributed Compute](/operations/distributed-compute)

## Related Reference

- [Platform Objects](/core-concepts/access-control)
- [Advanced API Reference](/reference/api)
