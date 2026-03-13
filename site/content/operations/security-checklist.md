---
title: Security Checklist
description: Review the minimum controls required before exposing Duck to shared users or sensitive data.
---

# Security Checklist

Use this as a release gate before exposing the platform to shared users or sensitive data.

## Identity and auth

- OIDC is configured for user-facing environments, or the non-OIDC choice is explicitly justified
- API keys are limited to service or automation use cases
- local-only auth conveniences are disabled outside development
- principal and group ownership is documented

## Secrets and encryption

- `ENCRYPTION_KEY` is set from a managed secret source
- local JWT secrets are not reused in production
- secret values are not committed into config files or example artifacts
- secret rotation and recovery paths are known to operators

## Access policy

- grants follow least privilege
- row filters are in place where tenant or geography scoping is required
- column masks cover sensitive fields that should not be returned verbatim
- policy changes are verified through the real query path before release

## Runtime posture

- the correct listener addresses are exposed
- rate limits are configured for the intended load profile
- proxy trust is enabled only when a trusted reverse proxy is actually in front of the service
- remote compute fallback is configured intentionally rather than implicitly

## Operations

- health and metrics endpoints are monitored
- distributed compute rollout uses fallback intentionally
- upgrade and drift procedures are documented for the team
- health and metrics endpoints are connected to real alerting

## Warning Signs

- a shared environment depends on development auth shortcuts
- one admin API key is reused broadly across people and systems
- policy changes are applied without query-path verification
- a published product has no clear owner, request path, or freshness expectation

## Next Steps

- [Access Design](/govern/access-design)
- [Distributed Compute](/operations/distributed-compute)
- [Policy Verification](/govern/policy-verification)

## Related Reference

- [Privileges](/reference/privileges)
