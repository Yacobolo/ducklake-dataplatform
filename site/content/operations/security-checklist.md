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

<div class="site-card-grid">
  <a class="site-card" href="/govern/access-design" style="position: relative; display: flex; align-items: center; gap: 0.875rem; padding: 1rem 1.25rem; border: 1px solid var(--site-border); background: transparent; color: inherit; text-decoration: none;"><span aria-hidden="true" style="display: inline-flex; height: 3rem; width: 3rem; align-items: center; justify-content: center; border-radius: 1rem; background: color-mix(in srgb, var(--site-accent) 12%, transparent); color: var(--site-accent-strong);"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M12 3l7 3v5c0 5-3 8-7 10-4-2-7-5-7-10V6l7-3z"></path></svg></span><span style="flex: 1; min-width: 0; text-align: left;"><strong style="display: block; color: var(--site-ink); font-size: 0.98rem;">Access Design</strong><span style="display: block; margin-top: 0.2rem; color: var(--site-muted); font-size: 0.875rem; line-height: 1.35;">Model least-privilege access.</span></span></a>
  <a class="site-card" href="/operations/distributed-compute" style="position: relative; display: flex; align-items: center; gap: 0.875rem; padding: 1rem 1.25rem; border: 1px solid var(--site-border); background: transparent; color: inherit; text-decoration: none;"><span aria-hidden="true" style="display: inline-flex; height: 3rem; width: 3rem; align-items: center; justify-content: center; border-radius: 1rem; background: color-mix(in srgb, var(--site-accent) 12%, transparent); color: var(--site-accent-strong);"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><rect x="4" y="5" width="16" height="6" rx="1"></rect><rect x="7" y="13" width="10" height="6" rx="1"></rect></svg></span><span style="flex: 1; min-width: 0; text-align: left;"><strong style="display: block; color: var(--site-ink); font-size: 0.98rem;">Distributed Compute</strong><span style="display: block; margin-top: 0.2rem; color: var(--site-muted); font-size: 0.875rem; line-height: 1.35;">Roll out workers safely.</span></span></a>
  <a class="site-card" href="/govern/policy-verification" style="position: relative; display: flex; align-items: center; gap: 0.875rem; padding: 1rem 1.25rem; border: 1px solid var(--site-border); background: transparent; color: inherit; text-decoration: none;"><span aria-hidden="true" style="display: inline-flex; height: 3rem; width: 3rem; align-items: center; justify-content: center; border-radius: 1rem; background: color-mix(in srgb, var(--site-accent) 12%, transparent); color: var(--site-accent-strong);"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M5 18V9"></path><path d="M12 18V5"></path><path d="M19 18v-6"></path><path d="M4 18h16"></path></svg></span><span style="flex: 1; min-width: 0; text-align: left;"><strong style="display: block; color: var(--site-ink); font-size: 0.98rem;">Policy Verification</strong><span style="display: block; margin-top: 0.2rem; color: var(--site-muted); font-size: 0.875rem; line-height: 1.35;">Test policies through real queries.</span></span></a>
</div>

## Related Reference

- [Privileges](/reference/privileges)
