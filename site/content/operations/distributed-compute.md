---
title: Distributed Compute
description: Roll out remote workers without moving identity, policy, or governance out of the control plane.
---

# Distributed Compute

This runbook describes how to roll out remote compute without weakening Duck’s security and governance model.

## Architecture Boundaries

- the gateway remains the single policy enforcement point
- workers execute already-rewritten SQL
- gateway-to-worker transport uses internal gRPC
- storage, auth, and governance metadata remain anchored in the control plane

## When to Use Remote Compute

- you need worker isolation or a separate execution fleet
- you want lifecycle-style async execution
- you need a staged rollout with local fallback
- query or orchestration load makes local-only execution an operator bottleneck

## Admin Checklist

- confirm the gateway feature flags match the intended rollout
- set worker auth and listen addresses explicitly
- start with fallback enabled on assignments
- canary a small set of users or groups before widening traffic
- monitor queue latency and failure reasons before widening scope

## Worker Settings

- `AGENT_TOKEN`
- `LISTEN_ADDR`
- `GRPC_LISTEN_ADDR`
- `MAX_MEMORY_GB`
- `QUERY_RESULT_TTL`
- `QUERY_CLEANUP_INTERVAL`

## Gateway Settings

- `FEATURE_REMOTE_ROUTING`
- `FEATURE_ASYNC_QUEUE`
- `FEATURE_CURSOR_MODE`
- `FEATURE_INTERNAL_GRPC`
- `REMOTE_CANARY_USERS`

## Health and Failure Handling

- monitor `GET /health` and `GET /metrics`
- expect fallback behavior when worker health degrades and assignments allow local execution
- use retention settings to control in-memory lifecycle result pressure
- document the operator decision for when fallback should be automatic versus disabled

## Rollout Sequence

1. enable remote support with local fallback
2. route a limited audience
3. observe queue latency and completion behavior
4. widen scope only after representative success

## Next Steps

<div class="site-card-grid">
  <a class="site-card" href="/operations/configuration" style="position: relative; display: flex; align-items: center; gap: 0.875rem; padding: 1rem 1.25rem; border: 1px solid var(--site-border); background: transparent; color: inherit; text-decoration: none;"><span aria-hidden="true" style="display: inline-flex; height: 3rem; width: 3rem; align-items: center; justify-content: center; border-radius: 1rem; background: color-mix(in srgb, var(--site-accent) 12%, transparent); color: var(--site-accent-strong);"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M6 5h12v14H6z"></path><path d="M9 9h6"></path><path d="M9 13h6"></path></svg></span><span style="flex: 1; min-width: 0; text-align: left;"><strong style="display: block; color: var(--site-ink); font-size: 0.98rem;">Platform Settings</strong><span style="display: block; margin-top: 0.2rem; color: var(--site-muted); font-size: 0.875rem; line-height: 1.35;">Configure gateway and workers.</span></span></a>
  <a class="site-card" href="/operations/security-checklist" style="position: relative; display: flex; align-items: center; gap: 0.875rem; padding: 1rem 1.25rem; border: 1px solid var(--site-border); background: transparent; color: inherit; text-decoration: none;"><span aria-hidden="true" style="display: inline-flex; height: 3rem; width: 3rem; align-items: center; justify-content: center; border-radius: 1rem; background: color-mix(in srgb, var(--site-accent) 12%, transparent); color: var(--site-accent-strong);"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M12 3l7 3v5c0 5-3 8-7 10-4-2-7-5-7-10V6l7-3z"></path></svg></span><span style="flex: 1; min-width: 0; text-align: left;"><strong style="display: block; color: var(--site-ink); font-size: 0.98rem;">Security Checklist</strong><span style="display: block; margin-top: 0.2rem; color: var(--site-muted); font-size: 0.875rem; line-height: 1.35;">Check rollout hardening.</span></span></a>
  <a class="site-card" href="/operations/observability-and-troubleshooting" style="position: relative; display: flex; align-items: center; gap: 0.875rem; padding: 1rem 1.25rem; border: 1px solid var(--site-border); background: transparent; color: inherit; text-decoration: none;"><span aria-hidden="true" style="display: inline-flex; height: 3rem; width: 3rem; align-items: center; justify-content: center; border-radius: 1rem; background: color-mix(in srgb, var(--site-accent) 12%, transparent); color: var(--site-accent-strong);"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M5 18V9"></path><path d="M12 18V5"></path><path d="M19 18v-6"></path><path d="M4 18h16"></path></svg></span><span style="flex: 1; min-width: 0; text-align: left;"><strong style="display: block; color: var(--site-ink); font-size: 0.98rem;">Observability And Troubleshooting</strong><span style="display: block; margin-top: 0.2rem; color: var(--site-muted); font-size: 0.875rem; line-height: 1.35;">Debug queue and worker issues.</span></span></a>
</div>

## Related Reference

- [Compute API](/reference/generated/api/endpoints/compute)
