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

## Remote Compute Settings

| Setting | Applies To | Why It Matters |
| --- | --- | --- |
| `AGENT_TOKEN` | Worker | Authenticates the worker to the control plane |
| `LISTEN_ADDR` | Worker | Binds the worker’s public listener correctly |
| `GRPC_LISTEN_ADDR` | Worker | Exposes the internal gRPC path for execution traffic |
| `MAX_MEMORY_GB` | Worker | Caps worker memory for safer isolation |
| `QUERY_RESULT_TTL` | Worker | Controls how long async results stay available |
| `QUERY_CLEANUP_INTERVAL` | Worker | Governs lifecycle cleanup pressure |
| `FEATURE_REMOTE_ROUTING` | Gateway | Enables routing work to remote workers |
| `FEATURE_ASYNC_QUEUE` | Gateway | Turns on queued async execution behavior |
| `FEATURE_CURSOR_MODE` | Gateway | Affects cursor-style remote result handling |
| `FEATURE_INTERNAL_GRPC` | Gateway | Enables the internal transport to workers |
| `REMOTE_CANARY_USERS` | Gateway | Limits early rollout to a known audience |

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
  <a class="site-card" href="/operations/configuration" style="position: relative; display: flex; align-items: center; gap: 0.875rem; padding: 1rem 1.25rem; border: 1px solid var(--borderColor-default); background: transparent; color: inherit; text-decoration: none;"><span aria-hidden="true" style="display: inline-flex; height: 3rem; width: 3rem; align-items: center; justify-content: center; border-radius: 1rem; background: color-mix(in srgb, var(--fgColor-accent) 12%, transparent); color: var(--fgColor-accent);"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M6 5h12v14H6z"></path><path d="M9 9h6"></path><path d="M9 13h6"></path></svg></span><span style="flex: 1; min-width: 0; text-align: left;"><strong style="display: block; color: var(--fgColor-default); font-size: 0.98rem;">Platform Settings</strong><span style="display: block; margin-top: 0.2rem; color: var(--fgColor-muted); font-size: 0.875rem; line-height: 1.35;">Configure gateway and workers.</span></span></a>
  <a class="site-card" href="/operations/security-checklist" style="position: relative; display: flex; align-items: center; gap: 0.875rem; padding: 1rem 1.25rem; border: 1px solid var(--borderColor-default); background: transparent; color: inherit; text-decoration: none;"><span aria-hidden="true" style="display: inline-flex; height: 3rem; width: 3rem; align-items: center; justify-content: center; border-radius: 1rem; background: color-mix(in srgb, var(--fgColor-accent) 12%, transparent); color: var(--fgColor-accent);"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M12 3l7 3v5c0 5-3 8-7 10-4-2-7-5-7-10V6l7-3z"></path></svg></span><span style="flex: 1; min-width: 0; text-align: left;"><strong style="display: block; color: var(--fgColor-default); font-size: 0.98rem;">Security Checklist</strong><span style="display: block; margin-top: 0.2rem; color: var(--fgColor-muted); font-size: 0.875rem; line-height: 1.35;">Check rollout hardening.</span></span></a>
  <a class="site-card" href="/operations/observability-and-troubleshooting" style="position: relative; display: flex; align-items: center; gap: 0.875rem; padding: 1rem 1.25rem; border: 1px solid var(--borderColor-default); background: transparent; color: inherit; text-decoration: none;"><span aria-hidden="true" style="display: inline-flex; height: 3rem; width: 3rem; align-items: center; justify-content: center; border-radius: 1rem; background: color-mix(in srgb, var(--fgColor-accent) 12%, transparent); color: var(--fgColor-accent);"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M5 18V9"></path><path d="M12 18V5"></path><path d="M19 18v-6"></path><path d="M4 18h16"></path></svg></span><span style="flex: 1; min-width: 0; text-align: left;"><strong style="display: block; color: var(--fgColor-default); font-size: 0.98rem;">Observability And Troubleshooting</strong><span style="display: block; margin-top: 0.2rem; color: var(--fgColor-muted); font-size: 0.875rem; line-height: 1.35;">Debug queue and worker issues.</span></span></a>
</div>

## Related Reference

- [Compute API](/reference/generated/api/endpoints/compute)
