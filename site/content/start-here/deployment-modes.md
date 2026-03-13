---
title: Ways to Access Duck
description: Choose the right interaction mode for users, builders, and operators.
---

# Ways to Access Duck

Duck supports multiple interaction modes because the same governed platform serves end users, builders, and operators.

## Query Surfaces

Use this when:

- you want guided product surfaces
- your team exposes browser-based discovery or dashboards
- you are troubleshooting user experience rather than API behavior

## SQL, BI, And Programmatic Access

Use this when:

- you use SQL clients, BI tools, or service-to-service calls
- you want a familiar query workflow
- you need repeatable automation or scripting

## Builder Workflows

Use this when:

- you are creating models, assets, notebooks, metrics, and products
- you need declarative change management
- you want exact API and CLI control over platform state

## Operator And Runtime Access

Use this when:

- you are configuring auth, storage, networking, or compute routing
- you are rolling out distributed compute
- you are debugging health, throughput, or fallback behavior

## Execution Topology

Use this when:

- your admins have enabled remote execution
- you need worker isolation or a separate compute fleet
- execution topology matters for scale or control

Read [Distributed Compute](/operations/distributed-compute) before rollout.

## Decision Guide

| Need | Recommended path |
| --- | --- |
| Reach trusted data quickly | Query surfaces |
| Build reusable products | Builder workflows |
| Manage policy and runtime posture | Operator and runtime access |
| Isolate or scale execution | Execution topology and remote compute |

## Next Steps

<div class="site-card-grid">
  <a class="site-card" href="/start-here/quickstart" style="position: relative; display: flex; align-items: center; gap: 0.875rem; padding: 1rem 1.25rem; border: 1px solid var(--site-border); background: transparent; color: inherit; text-decoration: none;">
    <span aria-hidden="true" style="display: inline-flex; height: 3rem; width: 3rem; align-items: center; justify-content: center; border-radius: 1rem; background: color-mix(in srgb, var(--site-accent) 12%, transparent); color: var(--site-accent-strong);"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M4 7h16"></path><path d="M7 12h10"></path><path d="M9 17h6"></path></svg></span>
    <span style="flex: 1; min-width: 0; text-align: left;"><strong style="display: block; color: var(--site-ink); font-size: 0.98rem;">Quickstart</strong><span style="display: block; margin-top: 0.2rem; color: var(--site-muted); font-size: 0.875rem; line-height: 1.35;">Run your first governed query.</span></span>
  </a>
  <a class="site-card" href="/start-here/first-data-product" style="position: relative; display: flex; align-items: center; gap: 0.875rem; padding: 1rem 1.25rem; border: 1px solid var(--site-border); background: transparent; color: inherit; text-decoration: none;">
    <span aria-hidden="true" style="display: inline-flex; height: 3rem; width: 3rem; align-items: center; justify-content: center; border-radius: 1rem; background: color-mix(in srgb, var(--site-accent) 12%, transparent); color: var(--site-accent-strong);"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M6 5h12v14H6z"></path><path d="M9 9h6"></path><path d="M9 13h6"></path></svg></span>
    <span style="flex: 1; min-width: 0; text-align: left;"><strong style="display: block; color: var(--site-ink); font-size: 0.98rem;">First Data Product</strong><span style="display: block; margin-top: 0.2rem; color: var(--site-muted); font-size: 0.875rem; line-height: 1.35;">Go from source to product.</span></span>
  </a>
  <a class="site-card" href="/start-here/first-operator-setup" style="position: relative; display: flex; align-items: center; gap: 0.875rem; padding: 1rem 1.25rem; border: 1px solid var(--site-border); background: transparent; color: inherit; text-decoration: none;">
    <span aria-hidden="true" style="display: inline-flex; height: 3rem; width: 3rem; align-items: center; justify-content: center; border-radius: 1rem; background: color-mix(in srgb, var(--site-accent) 12%, transparent); color: var(--site-accent-strong);"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M12 5v14"></path><path d="M5 12h14"></path></svg></span>
    <span style="flex: 1; min-width: 0; text-align: left;"><strong style="display: block; color: var(--site-ink); font-size: 0.98rem;">First Operator Setup</strong><span style="display: block; margin-top: 0.2rem; color: var(--site-muted); font-size: 0.875rem; line-height: 1.35;">Stand up the runtime baseline.</span></span>
  </a>
</div>
