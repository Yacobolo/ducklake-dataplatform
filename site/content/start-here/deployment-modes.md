---
title: Ways to Access Duck
description: Choose the right interaction mode for users, builders, and operators.
---

# Ways to Access Duck

Duck supports multiple interaction modes because the same governed platform serves end users, builders, and operators.

## Access Modes

| Access Mode | Best For | Typical User | When To Avoid |
| --- | --- | --- | --- |
| Query surfaces | Guided product experiences, dashboards, and discovery | Business users and analysts | When you need low-level API or CLI control |
| SQL, BI, and programmatic access | Familiar query tooling and automation | Analysts, BI tools, and services | When the problem is really about product UX or platform config |
| Builder workflows | Models, assets, notebooks, metrics, and products | Data builders and analytics engineers | When you only need to query trusted outputs |
| Operator and runtime access | Auth, storage, networking, and compute posture | Platform operators and admins | When you are not responsible for deployment or runtime safety |
| Execution topology | Worker isolation and remote routing strategy | Operators planning scale or isolation | When local execution already matches the workload |

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
  <a class="site-card" href="/start-here/quickstart" style="position: relative; display: flex; align-items: center; gap: 0.875rem; padding: 1rem 1.25rem; border: 1px solid var(--borderColor-default); background: transparent; color: inherit; text-decoration: none;">
    <span aria-hidden="true" style="display: inline-flex; height: 3rem; width: 3rem; align-items: center; justify-content: center; border-radius: 1rem; background: color-mix(in srgb, var(--fgColor-accent) 12%, transparent); color: var(--fgColor-accent);"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M4 7h16"></path><path d="M7 12h10"></path><path d="M9 17h6"></path></svg></span>
    <span style="flex: 1; min-width: 0; text-align: left;"><strong style="display: block; color: var(--fgColor-default); font-size: 0.98rem;">Quickstart</strong><span style="display: block; margin-top: 0.2rem; color: var(--fgColor-muted); font-size: 0.875rem; line-height: 1.35;">Run your first governed query.</span></span>
  </a>
  <a class="site-card" href="/start-here/first-data-product" style="position: relative; display: flex; align-items: center; gap: 0.875rem; padding: 1rem 1.25rem; border: 1px solid var(--borderColor-default); background: transparent; color: inherit; text-decoration: none;">
    <span aria-hidden="true" style="display: inline-flex; height: 3rem; width: 3rem; align-items: center; justify-content: center; border-radius: 1rem; background: color-mix(in srgb, var(--fgColor-accent) 12%, transparent); color: var(--fgColor-accent);"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M6 5h12v14H6z"></path><path d="M9 9h6"></path><path d="M9 13h6"></path></svg></span>
    <span style="flex: 1; min-width: 0; text-align: left;"><strong style="display: block; color: var(--fgColor-default); font-size: 0.98rem;">First Data Product</strong><span style="display: block; margin-top: 0.2rem; color: var(--fgColor-muted); font-size: 0.875rem; line-height: 1.35;">Go from source to product.</span></span>
  </a>
  <a class="site-card" href="/start-here/first-operator-setup" style="position: relative; display: flex; align-items: center; gap: 0.875rem; padding: 1rem 1.25rem; border: 1px solid var(--borderColor-default); background: transparent; color: inherit; text-decoration: none;">
    <span aria-hidden="true" style="display: inline-flex; height: 3rem; width: 3rem; align-items: center; justify-content: center; border-radius: 1rem; background: color-mix(in srgb, var(--fgColor-accent) 12%, transparent); color: var(--fgColor-accent);"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M12 5v14"></path><path d="M5 12h14"></path></svg></span>
    <span style="flex: 1; min-width: 0; text-align: left;"><strong style="display: block; color: var(--fgColor-default); font-size: 0.98rem;">First Operator Setup</strong><span style="display: block; margin-top: 0.2rem; color: var(--fgColor-muted); font-size: 0.875rem; line-height: 1.35;">Stand up the runtime baseline.</span></span>
  </a>
</div>
