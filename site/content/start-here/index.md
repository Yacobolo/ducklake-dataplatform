---
title: Getting Started
description: Pick the right path for querying data, building a product, or operating QuackStack.
---

# Getting Started

QuackStack is a governed data platform built around three outcomes:

- users can query trusted data through a secure control plane
- builders can package reusable data products instead of shipping scattered SQL
- operators can scale execution and governance without giving up policy control

## Choose Your Journey

- [First governed query](/start-here/quickstart) if you want to reach trusted data quickly.
- [First data product](/start-here/first-data-product) if you are building sources, transformations, assets, and semantic entrypoints.
- [First operator setup](/start-here/first-operator-setup) if you own auth, storage, compute, or platform posture.

## What QuackStack Covers

| Platform Area | What It Covers | Primary Audience |
| --- | --- | --- |
| Governed query execution | RBAC, row filters, and column masks on live queries | Data consumers and platform owners |
| Data products | Domains, teams, contracts, versions, and subscriptions | Builders and product owners |
| Asset orchestration | Checks, dependencies, freshness, and backfills | Builders and operators |
| Transformation workflows | Models, macros, notebooks, tests, and runs | Analytics engineers and data builders |
| Semantic modeling | Metrics, relationships, and pre-aggregations | Consumers and semantic model authors |
| Flexible compute | Local and remote execution with central policy enforcement | Operators |

## Recommended Reading Order

1. Start with the role-based quickstart that matches your job.
2. Read the [Concepts](/docs/concepts/) section to build a shared platform mental model.
3. Move into [Build](/docs/build/), [Govern](/docs/govern/), or [Operate](/operations/) depending on your responsibilities.
4. Use [Reference](/reference/) when you need exact payload, CLI, or schema detail.

## Recommended Next Steps

<div class="site-card-grid">
  <a class="site-card" href="/docs/concepts/" style="position: relative; display: flex; align-items: center; gap: 0.875rem; padding: 1rem 1.25rem; border: 1px solid var(--borderColor-default); background: transparent; color: inherit; text-decoration: none;">
    <span aria-hidden="true" style="display: inline-flex; height: 3rem; width: 3rem; align-items: center; justify-content: center; border-radius: 1rem; background: color-mix(in srgb, var(--fgColor-accent) 12%, transparent); color: var(--fgColor-accent);">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M4 7h16"></path><path d="M4 12h16"></path><path d="M4 17h10"></path></svg>
    </span>
    <span style="flex: 1; min-width: 0; text-align: left;">
      <strong style="display: block; color: var(--fgColor-default); font-size: 0.98rem;">Concepts</strong>
      <span style="display: block; margin-top: 0.2rem; color: var(--fgColor-muted); font-size: 0.875rem; line-height: 1.35;">Build the platform mental model.</span>
    </span>
  </a>
  <a class="site-card" href="/reference/feature-map" style="position: relative; display: flex; align-items: center; gap: 0.875rem; padding: 1rem 1.25rem; border: 1px solid var(--borderColor-default); background: transparent; color: inherit; text-decoration: none;">
    <span aria-hidden="true" style="display: inline-flex; height: 3rem; width: 3rem; align-items: center; justify-content: center; border-radius: 1rem; background: color-mix(in srgb, var(--fgColor-accent) 12%, transparent); color: var(--fgColor-accent);">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M5 18V9"></path><path d="M12 18V5"></path><path d="M19 18v-6"></path><path d="M4 18h16"></path></svg>
    </span>
    <span style="flex: 1; min-width: 0; text-align: left;">
      <strong style="display: block; color: var(--fgColor-default); font-size: 0.98rem;">Reference Feature Map</strong>
      <span style="display: block; margin-top: 0.2rem; color: var(--fgColor-muted); font-size: 0.875rem; line-height: 1.35;">Jump to specific capabilities.</span>
    </span>
  </a>
</div>

## Related Reference

- [Glossary](/reference/glossary)
- [API Entry Guide](/reference/api)
