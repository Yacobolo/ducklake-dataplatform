---
title: Concepts
description: Build the mental model for how Duck’s control plane, products, orchestration, semantics, and compute fit together.
---

# Concepts

The concepts section should teach the system, not just label its parts. If you only remember one thing, remember this: Duck becomes much easier to reason about once you separate governance, products, orchestration, transformation, semantics, and compute.

When teams first encounter Duck, many ideas can sound similar because they all touch the same outputs. A model, asset, semantic model, and data product may all point at the same business domain, but they answer different questions:

- governance asks who is allowed to see it
- products ask how it is packaged and owned
- orchestration asks how it is kept healthy and current
- transformation asks how it is built
- semantics ask how consumers should query it
- compute asks where the work runs

## Read These First

- [Platform Architecture](/docs/concepts/platform-architecture)
- [Governance Model](/docs/concepts/governance-model)
- [Data Products](/docs/concepts/data-products)
- [Asset Orchestration](/docs/concepts/asset-orchestration)
- [Transformation Framework](/docs/concepts/transformation-framework)
- [Semantic Layer](/docs/concepts/semantic-layer)
- [Lineage And Freshness](/docs/concepts/lineage-and-freshness)
- [Compute Topology](/docs/concepts/compute-topology)

## How To Use This Section

Read these pages when you want a mental model before implementation details. Each page should answer three questions:

1. what is this concept in plain English
2. why does Duck have it as a separate concept
3. how is it different from the nearby concepts people often confuse it with

## Example In Duck

Imagine one business question: "is daily revenue healthy across pickup zones?"

Duck answers that one question through several separate concepts that are easy to blur together if the docs do not name them clearly:

- [Governance Model](/docs/concepts/governance-model) explains which principals are allowed to see the answer and whether rows or columns should be filtered or masked
- [Transformation Framework](/docs/concepts/transformation-framework) explains how builders turn raw trip data into curated revenue logic
- [Asset Orchestration](/docs/concepts/asset-orchestration) explains how the curated output is refreshed, checked, backfilled, and monitored for freshness
- [Semantic Layer](/docs/concepts/semantic-layer) explains how the business-facing metric is defined so consumers do not have to rebuild it by hand
- [Data Products](/docs/concepts/data-products) explains how the final output is packaged, owned, versioned, and published for discovery
- [Compute Topology](/docs/concepts/compute-topology) explains where the work runs without changing governance outcomes

That is why the concepts section exists. Duck is one platform, but the platform solves several different problems at once. The articles in this section separate those concerns so the rest of the docs can stay practical instead of constantly redefining the same terms.

## Common Misunderstandings

- A concept page is not the same thing as a task guide. Concepts explain what something is and why it exists; task guides explain how to use it.
- Many Duck objects touch the same output, but that does not make them interchangeable. A model, asset, semantic model, and data product are related, not synonymous.

## Recommended Next Steps

<div class="site-card-grid">
  <a class="site-card" href="/docs/build/" style="position: relative; display: flex; align-items: center; gap: 0.875rem; padding: 1rem 1.25rem; border: 1px solid var(--borderColor-default); background: transparent; color: inherit; text-decoration: none;">
    <span aria-hidden="true" style="display: inline-flex; height: 3rem; width: 3rem; align-items: center; justify-content: center; border-radius: 1rem; background: color-mix(in srgb, var(--fgColor-accent) 12%, transparent); color: var(--fgColor-accent);">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M6 5h12v14H6z"></path><path d="M9 9h6"></path><path d="M9 13h6"></path></svg>
    </span>
    <span style="flex: 1; min-width: 0; text-align: left;"><strong style="display: block; color: var(--fgColor-default); font-size: 0.98rem;">Build</strong><span style="display: block; margin-top: 0.2rem; color: var(--fgColor-muted); font-size: 0.875rem; line-height: 1.35;">Turn concepts into workflows.</span></span>
  </a>
  <a class="site-card" href="/docs/govern/" style="position: relative; display: flex; align-items: center; gap: 0.875rem; padding: 1rem 1.25rem; border: 1px solid var(--borderColor-default); background: transparent; color: inherit; text-decoration: none;">
    <span aria-hidden="true" style="display: inline-flex; height: 3rem; width: 3rem; align-items: center; justify-content: center; border-radius: 1rem; background: color-mix(in srgb, var(--fgColor-accent) 12%, transparent); color: var(--fgColor-accent);">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M12 3l7 3v5c0 5-3 8-7 10-4-2-7-5-7-10V6l7-3z"></path></svg>
    </span>
    <span style="flex: 1; min-width: 0; text-align: left;"><strong style="display: block; color: var(--fgColor-default); font-size: 0.98rem;">Govern</strong><span style="display: block; margin-top: 0.2rem; color: var(--fgColor-muted); font-size: 0.875rem; line-height: 1.35;">Design safe access patterns.</span></span>
  </a>
  <a class="site-card" href="/operations/" style="position: relative; display: flex; align-items: center; gap: 0.875rem; padding: 1rem 1.25rem; border: 1px solid var(--borderColor-default); background: transparent; color: inherit; text-decoration: none;">
    <span aria-hidden="true" style="display: inline-flex; height: 3rem; width: 3rem; align-items: center; justify-content: center; border-radius: 1rem; background: color-mix(in srgb, var(--fgColor-accent) 12%, transparent); color: var(--fgColor-accent);">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M12 5v14"></path><path d="M5 12h14"></path></svg>
    </span>
    <span style="flex: 1; min-width: 0; text-align: left;"><strong style="display: block; color: var(--fgColor-default); font-size: 0.98rem;">Operate</strong><span style="display: block; margin-top: 0.2rem; color: var(--fgColor-muted); font-size: 0.875rem; line-height: 1.35;">Run the platform safely.</span></span>
  </a>
</div>

## Related Reference

- [Feature Map](/reference/feature-map)
- [Glossary](/reference/glossary)
