---
title: First Data Product
description: Build your first reusable product from source data to semantic entrypoint.
---

# First Data Product

Use this quickstart when you want to understand how Duck’s builder workflow fits together from source data to a published, discoverable product.

## What You Will Create

In this walkthrough, the end state is:

- a governed source table or view
- one or more transformation models
- an orchestration asset graph with checks and freshness
- a semantic entrypoint for consumers
- a data product with ownership, contract, outputs, and versioning

## 1. Start from a real source

Choose a source that already exists or register one in the right catalog and schema. The source should have:

- a clear owner
- an intended consumer audience
- a sensible query grain
- a place in your governance model

## 2. Turn raw data into reusable transformations

Use models, macros, tests, and notebooks to shape the source into stable outputs.

Common early outputs are:

- staging models that normalize source shape
- marts that encode trusted business logic
- notebook outputs promoted into durable models

## 3. Define how outputs are produced

Create assets and dependencies so the platform knows:

- what depends on what
- which checks gate materialization
- what freshness each output is expected to meet
- how backfills and remediation should work

## 4. Add semantics for consumption

Create semantic models, metrics, and relationships so consumers can query business concepts instead of internal transformation details.

## 5. Package the result as a product

A strong data product in Duck includes:

- domain and owner team
- contract and intended audience
- outputs and semantic entrypoints
- publication intent and release/version state
- a request path for consumers who need access

## Lifecycle at a glance

<figure class="site-mermaid">
  <img src="/_site/diagrams/data-product-anatomy.svg" alt="Flow diagram showing source data becoming transformations, assets, semantic entrypoints, and a packaged data product with ownership and subscriptions." loading="lazy" decoding="async">
</figure>

## How To Know It Worked

You are in good shape when:

- consumers can discover the product by contract and output, not just by table name
- builders can explain dependencies, freshness, and run behavior
- operators can identify owner, request path, and runtime posture without reverse engineering implementation details

## Next Steps

<div class="site-card-grid">
  <a class="site-card" href="/concepts/data-products" style="position: relative; display: flex; height: 100%; align-items: flex-start; gap: 1rem; color: inherit; text-decoration: none;">
    <span aria-hidden="true" style="margin-top: 0.25rem; display: inline-flex; height: 2.75rem; width: 2.75rem; flex-shrink: 0; align-items: center; justify-content: center; border-radius: 1rem; background: color-mix(in srgb, var(--site-accent) 14%, var(--site-surface-strong)); color: var(--site-accent-strong);">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
        <path d="M4 7h16"></path>
        <path d="M7 4h10v16H7z"></path>
        <path d="M10 11h4"></path>
        <path d="M10 15h4"></path>
      </svg>
    </span>
    <span style="min-width: 0; flex: 1;">
      <h3 style="margin: 0; color: var(--site-ink); font-size: 1rem; font-weight: 600;">Data Products</h3>
      <p style="margin: 0.5rem 0 0; color: var(--site-muted); font-size: 0.95rem; line-height: 1.7;">Learn how Duck packages contracts, outputs, ownership, versions, and subscriptions into a discoverable product surface.</p>
    </span>
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true" style="position: absolute; right: 1.5rem; top: 1.5rem; height: 1rem; width: 1rem; color: var(--site-muted-soft);">
      <path d="M7 17 17 7"></path>
      <path d="M7 7h10v10"></path>
    </svg>
  </a>
  <a class="site-card" href="/concepts/transformation-framework" style="position: relative; display: flex; height: 100%; align-items: flex-start; gap: 1rem; color: inherit; text-decoration: none;">
    <span aria-hidden="true" style="margin-top: 0.25rem; display: inline-flex; height: 2.75rem; width: 2.75rem; flex-shrink: 0; align-items: center; justify-content: center; border-radius: 1rem; background: color-mix(in srgb, var(--site-accent) 14%, var(--site-surface-strong)); color: var(--site-accent-strong);">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
        <path d="M8 7h8"></path>
        <path d="M8 12h8"></path>
        <path d="M8 17h5"></path>
        <path d="M6 4h12v16H6z"></path>
      </svg>
    </span>
    <span style="min-width: 0; flex: 1;">
      <h3 style="margin: 0; color: var(--site-ink); font-size: 1rem; font-weight: 600;">Transformation Framework</h3>
      <p style="margin: 0.5rem 0 0; color: var(--site-muted); font-size: 0.95rem; line-height: 1.7;">See how models, macros, notebooks, tests, and runs turn raw sources into stable analytical outputs.</p>
    </span>
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true" style="position: absolute; right: 1.5rem; top: 1.5rem; height: 1rem; width: 1rem; color: var(--site-muted-soft);">
      <path d="M7 17 17 7"></path>
      <path d="M7 7h10v10"></path>
    </svg>
  </a>
  <a class="site-card" href="/build/data-product-lifecycle" style="position: relative; display: flex; height: 100%; align-items: flex-start; gap: 1rem; color: inherit; text-decoration: none;">
    <span aria-hidden="true" style="margin-top: 0.25rem; display: inline-flex; height: 2.75rem; width: 2.75rem; flex-shrink: 0; align-items: center; justify-content: center; border-radius: 1rem; background: color-mix(in srgb, var(--site-accent) 14%, var(--site-surface-strong)); color: var(--site-accent-strong);">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
        <path d="M7 4v4"></path>
        <path d="M17 4v4"></path>
        <path d="M5 8h14"></path>
        <rect x="4" y="6" width="16" height="14" rx="2"></rect>
        <path d="M8 12h3"></path>
        <path d="M13 12h3"></path>
      </svg>
    </span>
    <span style="min-width: 0; flex: 1;">
      <h3 style="margin: 0; color: var(--site-ink); font-size: 1rem; font-weight: 600;">Data Product Lifecycle</h3>
      <p style="margin: 0.5rem 0 0; color: var(--site-muted); font-size: 0.95rem; line-height: 1.7;">Follow the builder path from draft outputs to governed release, versioning, and ongoing operational ownership.</p>
    </span>
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true" style="position: absolute; right: 1.5rem; top: 1.5rem; height: 1rem; width: 1rem; color: var(--site-muted-soft);">
      <path d="M7 17 17 7"></path>
      <path d="M7 7h10v10"></path>
    </svg>
  </a>
  <a class="site-card" href="/build/semantic-models-and-metrics" style="position: relative; display: flex; height: 100%; align-items: flex-start; gap: 1rem; color: inherit; text-decoration: none;">
    <span aria-hidden="true" style="margin-top: 0.25rem; display: inline-flex; height: 2.75rem; width: 2.75rem; flex-shrink: 0; align-items: center; justify-content: center; border-radius: 1rem; background: color-mix(in srgb, var(--site-accent) 14%, var(--site-surface-strong)); color: var(--site-accent-strong);">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
        <path d="M5 18V9"></path>
        <path d="M12 18V5"></path>
        <path d="M19 18v-6"></path>
        <path d="M4 18h16"></path>
      </svg>
    </span>
    <span style="min-width: 0; flex: 1;">
      <h3 style="margin: 0; color: var(--site-ink); font-size: 1rem; font-weight: 600;">Semantic Models And Metrics</h3>
      <p style="margin: 0.5rem 0 0; color: var(--site-muted); font-size: 0.95rem; line-height: 1.7;">Move from technical outputs to business-facing metrics, relationships, and query entrypoints that consumers can trust.</p>
    </span>
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true" style="position: absolute; right: 1.5rem; top: 1.5rem; height: 1rem; width: 1rem; color: var(--site-muted-soft);">
      <path d="M7 17 17 7"></path>
      <path d="M7 7h10v10"></path>
    </svg>
  </a>
</div>

## Related Reference

- [Feature Map](/reference/feature-map)
- [Declarative Entry Guide](/reference/declarative)
