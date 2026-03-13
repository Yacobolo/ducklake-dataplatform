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

<style>
  .product-next-steps {
    display: grid;
    gap: 0.75rem;
  }

  .product-next-step-card {
    position: relative;
    display: flex;
    align-items: center;
    gap: 0.875rem;
    border: 1px solid var(--site-border);
    background: transparent;
    color: inherit;
    padding: 1rem 1.25rem !important;
    text-decoration: none;
    transition:
      transform 180ms ease,
      border-color 180ms ease,
      box-shadow 180ms ease,
      background-color 180ms ease;
  }

  .product-next-step-card,
  .product-next-step-card:hover,
  .product-next-step-card:focus,
  .product-next-step-card:visited {
    text-decoration: none !important;
  }

  .product-next-step-card:hover {
    transform: translateY(-2px);
    border-color: var(--site-accent);
    background: color-mix(in srgb, var(--site-accent) 3%, transparent);
    box-shadow: 0 18px 40px rgba(15, 23, 42, 0.08);
  }

  .product-next-step-card:hover .product-next-step-arrow {
    color: var(--site-accent-strong);
    transform: translate(2px, -2px);
  }

  .product-next-step-icon {
    display: inline-flex;
    height: 3rem;
    width: 3rem;
    flex-shrink: 0;
    align-items: center;
    justify-content: center;
    align-self: center;
    border-radius: 1.25rem;
    background: color-mix(in srgb, var(--site-accent) 12%, transparent);
    color: var(--site-accent-strong);
  }

  .product-next-step-copy {
    min-width: 0;
    flex: 1;
    text-align: left;
  }

  .product-next-step-title {
    margin: 0;
    color: var(--site-ink);
    font-size: 0.98rem;
    font-weight: 600;
  }

  .product-next-step-subtitle {
    margin: 0.2rem 0 0;
    color: var(--site-muted);
    font-size: 0.875rem;
    line-height: 1.35;
    white-space: nowrap;
  }

  .product-next-step-arrow {
    position: absolute;
    right: 1.25rem;
    top: 1.25rem;
    height: 1rem;
    width: 1rem;
    color: var(--site-muted-soft);
    transition:
      color 180ms ease,
      transform 180ms ease;
  }

  @media (max-width: 767px) {
    .product-next-step-card {
      align-items: flex-start;
      padding-right: 3.5rem;
    }

    .product-next-step-subtitle {
      white-space: normal;
    }
  }
</style>

<div class="product-next-steps">
  <a class="site-card product-next-step-card" href="/concepts/data-products">
    <span class="product-next-step-icon" aria-hidden="true">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
        <path d="M4 7h16"></path>
        <path d="M7 4h10v16H7z"></path>
        <path d="M10 11h4"></path>
        <path d="M10 15h4"></path>
      </svg>
    </span>
    <span class="product-next-step-copy">
      <h3 class="product-next-step-title">Data Products</h3>
      <p class="product-next-step-subtitle">Contracts, versions, and ownership.</p>
    </span>
    <svg class="product-next-step-arrow" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
      <path d="M7 17 17 7"></path>
      <path d="M7 7h10v10"></path>
    </svg>
  </a>
  <a class="site-card product-next-step-card" href="/concepts/transformation-framework">
    <span class="product-next-step-icon" aria-hidden="true">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
        <path d="M8 7h8"></path>
        <path d="M8 12h8"></path>
        <path d="M8 17h5"></path>
        <path d="M6 4h12v16H6z"></path>
      </svg>
    </span>
    <span class="product-next-step-copy">
      <h3 class="product-next-step-title">Transformation Framework</h3>
      <p class="product-next-step-subtitle">Models, notebooks, tests, and runs.</p>
    </span>
    <svg class="product-next-step-arrow" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
      <path d="M7 17 17 7"></path>
      <path d="M7 7h10v10"></path>
    </svg>
  </a>
  <a class="site-card product-next-step-card" href="/build/data-product-lifecycle">
    <span class="product-next-step-icon" aria-hidden="true">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
        <path d="M7 4v4"></path>
        <path d="M17 4v4"></path>
        <path d="M5 8h14"></path>
        <rect x="4" y="6" width="16" height="14" rx="2"></rect>
        <path d="M8 12h3"></path>
        <path d="M13 12h3"></path>
      </svg>
    </span>
    <span class="product-next-step-copy">
      <h3 class="product-next-step-title">Data Product Lifecycle</h3>
      <p class="product-next-step-subtitle">From draft to release.</p>
    </span>
    <svg class="product-next-step-arrow" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
      <path d="M7 17 17 7"></path>
      <path d="M7 7h10v10"></path>
    </svg>
  </a>
  <a class="site-card product-next-step-card" href="/build/semantic-models-and-metrics">
    <span class="product-next-step-icon" aria-hidden="true">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
        <path d="M5 18V9"></path>
        <path d="M12 18V5"></path>
        <path d="M19 18v-6"></path>
        <path d="M4 18h16"></path>
      </svg>
    </span>
    <span class="product-next-step-copy">
      <h3 class="product-next-step-title">Semantic Models And Metrics</h3>
      <p class="product-next-step-subtitle">Business-ready metrics and entrypoints.</p>
    </span>
    <svg class="product-next-step-arrow" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
      <path d="M7 17 17 7"></path>
      <path d="M7 7h10v10"></path>
    </svg>
  </a>
</div>

## Related Reference

- [Feature Map](/reference/feature-map)
- [Declarative Entry Guide](/reference/declarative)
