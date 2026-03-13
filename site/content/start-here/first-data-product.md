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

- [Data Products](/concepts/data-products)
- [Transformation Framework](/concepts/transformation-framework)
- [Data Product Lifecycle](/build/data-product-lifecycle)
- [Semantic Models And Metrics](/build/semantic-models-and-metrics)

## Related Reference

- [Feature Map](/reference/feature-map)
- [Declarative Entry Guide](/reference/declarative)
