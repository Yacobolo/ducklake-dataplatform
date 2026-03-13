---
title: Data Products
description: Understand domains, teams, contracts, outputs, versions, status, and subscriptions.
---

# Data Products

If you only remember one thing, remember this: a data product in Duck packages technical outputs into something people can discover, trust, and request as a supported contract.

## Why It Matters

Without a product layer, consumers are forced to guess. They infer trust from table names, Slack threads, or whoever built the dashboard last year. Duck adds a packaging layer so ownership, intended audience, outputs, semantic entrypoints, and release state are explicit instead of tribal knowledge.

## What A Data Product Is

A data product is not the same thing as a table, dashboard, or semantic model.

- a table is one technical object
- a semantic model is one business-facing query interface
- a dashboard is one consumption surface
- a data product is the package around one or more outputs and the contract that explains what they are for

That package usually includes a domain, an owner team, a contract, outputs, optional semantic entrypoints, versioning, status, and a request path for consumers.

## What The Product Layer Connects

A data product connects:

- a business domain
- an owner team
- a contract and consumer audience
- one or more outputs
- optional semantic entrypoints
- release, publication, and status metadata

## Product Anatomy

<figure class="site-mermaid">
  <img src="/_site/diagrams/data-product-anatomy.svg" alt="Diagram showing a domain and team owning outputs, semantic entrypoints, versions, subscriptions, and a published data product." loading="lazy" decoding="async">
</figure>

Read the diagram from left to right. A domain and owner team define who stands behind the product. Trusted outputs and semantic entrypoints feed into the product because those are the things consumers actually use. Versioning and subscriptions sit alongside the product because products change over time and often have downstream consumers who need to know when something important changes.

## Why Versions, Outputs, And Subscriptions Matter

Outputs tell consumers what concrete things they can use. Semantic entrypoints tell them which business-facing interfaces are part of the contract. Versions tell them the product is managed over time rather than overwritten without ceremony. Subscriptions matter because once other teams depend on a product, changes need a communication path.

## Example In Duck

Imagine a “Daily Revenue” product owned by the Finance Analytics team in the `revenue` domain. The product might expose:

- a curated revenue asset
- a semantic model for finance reporting
- metrics such as gross revenue and net revenue
- a published request path for new consumers

The underlying tables and models still exist, but the product is what makes them legible as a supported analytical contract. A consumer should be able to ask, “who owns this, what does it include, what version is published, and how do I subscribe or request access?” without reading implementation details.

## How This Relates To Other Concepts

- [Asset Orchestration](/concepts/asset-orchestration) explains how outputs are produced and kept healthy
- [Semantic Layer](/concepts/semantic-layer) explains how business-facing entrypoints are defined
- this page explains how those outputs are packaged for consumption and ownership

## Common Misunderstandings

- A data product is not a synonym for any one table.
- A product can include multiple outputs and semantic entrypoints.
- Product metadata does not replace governance rules, but it makes governance easier to understand and request.

## Related Tasks

- [First Data Product](/start-here/first-data-product)
- [Data Product Lifecycle](/build/data-product-lifecycle)
- [Product Ownership](/govern/product-ownership)

## Related Reference

- [Products API](/reference/generated/api/endpoints/products)
- [Declarative DataProduct Kind](/reference/generated/declarative/kinds/data-product)
