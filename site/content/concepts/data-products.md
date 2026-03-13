---
title: Data Products
description: Understand domains, teams, contracts, outputs, versions, status, and subscriptions.
---

# Data Products

Data products are the platform’s packaging layer. They turn technical outputs into governed, owned, discoverable analytical contracts.

## Why It Matters

Without a product layer, consumers have to discover trust and ownership from table names, tribal knowledge, or dashboards. Duck makes those concerns explicit.

## Mental Model

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

## Key Objects

- domains and teams
- product contract and SLO
- outputs and semantic entrypoints
- versions, publication intent, and release state
- dependencies, subscriptions, and scorecards

## Related Tasks

- [First Data Product](/start-here/first-data-product)
- [Data Product Lifecycle](/build/data-product-lifecycle)
- [Product Ownership](/govern/product-ownership)

## Related Reference

- [Products API](/reference/generated/api/endpoints/products)
- [Declarative DataProduct Kind](/reference/generated/declarative/kinds/data-product)
