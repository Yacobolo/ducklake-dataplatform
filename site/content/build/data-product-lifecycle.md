---
title: Data Product Lifecycle
description: Package trusted outputs into a discoverable data product with ownership, contracts, and versions.
---

# Data Product Lifecycle

Use this guide when you are turning technical outputs into a reusable, governed product for consumers.

## Inputs

- a domain and owner team
- one or more trusted outputs
- a contract and intended consumer audience
- an access request path

## Flow

1. define the owner team and business purpose
2. identify outputs and semantic entrypoints worth publishing
3. write the product contract and audience statement
4. create the product and attach outputs
5. create or update a version
6. publish the version and monitor status, subscriptions, and scorecards

## Visual Model

<figure class="site-mermaid">
  <img src="/_site/diagrams/data-product-anatomy.svg" alt="Diagram showing outputs and semantic entrypoints grouped into a versioned data product owned by a team within a domain." loading="lazy" decoding="async">
</figure>

## Verification

- consumers can discover the product by slug and owner
- outputs and semantic entrypoints are listed on the product
- version and publication state reflect reality
- the request path is clear for new consumers

## Related Reference

- [Products API](/reference/generated/api/endpoints/products)
