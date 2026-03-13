---
title: Platform Architecture
description: Understand the secure control-plane path from principal to governed result and product metadata.
---

# Platform Architecture

Duck is organized around a simple rule: the control plane owns identity, policy, and product metadata; the execution layer runs queries under that policy context.

## Why It Matters

This separation lets you scale or relocate compute without changing who can see what.

## Mental Model

At a high level:

1. a principal reaches Duck through browser, SQL, API, or CLI
2. identity is resolved
3. the control plane checks grants, row filters, column masks, and product metadata
4. the rewritten or authorized plan executes
5. the result comes back as a governed response

## Architecture Diagram

<figure class="site-mermaid">
  <img src="/_site/diagrams/secure-query-path.svg" alt="Diagram showing a principal flowing through identity, Duck API, policy enforcement, execution, and a governed result." loading="lazy" decoding="async">
</figure>

## Key Objects

- principals, groups, grants, row filters, and column masks
- catalogs, schemas, tables, views, and volumes
- models, notebooks, assets, pipelines, and semantic models
- data products, versions, outputs, and subscriptions

## Related Tasks

- [Quickstart](/start-here/quickstart)
- [Access Design](/govern/access-design)
- [Platform Settings](/operations/configuration)

## Related Reference

- [API Entry Guide](/reference/api)
