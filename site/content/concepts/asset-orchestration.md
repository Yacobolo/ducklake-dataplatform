---
title: Asset Orchestration
description: Understand assets, dependency graphs, checks, partitions, materialization, backfills, and freshness.
---

# Asset Orchestration

Assets are the platform’s orchestration primitive for durable outputs and their operational guarantees.

## Why It Matters

Assets make execution intent explicit. Instead of asking only “what SQL produces this table,” you can also ask:

- what upstreams does it depend on
- what checks gate it
- how fresh should it be
- how should it be backfilled or rematerialized

## Mental Model

An asset definition captures:

- its key and type
- its upstream asset dependencies
- checks and policies
- materialization behavior
- freshness expectations
- partitions or backfill slices where relevant

## Asset Graph

<figure class="site-mermaid">
  <img src="/_site/diagrams/asset-dag-materialization.svg" alt="Diagram showing source assets flowing through checks and freshness policies into materialized downstream assets and data products." loading="lazy" decoding="async">
</figure>

## Key Objects

- asset definitions
- runs, materializations, and backfills
- checks and check results
- freshness status, blockers, and reconcile actions
- partitions and dependency graphs

## Related Tasks

- [Asset Orchestration Guide](/build/asset-orchestration)
- [Lineage And Freshness](/concepts/lineage-and-freshness)
- [Policy Verification](/govern/policy-verification)

## Related Reference

- [Assets API](/reference/generated/api/endpoints/assets)
- [Declarative Asset Kind](/reference/generated/declarative/kinds/asset)
