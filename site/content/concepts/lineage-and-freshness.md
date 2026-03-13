---
title: Lineage And Freshness
description: Understand how Duck explains provenance, impact, blockers, and data staleness.
---

# Lineage And Freshness

Lineage explains where data came from. Freshness explains whether it is current enough to trust.

## Why It Matters

Product teams need both answers before they can confidently publish or troubleshoot an output.

## Mental Model

- table lineage shows upstream and downstream object relationships
- column lineage shows how specific fields were derived
- freshness status shows whether the asset or metric meets its expected lag target
- blocker and requirement views explain what upstream state is preventing recovery

## Freshness And Impact View

<figure class="site-mermaid">
  <img src="/_site/diagrams/lineage-flow.svg" alt="Diagram showing lineage from source tables through models and assets into metrics, with freshness and impact indicators." loading="lazy" decoding="async">
</figure>

## Blocker Tree

<figure class="site-mermaid">
  <img src="/_site/diagrams/freshness-blocker-tree.svg" alt="Diagram showing an unhealthy downstream asset tracing freshness blockers back through upstream assets and checks." loading="lazy" decoding="async">
</figure>

## Key Objects

- table lineage edges and nodes
- column lineage edges
- freshness status and explanations
- freshness requirements and blockers
- reconcile actions and impact exploration

## Related Tasks

- [Asset Orchestration Guide](/build/asset-orchestration)
- [Policy Verification](/govern/policy-verification)
- [Observability And Troubleshooting](/operations/observability-and-troubleshooting)

## Related Reference

- [Lineage API](/reference/generated/api/endpoints/lineage)
- [Assets API](/reference/generated/api/endpoints/assets)
