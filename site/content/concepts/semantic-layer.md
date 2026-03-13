---
title: Semantic Layer
description: Understand semantic models, metrics, relationships, pre-aggregations, and metric query flow.
---

# Semantic Layer

The semantic layer turns technical transformations into reusable business-facing query interfaces.

## Why It Matters

Consumers should not have to know every table join or aggregation rule. Semantic models let builders publish consistent metrics and reusable relationships.

## Mental Model

A semantic model usually sits on top of a trusted base model and defines:

- dimensions and time grain
- metrics and metric types
- relationships to other semantic models
- pre-aggregations for performance

## Query Planning Flow

<figure class="site-mermaid">
  <img src="/_site/diagrams/semantic-query-flow.svg" alt="Diagram showing semantic models and metrics resolving through the semantic planner into governed query execution." loading="lazy" decoding="async">
</figure>

## Key Objects

- semantic models
- semantic metrics
- semantic relationships
- pre-aggregations
- metric query explain and run endpoints

## Related Tasks

- [Semantic Models And Metrics](/build/semantic-models-and-metrics)
- [Data Products](/concepts/data-products)
- [Lineage And Freshness](/concepts/lineage-and-freshness)

## Related Reference

- [Semantic Layer API](/reference/generated/api/endpoints/semantic-layer)
- [Declarative SemanticModel Kind](/reference/generated/declarative/kinds/semantic-model)
