---
title: Semantic Models And Metrics
description: Publish trusted business-facing entrypoints on top of technical transformations.
---

# Semantic Models And Metrics

Use this guide when downstream users need reusable business definitions rather than direct table knowledge.

## Inputs

- a trusted base model
- key dimensions and time grain
- metric definitions
- relationships to other semantic models

## Flow

1. choose the stable base model
2. create the semantic model and default time dimension
3. define metrics and relationships
4. add pre-aggregations when performance matters
5. test metric queries and attach the entrypoint to a data product

## Query Flow

<figure class="site-mermaid">
  <img src="/_site/diagrams/semantic-query-flow.svg" alt="Diagram showing semantic models and metrics being planned into a governed SQL query." loading="lazy" decoding="async">
</figure>

## Verification

- consumers can use the metric interface without reverse engineering joins
- metric freshness is visible
- the semantic entrypoint is attached to the right product contract

## Related Reference

- [Semantic Layer API](/reference/generated/api/endpoints/semantic-layer)
