---
title: Define Semantic Metrics
description: Build semantic models, metrics, and relationships that agents and users can query consistently.
doc_kind: task
audiences: [ai-agents, builders, end-users, admins]
product_areas: [semantic-layer, models, governance]
surfaces: [api, declarative, docs]
tasks: [create semantic model, define metric, manage relationships, run metric query]
prerequisites: [modeled source data, business definitions, metric consumers]
permissions: [semantic model administration, metric query access]
cli_commands: [docs search, api search semantic]
command_groups: [docs, api]
operation_ids: [createSemanticModel, listSemanticMetrics, createSemanticMetric, runMetricQuery]
api_tags: [Semantic Layer]
declarative_kinds: [semantic-model]
related_docs: [reference/feature-semantic-layer, how-to/build-and-run-model-pipelines, how-to/use-the-cli]
keywords: [semantic metrics, business metrics, metric query, semantic model]
last_verified: 2026-03-12
source_of_truth: [docs, api/gen/openapi.yaml, schemas/declarative/v1/index.json]
---

# Define Semantic Metrics

## Objective

Create reusable business metrics that stay consistent across notebooks, dashboards, and agent-driven query workflows.

## When to use

Use this workflow when teams need named metrics, governed dimensions, and reusable relationship logic instead of raw SQL definitions repeated in many places.

## Prerequisites

- stable modeled tables or views to serve as the semantic source
- clear business definitions for each metric
- agreement on grain, dimensions, and ownership

## Required permissions

- semantic model and metric administration access
- read access to all underlying modeled data
- metric query access for validation

## Exact steps

### 1. Pick the source model and business grain

- Start from modeled outputs that are already governed and stable.
- Define the entity grain before introducing metrics.

Expected result: the semantic model has a clear base object and row-level meaning.

### 2. Create the semantic model

- Register dimensions, measures, and any pre-aggregation strategy needed for repeated use.
- Keep naming explicit enough for both humans and agents to discover the right object.

```bash
duck docs search "create semantic model"
duck api search semantic
```

Expected result: the semantic model exists with the intended dimensions and metric-ready structure.

### 3. Add metrics and relationships

- Define each metric in business terms, not only technical expressions.
- Add relationships only where cross-model navigation is genuinely useful.

Expected result: consumers can ask for metrics without rebuilding joins or formulas manually.

### 4. Run metric queries

- Validate the metric against known expected values.
- Compare semantic results to trusted SQL or notebook checks before broad rollout.

Expected result: metric queries return values that match the approved business definition.

### 5. Expose metrics to notebooks, dashboards, and agents

- Route downstream consumers to semantic queries where consistency matters more than flexibility.
- Reserve raw-table access for exploratory or engineering-only workflows.

Expected result: dashboards and agents can rely on stable metric names and governed dimensions.

## Verified examples

- Semantic model flow: create semantic model, list semantic metrics, create semantic metric.
- Query flow: run metric query and compare output to trusted validation checks.

## Expected result

You end with a semantic layer that reduces duplicated metric logic and gives agents a safer retrieval target than raw tables alone.

## Failure modes

- metrics disagree with trusted SQL: verify grain, filter defaults, and relationship joins before changing formulas
- agents select the wrong metric: improve metric naming, keywords, and relationship descriptions
- metric queries are slow: revisit pre-aggregations and semantic scope instead of broadening raw access
- semantic model becomes stale after upstream changes: revalidate dimensions and metric definitions whenever source models change

## Related CLI commands

- `duck docs search "create semantic model"`
- `duck docs search "metric query"`
- `duck api search semantic`

## Related API operations

- `createSemanticModel`
- `listSemanticMetrics`
- `createSemanticMetric`
- `runMetricQuery`

## Related docs

- [Semantic Layer](/reference/feature-semantic-layer)
- [Build and Run Models and Pipelines](/how-to/build-and-run-model-pipelines)
- [Query and Explore Data](/how-to/use-the-cli)
