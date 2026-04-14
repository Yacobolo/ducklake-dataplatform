---
title: Semantic Layer
description: Understand semantic models, metrics, relationships, pre-aggregations, and metric query flow.
---

# Semantic Layer

If you only remember one thing, remember this: the semantic layer turns technical data structures into business-facing definitions that consumers can query consistently.

## Why It Matters

Consumers should not have to remember every join, every grain, or every aggregation rule behind a KPI. The semantic layer exists so builders can publish those definitions once and let everyone else ask for business concepts instead of reconstructing the logic by hand.

## What A Semantic Model Is

A semantic model is not the same thing as a transformation model or a data product.

- a transformation model defines how data is produced technically
- a semantic model defines how data should be understood and queried from a business point of view
- a data product packages outputs and contracts for discovery and ownership

In QuackStack, a semantic model usually sits on top of a trusted base model and provides a business-facing interface to it.

## What The Semantic Layer Contains

A semantic model usually sits on top of a trusted base model and defines:

- dimensions and time grain
- metrics and metric types
- relationships to other semantic models
- pre-aggregations for performance

## Query Planning Flow

<figure class="my-8 overflow-x-auto rounded-[1.5rem] border border-[var(--borderColor-default)] bg-[var(--bgColor-inset)] p-5">
  <img class="mx-auto block h-auto w-max max-w-none rounded-none border-0 bg-transparent" src="/_site/diagrams/semantic-query-flow.svg" alt="Diagram showing semantic models and metrics resolving through the semantic planner into governed query execution." loading="lazy" decoding="async">
</figure>

Read the diagram from left to right. Semantic models define the available business structure. Metrics sit on top of those models because they are the named measurements consumers ask for. The metric planner sits in the middle because it translates business-facing requests into a governed query plan. The QuackStack API and governed SQL execution sit on the right because the semantic layer still runs through the same secure platform path as any other query.

## Metrics, Relationships, And Pre-Aggregations

A metric is a named business measurement, such as gross revenue or weekly active riders. A relationship explains how one semantic model connects to another. A pre-aggregation is a stored or managed summary that exists to make repeated metric queries faster.

These matter because the semantic layer is not only about naming columns. It is about giving consumers a stable vocabulary and letting the platform plan repeatable metric queries around that vocabulary.

## Example In QuackStack

Imagine a finance team wants to ask for `gross_revenue` by `pickup_zone` and week. Without a semantic layer, every analyst would need to know which curated model to start from, which joins are allowed, which timestamp column defines the reporting grain, and how gross revenue is calculated. With a semantic model, the builder defines those choices once. The consumer asks for the metric. QuackStack explains or runs the metric query against the governed base model and can also report freshness for that metric.

## Common Misunderstandings

- A semantic model is not just another table name. It is a business-facing contract on top of trusted data.
- A metric is not merely a saved SQL query. It is a named semantic definition the platform can reason about.
- A data product may publish semantic entrypoints, but it is not itself the semantic layer.

## Related Tasks

- [Semantic Models And Metrics](/docs/build/semantic-models-and-metrics)
- [Data Products](/docs/concepts/data-products)
- [Lineage And Freshness](/docs/concepts/lineage-and-freshness)

## Related Reference

- [Semantic Layer API](/reference/generated/api/endpoints/semantic-layer)
- [Declarative Reference](/reference/generated/declarative/)
