---
title: Assets and Lineage
description: Use this feature guide for governed assets, checks, runs, and lineage inspection.
doc_kind: overview
audiences: [ai-agents, builders, admins]
product_areas: [assets, lineage]
surfaces: [api, cli, declarative]
tasks: [define assets, run checks, inspect lineage, reason about downstream impact]
prerequisites: [catalog objects or transformation outputs to govern]
permissions: [asset administration or lineage visibility]
cli_commands: [apply, api search asset, api search lineage]
command_groups: [apply, api]
operation_ids: [createAsset, listAssets, getAssetGraph, getTableLineage]
api_tags: [Assets, Lineage]
declarative_kinds: [asset]
related_docs: [how-to/catalog-and-ingestion, reference/api, core-concepts/index]
keywords: [assets, checks, lineage, asset graph]
last_verified: 2026-03-12
source_of_truth: [docs, api/gen/openapi.yaml, schemas/declarative/v1/index.json]
---

# Assets and Lineage

## When to use

Use this guide when the workflow is about governed asset definitions, data quality checks, backfills, or dependency visibility between upstream and downstream data products.

## Primary tasks

- define and materialize governed assets
- inspect asset graphs and quality checks
- understand upstream and downstream lineage before rollout

## Exact entry points

- Start with [Load Data and Build Assets](/how-to/catalog-and-ingestion) for ingestion and governed output creation.
- Use [Build Data Products](/core-concepts/) for the concept model.
- Use [Advanced API Reference](/reference/api) for exact lineage and asset operations.

## Generated reference

- [Assets endpoints](/reference/generated/api/endpoints/assets)
- [Lineage endpoints](/reference/generated/api/endpoints/lineage)
