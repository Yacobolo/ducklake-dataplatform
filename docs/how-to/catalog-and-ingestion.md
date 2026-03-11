---
title: Load Data and Build Assets
description: Bring data into Duck and build tables, models, views, and other reusable assets.
doc_kind: task
audiences: [ai-agents, builders, admins]
product_areas: [catalogs, storage, assets, ingestion]
surfaces: [api, cli, declarative]
tasks: [create catalog hierarchy, ingest data, register governed assets]
prerequisites: [storage location, compute access, target catalog design]
permissions: [catalog administration, storage configuration]
cli_commands: [catalog create, catalog schemas create, storage external-locations create, apply]
command_groups: [catalog, storage, apply]
operation_ids: [registerCatalog, createSchema, createExternalLocation, loadTableExternalFiles]
api_tags: [Catalogs, Storage, Assets]
declarative_kinds: [catalog, schema, table, view, volume, asset, external-location-list, storage-credential-list]
related_docs: [how-to/declarative-workflows, reference/feature-storage-and-integrations, reference/feature-assets-and-lineage]
keywords: [ingestion, landing zone, catalog lifecycle]
last_verified: 2026-03-12
source_of_truth: [docs, api/gen/openapi.yaml, schemas/declarative/v1/index.json]
---

# Load Data and Build Assets

This flow is for teams moving from raw data access toward reusable, governed data products.

## Inputs

- a Duck environment
- storage or source data available to load
- a builder or admin path with object-management permissions

## Typical Lifecycle

1. define or choose the target catalog and schema
2. register the objects you want users to see
3. load or ingest source data
4. build downstream tables, views, models, notebooks, or assets
5. apply governance controls before broad access

## Expected Result

At the end of this flow:

- the target objects exist in the intended catalog and schema
- source data is ingested or registered
- downstream consumers can discover the governed outputs they are allowed to use

## What This Covers

- catalog and schema structure
- ingestion into managed tables
- creation of views, models, assets, notebooks, and pipelines
- governance before broad consumption

## Next Steps

- [Query and Explore Data](/how-to/use-the-cli)
- [Govern & Administer](/operations/)

## Related Reference

- [Advanced API Reference](/reference/api)
- [Build Data Products](/core-concepts/)
