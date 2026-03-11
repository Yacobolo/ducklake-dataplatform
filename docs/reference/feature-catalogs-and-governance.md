---
title: Catalogs and Governance
description: Use this feature guide for catalogs, schemas, views, grants, row filters, and column masks.
doc_kind: overview
audiences: [ai-agents, admins, builders]
product_areas: [catalogs, governance]
surfaces: [api, cli, declarative, sql]
tasks: [create object hierarchy, grant access, enforce policy, inspect securables]
prerequisites: [target catalog design, principal model, admin path]
permissions: [catalog administration, governance administration]
cli_commands: [catalog create, security grants create, security row-filters create]
command_groups: [catalog, security]
operation_ids: [registerCatalog, createSchema, createGrant, createRowFilter, createColumnMask]
api_tags: [Catalogs, Governance]
declarative_kinds: [catalog, schema, table, view, volume, grant-list, row-filter-list, column-mask-list, tag-config]
related_docs: [how-to/access-control, how-to/catalog-and-ingestion, core-concepts/access-control]
keywords: [catalogs, grants, row filters, column masks, governance]
last_verified: 2026-03-12
source_of_truth: [docs, api/gen/openapi.yaml, schemas/declarative/v1/index.json]
---

# Catalogs and Governance

## When to use

Use this guide when the workflow changes object structure, object visibility, or the policy envelope around data access.

## Primary tasks

- create and evolve catalogs, schemas, tables, views, and volumes
- model grants on securables
- attach row filters and column masks
- connect governance workflows to declarative state management

## Exact entry points

- Start with [Manage Access](/how-to/access-control) for end-to-end permission changes.
- Use [Load Data and Build Assets](/how-to/catalog-and-ingestion) for catalog creation plus ingestion.
- Use [Platform Objects and Governance](/core-concepts/access-control) for the mental model.

## Generated reference

- [Catalogs endpoints](/reference/generated/api/endpoints/catalogs)
- [Governance endpoints](/reference/generated/api/endpoints/governance)
- [Generated declarative reference](/reference/generated/declarative/index)
