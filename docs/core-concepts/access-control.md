---
title: Platform Objects and Governance
description: Understand the objects users consume and the governance layers that shape visibility.
doc_kind: concept
audiences: [ai-agents, end-users, builders, admins]
product_areas: [catalogs, governance, identity]
surfaces: [api, cli, declarative]
tasks: [understand securables, model access, reason about policy effects]
prerequisites: [basic platform familiarity]
permissions: [documentation access]
cli_commands: [security grants list, find tables]
command_groups: [security, find]
operation_ids: [listGrants, listCatalogs, listSchemas]
api_tags: [Catalogs, Governance, Identity]
declarative_kinds: [catalog, schema, grant-list, group-list, row-filter-list, column-mask-list]
related_docs: [how-to/access-control, reference/feature-catalogs-and-governance, reference/glossary]
keywords: [securables, governance model, principals]
last_verified: 2026-03-12
source_of_truth: [docs, api/gen/openapi.yaml, schemas/declarative/v1/index.json]
---

# Platform Objects and Governance

Duck data products live inside a governed object hierarchy. Users query catalogs, schemas, tables, and views, while admins and builders shape visibility with grants, row filters, and column masks.

## Core Objects

### Catalogs, schemas, tables, and views

- catalogs are the top-level container users browse first
- schemas group related objects
- tables hold data
- views expose reusable logic over data

### Principals, groups, and grants

- principals represent users or services
- groups simplify access management at team scale
- grants determine whether a principal can reach an object

### Row filters and column masks

- row filters limit which rows are visible to a given principal or group
- column masks rewrite or obfuscate sensitive values for selected audiences
- both are useful when broad object access is allowed but full data exposure is not

## Mental Model

Think of enforcement as layers:

1. authentication establishes identity
2. grants decide whether the object can be accessed
3. row filters narrow the result set
4. column masks redact or transform sensitive values

## Why This Matters

This layered model is the platform's core safety mechanism. Builders and admins should treat grants, row filters, and masks as part of the data product contract for every sensitive dataset.

## Next Steps

- [Manage Access](/how-to/access-control)
- [Security Checklist](/operations/security-checklist)

## Related Reference

- [Glossary](/reference/glossary)
