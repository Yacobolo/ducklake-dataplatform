---
title: Manage Access
description: Create secure access paths with groups, grants, row filters, and column masks.
doc_kind: task
audiences: [ai-agents, admins, builders]
product_areas: [governance, identity, security]
surfaces: [api, cli, declarative]
tasks: [grant access, manage groups, apply row filters, apply column masks]
prerequisites: [admin credential, target principals, target securables]
permissions: [grant management, governance policy management]
cli_commands: [security grants create, security groups create, security row-filters create, security column-masks create]
command_groups: [security]
operation_ids: [createGrant, createGroup, createRowFilter, createColumnMask]
api_tags: [Governance, Identity]
declarative_kinds: [grant-list, group-list, row-filter-list, column-mask-list]
related_docs: [how-to/authentication, operations/security-checklist, reference/feature-catalogs-and-governance]
keywords: [rbac, row filter, column mask, grants]
last_verified: 2026-03-12
source_of_truth: [docs, api/gen/openapi.yaml, schemas/declarative/v1/index.json]
---

# Manage Access

Use this flow when the platform is available and you need to make sure the right users can see the right data safely.

## Inputs

- a Duck environment
- an admin-authenticated path
- target catalog, schema, or table names
- the principal or group you want to manage

## Workflow

1. create or identify the principal
2. create a group if access will be shared
3. grant minimum required privileges
4. add row filters for scoped visibility
5. add column masks for sensitive fields
6. verify with a principal-scoped query

## Why This Sequence Matters

Grants define reachability. Filters and masks refine visibility. If you skip verification at the end, you risk believing the policy is correct without testing the actual query path.

## What Good Access Management Looks Like

- analysts can find and query the data they need
- service accounts have only the minimum required privileges
- sensitive rows and columns stay protected without breaking legitimate workflows

## Expected Result

- the target principal can query only the intended object scope
- restricted rows stay hidden
- sensitive columns are masked as designed

## Verification

Run a query as the target principal or service identity and compare the result set against your expected policy outcome.

## Next Steps

- [Security Checklist](/operations/security-checklist)
- [Platform Objects](/core-concepts/access-control)

## Related Reference

- [Advanced API Reference](/reference/api)
- [Glossary](/reference/glossary)
