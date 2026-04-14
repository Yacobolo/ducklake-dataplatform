---
title: Register Source Data
description: Bring source data into the right catalog, schema, and ownership structure before downstream work begins.
---

# Register Source Data

Use this guide when you are establishing the foundation for downstream models, assets, and products.

## Inputs

- source files or external locations
- the target catalog and schema
- a builder or admin path with create permissions

## Flow

1. choose the catalog and schema that match ownership and visibility
2. create or register the source table or view
3. document owner, comment, and intended usage
4. apply baseline grants before wider discovery
5. confirm the object is queryable through QuackStack

## What Good Looks Like

- the source exists in a stable namespace
- builders know who owns it
- consumers do not accidentally query raw, sensitive, or unstable objects without context

## Related Concepts

- [Platform Architecture](/docs/concepts/platform-architecture)
- [Governance Model](/docs/concepts/governance-model)

## Related Reference

- [Catalogs API](/reference/generated/api/endpoints/catalogs)
- [Declarative Reference](/reference/generated/declarative/)
