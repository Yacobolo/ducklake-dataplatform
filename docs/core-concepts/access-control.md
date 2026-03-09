---
title: Platform Objects and Governance
description: Understand the objects users consume and the governance layers that shape visibility.
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
