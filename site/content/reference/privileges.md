---
title: Privileges
description: Understand the privilege vocabulary that appears across governance, assets, and operational APIs.
---

# Privileges

Duck uses privileges to define object reachability and mutation rights. The exact privilege names are enforced in the API and policy layers.

## Use This Page For

- mapping a task to the likely privilege boundary
- understanding why a `403` happened
- deciding whether a task belongs to a builder, operator, or consumer

## Common Patterns

| Area | Why it matters |
| --- | --- |
| Object reachability | A user cannot query or manage an object they cannot reach. |
| Asset orchestration | Defining assets and triggering materialization are different privilege boundaries. |
| Product management | Product ownership and publication actions should be more restricted than read-only discovery. |
| Governance changes | Grants, row filters, and column masks require explicit mutation rights. |
| Compute operations | Routing and endpoint management should stay with operators. |

## Practical Guidance

- start from the smallest object scope that supports the workflow
- separate read, mutate, and execute privileges where possible
- verify policy through the real user path after any change

## Related Reference

- [Governance API](/reference/generated/api/endpoints/governance)
- [Identity API](/reference/generated/api/endpoints/identity)
