---
title: Governance Model
description: Understand RBAC, row filters, and column masks as one layered enforcement model.
---

# Governance Model

If you only remember one thing, remember this: Duck treats authentication, grants, row filters, and column masks as separate layers because they solve different problems.

## Why It Matters

Teams often assume access control begins and ends with “can this user query the table?” In Duck, that is only the first question. A principal may be allowed to reach an object and still be allowed to see only some rows or transformed values. The governance model exists so teams can share useful data safely without cloning everything into separate per-audience copies.

## The Four Layers

| Layer | Question Answered | Functional Goal |
| --- | --- | --- |
| 1. **Authentication** | Who is this principal? | Identity Verification |
| 2. **Grants** | May they reach this object? | Reachability/Access |
| 3. **Row Filters** | Which rows can they see? | Scoped Visibility |
| 4. **Column Masks** | Should the raw value be shown? | Transformation/Redaction |

## How They Apply In Order

1. authentication proves who the principal is
2. grants decide whether the principal can access the object
3. row filters decide which rows survive
4. column masks decide how sensitive values are transformed

## Governance At A Glance

<figure class="my-8 overflow-x-auto rounded-[1.5rem] border border-[var(--borderColor-default)] bg-[var(--bgColor-inset)] p-5">
  <img class="mx-auto block h-auto w-max max-w-none rounded-none border-0 bg-transparent" src="/_site/diagrams/platform-object-map.svg" alt="Diagram showing principals and groups connecting to governed objects such as catalogs, tables, models, assets, and data products." loading="lazy" decoding="async">
</figure>

Read the diagram as a relationship map, not a step-by-step execution flow. Principals and groups sit on the left because governance starts with identity. The governed objects sit on the right because those are the things teams want to expose: tables, models, assets, and products. The point of the diagram is that governance is attached to the objects people actually use, not only to raw storage.

## How This Relates To Other Concepts

Governance is easy to confuse with architecture or products:

- [Platform Architecture](/docs/concepts/platform-architecture) explains where policy is enforced
- this page explains what policy decisions are made
- [Data Products](/docs/concepts/data-products) explain how governed outputs are packaged for discovery

## Example In Duck

Imagine two analysts querying the same sales table. Both analysts authenticate successfully. Both have a grant that lets them reach the table. One analyst belongs to the APAC group, so a row filter shows only APAC rows. Both analysts can still see a customer email column, but a column mask rewrites the value to a redacted form. In that example:

- auth proved identity
- the grant allowed reachability
- the row filter narrowed the result set
- the mask transformed a sensitive field

That is why Duck separates these layers. If all four concerns were collapsed into a single rule, the system would be much harder to reason about and much harder to operate safely.

## Common Misunderstandings

- A grant does not mean “full visibility.” It only means the object is reachable.
- A row filter does not deny the object entirely. It narrows the rows that survive.
- A mask does not necessarily hide a column. It can transform the value while still returning the column.
- Product ownership metadata is not itself an access control rule, but it matters because it tells people how to request or justify access.

## Related Tasks

- [Authentication And Identities](/docs/govern/authentication-and-identities)
- [Access Design](/docs/govern/access-design)
- [Policy Verification](/docs/govern/policy-verification)

## Related Reference

- [Privileges](/reference/privileges)
- [Glossary](/reference/glossary)
