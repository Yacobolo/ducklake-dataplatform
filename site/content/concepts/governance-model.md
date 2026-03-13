---
title: Governance Model
description: Understand RBAC, row filters, and column masks as one layered enforcement model.
---

# Governance Model

Duck’s governance model is layered. Each layer answers a different question.

## Why It Matters

Teams often treat grants as the whole access model. In Duck, grants define reachability, while row filters and column masks define safe exposure inside the reachable surface.

## Mental Model

1. authentication proves who the principal is
2. grants decide whether the principal can access the object
3. row filters decide which rows survive
4. column masks decide how sensitive values are transformed

## Governance At A Glance

<figure class="site-mermaid">
  <img src="/_site/diagrams/platform-object-map.svg" alt="Diagram showing principals and groups connecting to governed objects such as catalogs, tables, models, assets, and data products." loading="lazy" decoding="async">
</figure>

## Key Objects

- principal and group
- grant and privilege
- row filter and filter binding
- column mask and mask binding
- product request path and ownership metadata

## Related Tasks

- [Authentication And Identities](/govern/authentication-and-identities)
- [Access Design](/govern/access-design)
- [Policy Verification](/govern/policy-verification)

## Related Reference

- [Privileges](/reference/privileges)
- [Glossary](/reference/glossary)
