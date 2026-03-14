---
title: Access Design
description: Design least-privilege grants, row filters, and column masks around real usage patterns.
---

# Access Design

Use this guide when you want to make access both safe and usable.

## Inputs

- the principal or group
- the target object or product
- row-level and column-level protection requirements

## Flow

1. start with the product or object the user should reach
2. grant the minimum privilege required
3. add row filters where visibility depends on tenant, geography, or audience
4. add column masks where exposure should be transformed rather than denied
5. document the request path when access needs human approval

## Verification Criteria

- the principal can reach only the intended objects
- restricted rows stay out of the result set
- sensitive fields do not leak raw values

## Related Reference

- [Governance API](/reference/generated/api/endpoints/governance)
- [Privileges](/reference/privileges)
