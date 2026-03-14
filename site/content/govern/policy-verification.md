---
title: Policy Verification
description: Prove that grants, filters, masks, freshness, and run behavior match intent before users discover a mistake.
---

# Policy Verification

Policy is only real when it survives the actual query path.

## Inputs

- a target principal
- the object, metric, or product being validated
- the expected visible rows and column behavior

## Flow

1. test access using the same surface the user will use
2. verify allow, deny, masked, and filtered outcomes explicitly
3. inspect lineage or freshness if the issue may be stale upstream data rather than policy
4. repeat after any grant, filter, mask, or routing change

## What To Verify

- authorization outcome
- row-level visibility
- masked field behavior
- freshness state where the consumer contract includes it
- notebook, model, or asset run health where the output is operationally produced

## Related Reference

- [Queries API](/reference/generated/api/endpoints/queries)
- [Assets API](/reference/generated/api/endpoints/assets)
- [Lineage API](/reference/generated/api/endpoints/lineage)
