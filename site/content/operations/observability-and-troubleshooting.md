---
title: Observability And Troubleshooting
description: Troubleshoot auth, policy, storage, freshness, and worker issues with clear decision paths.
---

# Observability And Troubleshooting

Use this guide when the platform is behaving unexpectedly and you need to quickly localize the failure domain.

## Start With The Symptom

- `401` or login failure usually means an auth or credential problem
- `403` usually means a grant or policy issue
- missing rows or transformed values may be the intended result of row filters or masks
- stale products or metrics may be a freshness, run, or upstream availability issue
- slow or stuck work may be a compute routing or worker health issue

## Core Checks

1. check health and metrics endpoints
2. confirm the principal and credential path
3. verify the target object, product, or metric still exists
4. inspect lineage, freshness, and asset or model run state
5. inspect compute routing and worker health if execution is remote

## Escalation Boundaries

- auth and identity issues go to the platform access owner
- policy and object exposure issues go to governance or product owners
- stale outputs go to builders responsible for upstream models and assets
- compute and worker issues go to operators

## Related Reference

- [Health API](/reference/generated/api/endpoints/health)
- [Queries API](/reference/generated/api/endpoints/queries)
- [Assets API](/reference/generated/api/endpoints/assets)
