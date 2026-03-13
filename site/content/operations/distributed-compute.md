---
title: Distributed Compute
description: Roll out remote workers without moving identity, policy, or governance out of the control plane.
---

# Distributed Compute

This runbook describes how to roll out remote compute without weakening Duck’s security and governance model.

## Architecture Boundaries

- the gateway remains the single policy enforcement point
- workers execute already-rewritten SQL
- gateway-to-worker transport uses internal gRPC
- storage, auth, and governance metadata remain anchored in the control plane

## When to Use Remote Compute

- you need worker isolation or a separate execution fleet
- you want lifecycle-style async execution
- you need a staged rollout with local fallback
- query or orchestration load makes local-only execution an operator bottleneck

## Admin Checklist

- confirm the gateway feature flags match the intended rollout
- set worker auth and listen addresses explicitly
- start with fallback enabled on assignments
- canary a small set of users or groups before widening traffic
- monitor queue latency and failure reasons before widening scope

## Worker Settings

- `AGENT_TOKEN`
- `LISTEN_ADDR`
- `GRPC_LISTEN_ADDR`
- `MAX_MEMORY_GB`
- `QUERY_RESULT_TTL`
- `QUERY_CLEANUP_INTERVAL`

## Gateway Settings

- `FEATURE_REMOTE_ROUTING`
- `FEATURE_ASYNC_QUEUE`
- `FEATURE_CURSOR_MODE`
- `FEATURE_INTERNAL_GRPC`
- `REMOTE_CANARY_USERS`

## Health and Failure Handling

- monitor `GET /health` and `GET /metrics`
- expect fallback behavior when worker health degrades and assignments allow local execution
- use retention settings to control in-memory lifecycle result pressure
- document the operator decision for when fallback should be automatic versus disabled

## Rollout Sequence

1. enable remote support with local fallback
2. route a limited audience
3. observe queue latency and completion behavior
4. widen scope only after representative success

## Next Steps

- [Platform Settings](/operations/configuration)
- [Security Checklist](/operations/security-checklist)
- [Observability And Troubleshooting](/operations/observability-and-troubleshooting)

## Related Reference

- [Compute API](/reference/generated/api/endpoints/compute)
