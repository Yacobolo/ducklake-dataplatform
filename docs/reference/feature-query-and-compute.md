---
title: Query and Compute
description: Use this feature guide for query execution, async queries, compute endpoints, and health checks.
doc_kind: overview
audiences: [ai-agents, end-users, admins, builders]
product_areas: [queries, compute]
surfaces: [api, cli, sql, remote-compute]
tasks: [run queries, route execution, inspect compute health, reason about failure domains]
prerequisites: [query access, target environment]
permissions: [query access or compute administration]
cli_commands: [query run, api search query, api search compute]
command_groups: [query, api]
operation_ids: [submitQuery, executeQuery, listComputeEndpoints, getComputeEndpointHealth, getHealth]
api_tags: [Queries, Compute, Health]
declarative_kinds: [compute-endpoint-list, compute-assignment-list, compute-routing-defaults]
related_docs: [start-here/quickstart, core-concepts/compute-and-query, operations/distributed-compute]
keywords: [queries, compute endpoints, async queries, health]
last_verified: 2026-03-12
source_of_truth: [docs, api/gen/openapi.yaml, schemas/declarative/v1/index.json]
---

# Query and Compute

## When to use

Use this guide when the workflow is about running SQL, choosing where it runs, or understanding how compute topology affects execution without changing policy enforcement.

## Primary tasks

- submit synchronous and asynchronous queries
- inspect query history and failure states
- manage compute endpoints and assignments
- verify health and rollout posture for remote execution

## Exact entry points

- Start with [Quickstart](/start-here/quickstart) for the first secure query.
- Use [Query and Explore Data](/how-to/use-the-cli) for discovery and querying.
- Use [Distributed Compute](/operations/distributed-compute) for rollout and operating guidance.

## Generated reference

- [Queries endpoints](/reference/generated/api/endpoints/queries)
- [Compute endpoints](/reference/generated/api/endpoints/compute)
- [Health endpoints](/reference/generated/api/endpoints/health)
