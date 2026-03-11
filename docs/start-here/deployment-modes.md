---
title: Ways to Access Duck
description: Understand the main access paths for users, builders, and admins.
doc_kind: overview
audiences: [ai-agents, end-users, builders, admins]
product_areas: [auth, compute, integrations]
surfaces: [browser, sql, api, cli, remote-compute]
tasks: [choose access path, map workflow to surface, plan access mode]
prerequisites: [deployment URL, organization guidance]
permissions: [approved access path]
cli_commands: [auth login, docs search]
command_groups: [auth, docs]
operation_ids: [localLogin, getComputeEndpointHealth]
api_tags: [Auth, Compute]
declarative_kinds: []
related_docs: [start-here/quickstart, how-to/authentication, operations/distributed-compute]
keywords: [deployment modes, access modes, sql clients]
last_verified: 2026-03-12
source_of_truth: [docs, api/gen/openapi.yaml]
---

# Ways to Access Duck

Duck supports multiple access paths so different users can work in the way that fits them best. Most end users should think about access in terms of product experience, not infrastructure.

## Browser Access

Use this when:

- you are exploring the platform interactively
- your organization uses browser-based sign-in
- you want a guided product experience

## SQL and BI Access

Use this when:

- you want to connect query tools or BI clients
- your team works directly in SQL
- you need a familiar database-style experience

## API and Automation Access

Use this when:

- you are automating workflows
- you need repeatable scripted access
- you are integrating Duck into a larger platform flow

## Remote Compute Access

Use this when:

- your admins have enabled remote execution
- you need worker isolation or a separate compute fleet
- execution topology matters for scale or control

Read [Distributed Compute](/operations/distributed-compute) before rollout.

## Decision Guide

| Need | Recommended path |
| --- | --- |
| Guided product experience | Browser access |
| Direct query workflows | SQL or BI access |
| Automation and integrations | API access |
| Isolated or scaled execution | Remote compute |

## Next Steps

- [Quickstart](/start-here/quickstart)
- [Use the Platform](/how-to/)
- [Govern & Administer](/operations/)
