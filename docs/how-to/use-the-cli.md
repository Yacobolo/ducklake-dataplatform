---
title: Query and Explore Data
description: Discover catalogs, inspect objects, and query data through Duck's available access surfaces.
doc_kind: task
audiences: [ai-agents, end-users, builders]
product_areas: [queries, catalogs, governance]
surfaces: [cli, api, sql]
tasks: [discover data, inspect objects, run secure queries]
prerequisites: [deployment URL, valid credential, query access]
permissions: [catalog visibility, query access]
cli_commands: [find tables, describe table, query run, api search]
command_groups: [find, describe, query, api]
operation_ids: [listCatalogs, listSchemas, getTable, submitQuery]
api_tags: [Catalogs, Queries]
declarative_kinds: []
related_docs: [start-here/quickstart, how-to/access-control, reference/feature-query-and-compute]
keywords: [cli query, explore data, discover objects]
last_verified: 2026-03-12
source_of_truth: [docs, api/gen/openapi.yaml]
---

# Query and Explore Data

Most users think about Duck in terms of finding data and querying it. The exact surface can vary by organization, but the workflow is consistent.

## Common Access Surfaces

- browser-based product experiences
- SQL-compatible clients
- API-backed tools and scripted clients
- CLI workflows for advanced users

## Core Workflows

### Find what you can use

- search for catalogs, schemas, tables, and views
- inspect object names and schemas
- confirm whether an object is appropriate for your use case

### Run a query safely

```bash
curl -X POST "https://your-duck-host/v1/query" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <token>" \
  -d '{"sql":"SELECT borough, pickup_zone, trip_count, gross_revenue FROM sample_data.nyc_taxi.zone_metrics ORDER BY gross_revenue DESC LIMIT 10"}'
```

Expected result: a result set from the built-in `sample_data` catalog, or a permissions response that tells you additional access is required.

### Understand access effects

- if rows are filtered, you may see only a restricted subset
- if columns are masked, sensitive values may be transformed or hidden
- if the query is denied, request access instead of assuming the object is broken

## Expected Result

You should be able to find relevant data, query it through an approved access path, and understand whether governance policies affect what you see.

## Next Steps

- [Manage Access](/how-to/access-control)
- [Build Data Products](/core-concepts/)

## Related Reference

- [Glossary](/reference/glossary)
- [Advanced Reference](/reference/)
