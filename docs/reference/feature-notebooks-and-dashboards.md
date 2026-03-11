---
title: Notebooks and Dashboards
description: Use this feature guide for notebook authoring, notebook jobs, and dashboard publishing workflows.
doc_kind: overview
audiences: [ai-agents, builders, end-users, admins]
product_areas: [notebooks, dashboards]
surfaces: [api, browser, cli]
tasks: [author notebooks, run notebook jobs, publish dashboards, inspect dashboard sources]
prerequisites: [interactive workspace access]
permissions: [notebook or dashboard access]
cli_commands: [api search notebook, api search dashboard]
command_groups: [api]
operation_ids: [createNotebook, listNotebookJobs, createDashboard, listDashboards]
api_tags: [Notebooks, Dashboards]
declarative_kinds: [notebook]
related_docs: [reference/api, reference/feature-query-and-compute, how-to/catalog-and-ingestion]
keywords: [notebooks, dashboard, interactive analysis, notebook jobs]
last_verified: 2026-03-12
source_of_truth: [docs, api/gen/openapi.yaml, schemas/declarative/v1/index.json]
---

# Notebooks and Dashboards

## When to use

Use this guide when the workflow depends on interactive authoring, notebook execution state, or dashboard publication over notebook, SQL, or semantic sources.

## Primary tasks

- create and revise notebooks and notebook cells
- track notebook sessions and scheduled jobs
- create dashboards and inspect widget sources

## Exact entry points

- Use [Advanced API Reference](/reference/api) for notebook session and dashboard widget contract details.
- Use [Load Data and Build Assets](/how-to/catalog-and-ingestion) when notebooks or dashboards sit downstream of governed tables and models.

## Generated reference

- [Notebooks endpoints](/reference/generated/api/endpoints/notebooks)
- [Dashboards endpoints](/reference/generated/api/endpoints/dashboards)
