---
title: Author Notebooks and Publish Dashboards
description: Create notebooks, execute analysis, and publish dashboards backed by notebook, SQL, or semantic outputs.
doc_kind: task
audiences: [ai-agents, builders, end-users, admins]
product_areas: [notebooks, dashboards, queries, semantic-layer]
surfaces: [api, browser, cli]
tasks: [create notebook, run notebook job, publish dashboard, inspect dashboard widgets]
prerequisites: [interactive workspace access, source data or metric model, valid credential]
permissions: [notebook authoring access, dashboard publishing access, query access]
cli_commands: [docs search, api search notebook, api search dashboard]
command_groups: [docs, api]
operation_ids: [createNotebook, createNotebookSession, listNotebookJobs, createDashboard, createDashboardWidget, listDashboards]
api_tags: [Notebooks, Dashboards]
declarative_kinds: [notebook]
related_docs: [reference/feature-notebooks-and-dashboards, how-to/use-the-cli, how-to/define-semantic-metrics]
keywords: [notebook authoring, dashboard publishing, notebook jobs, dashboard widgets]
last_verified: 2026-03-12
source_of_truth: [docs, api/gen/openapi.yaml, schemas/declarative/v1/index.json]
---

# Author Notebooks and Publish Dashboards

## Objective

Create an interactive analysis workflow that starts in a notebook and ends in a reusable dashboard for other users.

## When to use

Use this workflow when the user needs iterative analysis, a saved analytical narrative, or a dashboard built from notebook, SQL, or semantic outputs.

## Prerequisites

- an environment URL and working credential
- a source table, view, or semantic model to analyze
- a notebook-capable workspace

## Required permissions

- notebook create and edit access
- query access to the source data
- dashboard create or publish access for the target folder or workspace

## Exact steps

### 1. Confirm the source you will analyze

- Identify the table, view, or metric source first.
- If you still need to locate data, use [Query and Explore Data](/how-to/use-the-cli).

Expected result: you know the exact object or metric source the notebook will use.

### 2. Create the notebook

Use the notebooks API surface to create the notebook shell and initial cells.

```bash
duck docs search "create notebook"
duck api search notebook
```

Expected result: a notebook exists with an identifier, title, and editable cells.

### 3. Open a notebook session and run cells

- Start a notebook session before running interactive work.
- Execute cells against the governed data source.
- Save useful result sets, SQL fragments, and notes as part of the notebook.

Expected result: the notebook contains a reproducible analysis with current outputs.

### 4. Decide whether the dashboard should read notebook outputs, SQL, or semantic metrics

- Use notebook or SQL-backed widgets for exploratory operational views.
- Use semantic metrics when the dashboard needs business-safe reusable KPIs.

Expected result: you have chosen the most stable source for downstream widgets.

### 5. Create the dashboard and add widgets

Use the dashboard operations to create the dashboard shell and attach charts or tables.

```bash
duck docs search "create dashboard widgets"
duck api search dashboard
```

Expected result: the dashboard exists with at least one widget pointing at the intended source.

### 6. Validate published output

- Open the dashboard as a viewer, not just as the author.
- Confirm governance behavior such as masked columns or filtered rows still makes sense in the published view.

Expected result: the published dashboard renders for the intended audience without exposing unauthorized data.

## Verified examples

- Notebook flow: create notebook, start notebook session, run cells, inspect notebook jobs.
- Dashboard flow: create dashboard, create dashboard widget, list dashboards, validate widget sources.

## Expected result

You end with a notebook that preserves the analytical workflow and a dashboard that exposes only the governed output intended for downstream users.

## Failure modes

- notebook session starts but queries fail: verify the notebook principal has query access to the source objects
- dashboard widget renders empty: confirm the widget source still exists and the viewer can access it
- viewer sees different values than the author: check row filters, column masks, and semantic-layer permissions
- notebook jobs fail repeatedly: inspect notebook job history and reduce assumptions about transient session state

## Related CLI commands

- `duck docs search "create notebook"`
- `duck docs search "dashboard widget"`
- `duck api search notebook`
- `duck api search dashboard`

## Related API operations

- `createNotebook`
- `createNotebookSession`
- `listNotebookJobs`
- `createDashboard`
- `createDashboardWidget`
- `listDashboards`

## Related docs

- [Notebooks and Dashboards](/reference/feature-notebooks-and-dashboards)
- [Query and Explore Data](/how-to/use-the-cli)
- [Define Semantic Metrics](/how-to/define-semantic-metrics)
