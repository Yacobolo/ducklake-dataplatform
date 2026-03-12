---
title: Models and Pipelines
description: Use this feature guide for models, macros, model runs, and pipeline orchestration workflows.
doc_kind: overview
audiences: [ai-agents, builders, admins]
product_areas: [models, pipelines]
surfaces: [api, cli, declarative]
tasks: [define models, run transformations, inspect run status, orchestrate pipelines]
prerequisites: [project or build context, compute access]
permissions: [builder or orchestration access]
cli_commands: [apply, api search model, api search pipeline]
command_groups: [apply, api]
operation_ids: [createModel, listModels, createPipeline, listPipelineRuns]
api_tags: [Models, Pipelines]
declarative_kinds: [model, macro]
related_docs: [how-to/catalog-and-ingestion, reference/cli, core-concepts/index, how-to/build-and-run-model-pipelines, how-to/declarative-workflows]
keywords: [models, macros, pipelines, orchestration]
last_verified: 2026-03-12
source_of_truth: [docs, api/gen/openapi.yaml, schemas/declarative/v1/index.json]
---

# Models and Pipelines

## When to use

Use this guide when the workflow manages transformation logic, model tests, or multi-step orchestration that publishes governed outputs.

## Primary tasks

- create and revise models and macros
- run and inspect model runs plus model tests
- define pipelines and inspect pipeline job runs

## Exact entry points

- Start with [Load Data and Build Assets](/how-to/catalog-and-ingestion) when the pipeline produces reusable outputs.
- Use [Advanced CLI Reference](/reference/cli) if the workflow is automation-heavy.
- Use [Advanced API Reference](/reference/api) for exact run and orchestration contracts.
- Use [Build and Run Models and Pipelines](/how-to/build-and-run-model-pipelines) for the authored production workflow.

## Generated reference

- [Models endpoints](/reference/generated/api/endpoints/models)
- [Pipelines endpoints](/reference/generated/api/endpoints/pipelines)
