---
title: Build and Run Models and Pipelines
description: Define transformation logic, validate model outputs, and orchestrate repeatable pipeline runs.
doc_kind: task
audiences: [ai-agents, builders, admins]
product_areas: [models, pipelines, assets, declarative]
surfaces: [api, cli, declarative]
tasks: [define model, create macro, run model, create pipeline, inspect pipeline run]
prerequisites: [source data, compute access, project build context]
permissions: [builder access, orchestration access, object creation access]
cli_commands: [apply, docs search, api search model, api search pipeline]
command_groups: [apply, docs, api]
operation_ids: [createModel, createModelTest, listModels, listModelRuns, createPipeline, listPipelineJobs, createPipelineJob, listPipelineRuns]
api_tags: [Models, Pipelines]
declarative_kinds: [model, macro, asset]
related_docs: [reference/feature-models-and-pipelines, how-to/catalog-and-ingestion, how-to/declarative-workflows]
keywords: [models, macros, pipelines, orchestrate transformations, model runs]
last_verified: 2026-03-12
source_of_truth: [docs, api/gen/openapi.yaml, schemas/declarative/v1/index.json]
---

# Build and Run Models and Pipelines

## Objective

Turn source data into reusable, governed outputs through model definitions and repeatable orchestration.

## When to use

Use this workflow when one query is no longer enough and the transformation must be versioned, tested, and rerun reliably.

## Prerequisites

- a source catalog and schema with readable input data
- compute capacity for model and pipeline execution
- a build context that stores model logic, macros, and related configuration

## Required permissions

- model and asset creation access in the target catalog
- pipeline creation and run access
- read access to all upstream dependencies

## Exact steps

### 1. Define the target outputs

- Decide which tables, views, or assets the pipeline is expected to publish.
- Map the upstream dependencies and ownership before writing transformations.

Expected result: you know the destination objects and the upstream lineage they depend on.

### 2. Create or update model logic

- Define the model body and any shared macros.
- Add model tests for assumptions that must hold across runs.

```bash
duck docs search "create model"
duck api search model
```

Expected result: the model exists and its dependencies are explicit.

### 3. Validate the model path before orchestration

- Run the model or model test path directly first.
- Confirm the output shape and governance expectations before adding scheduling or orchestration layers.

Expected result: the model can produce the intended output without pipeline-level troubleshooting noise.

### 4. Create the pipeline and pipeline jobs

- Define the sequence of jobs that materialize or refresh the target outputs.
- Keep the first pipeline simple enough to debug step-by-step.

```bash
duck docs search "create pipeline job"
duck api search pipeline
```

Expected result: a pipeline exists with jobs that map cleanly to the desired transformation stages.

### 5. Trigger a pipeline run and inspect status

- Start a run and inspect both job-level and pipeline-level status.
- Verify failure handling on one stage before trusting the pipeline in automation.

Expected result: you can identify which job succeeded, failed, or is still running.

### 6. Move stable resources into declarative management

- Once the workflow stabilizes, represent long-lived resources declaratively.
- Keep one-off experimentation separate from production state.

Expected result: the production path is repeatable and diffable instead of ad hoc.

## Verified examples

- Model flow: create model, create model test, list models, list model runs.
- Pipeline flow: create pipeline, create pipeline job, list pipeline jobs, list pipeline runs.

## Expected result

You end with tested transformation logic and a repeatable orchestration path that publishes governed outputs on demand or on schedule.

## Failure modes

- model compiles but returns wrong output shape: verify upstream schema drift before changing downstream contracts
- model tests fail intermittently: isolate data freshness assumptions from deterministic transformation checks
- pipeline run fails after model success: inspect job sequencing, credentials, and target object conflicts
- reruns produce unexpected duplicates or stale outputs: verify idempotency and downstream overwrite behavior

## Related CLI commands

- `duck docs search "create model"`
- `duck docs search "create pipeline job"`
- `duck api search model`
- `duck api search pipeline`
- `duck apply --config-dir <path>`

## Related API operations

- `createModel`
- `createModelTest`
- `listModels`
- `listModelRuns`
- `createPipeline`
- `listPipelineJobs`
- `createPipelineJob`
- `listPipelineRuns`

## Related docs

- [Models and Pipelines](/reference/feature-models-and-pipelines)
- [Load Data and Build Assets](/how-to/catalog-and-ingestion)
- [Work Declaratively](/how-to/declarative-workflows)
