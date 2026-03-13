---
title: Transformation Framework
description: Understand models, macros, tests, notebooks, runs, DAGs, and promotion workflows.
---

# Transformation Framework

If you only remember one thing, remember this: Duck’s transformation framework is the builder workflow that turns raw inputs into trusted outputs through reusable logic, tests, exploration, and repeatable runs.

## Why It Matters

Teams need more than one-off SQL files. They need a place for transformation logic to live, a way to reuse common patterns, quality gates that run with the work, and a path from exploratory notebook work into durable models. The transformation framework exists so those concerns can live in one system instead of being scattered across scripts, notebooks, and ad hoc jobs.

## What Lives In The Transformation Framework

| Object | What It Is | What It Is Not | Why It Exists |
| --- | --- | --- | --- |
| Model | A maintained SQL transformation | An ad hoc notebook experiment | To define durable transformation logic |
| Macro | A reusable SQL building block | A standalone output | To share logic across many models |
| Test | A quality gate on model behavior | A model by itself | To validate trust before downstream use |
| Notebook | An exploration and iteration surface | A guaranteed production artifact | To discover and refine logic quickly |
| Run | A recorded execution of transformation work | The transformation definition itself | To show what executed and what passed or failed |
| Transformation DAG | The graph of builder logic and model dependencies | The asset DAG | To explain how transformation code relates internally |

## Transformation DAG

<figure class="site-mermaid">
  <img src="/_site/diagrams/transformation-dag.svg" alt="Diagram showing source tables flowing through macros into models, tests, notebook promotion, and run outputs." loading="lazy" decoding="async">
</figure>

Read the diagram left to right. Source tables feed the builder workflow. Macros sit near the left because they are reused building blocks that shape many downstream models. Staging and curated models form the main transformation chain. Tests branch off the curated model because they validate whether the result is trustworthy. Notebook promotion points back into the curated model because exploration can graduate into maintained production logic. Runs sit at the end because they record what happened when the framework executed.

## How These Pieces Differ

The table above is the shortest way to keep these objects straight. The most common confusion is between the transformation DAG and the asset DAG. The transformation DAG explains builder logic and model dependencies, while the asset DAG explains operational outputs, freshness, and remediation. The two graphs are related, but they are not the same concept.

## Example In Duck

Imagine a raw trip events table. A builder first creates staging models to normalize source columns, then curated models to compute zone revenue and trip quality metrics. A shared macro encapsulates date bucketing logic so it is not copied into every model. Tests confirm the curated model has the expected keys and no impossible nulls. A notebook is used to experiment with a new calculation, and once the logic stabilizes, that notebook output is promoted into a managed model. When a model run executes, Duck records which steps ran and which tests passed or failed.

## Common Misunderstandings

- A notebook is not the same thing as a maintained model. It is an authoring surface that may later promote into one.
- A run is not the transformation logic; it is the execution record of that logic.
- Macros are not just convenience snippets. They define shared logic and therefore have scope, impact, and lifecycle implications.

## Related Tasks

- [Author Models And Tests](/docs/build/author-models-and-tests)
- [Reuse Macros](/docs/build/reuse-macros)
- [Notebooks And Promotion](/docs/build/notebooks-and-promotion)

## Related Reference

- [Models API](/reference/generated/api/endpoints/models)
- [Declarative Model Kind](/reference/generated/declarative/kinds/model)
