---
title: Transformation Framework
description: Understand models, macros, tests, notebooks, runs, DAGs, and promotion workflows.
---

# Transformation Framework

Duck includes a builder workflow for turning raw inputs into trusted outputs through repeatable SQL-based development.

## Why It Matters

Teams need more than one-off queries. They need reusable logic, shared macros, test execution, notebook exploration, and a promotion path into durable outputs.

## Mental Model

The transformation framework combines:

- models as versioned SQL transformations
- macros as reusable SQL building blocks
- tests as quality gates
- notebook cells for exploration and promotion
- runs and DAGs for execution history and dependency visibility

## Transformation DAG

<figure class="site-mermaid">
  <img src="/_site/diagrams/transformation-dag.svg" alt="Diagram showing source tables flowing through macros into models, tests, notebook promotion, and run outputs." loading="lazy" decoding="async">
</figure>

## Key Objects

- models and model runs
- macros, revisions, and impact
- model tests and test results
- notebooks, cells, sessions, and jobs
- notebook-to-model promotion and model DAGs

## Related Tasks

- [Author Models And Tests](/build/author-models-and-tests)
- [Reuse Macros](/build/reuse-macros)
- [Notebooks And Promotion](/build/notebooks-and-promotion)

## Related Reference

- [Models API](/reference/generated/api/endpoints/models)
- [Declarative Model Kind](/reference/generated/declarative/kinds/model)
