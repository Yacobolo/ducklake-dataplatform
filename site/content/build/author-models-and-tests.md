---
title: Author Models And Tests
description: Turn sources into trusted transformations with reusable SQL and explicit quality gates.
---

# Author Models And Tests

Use this guide when you want to move from raw or staging data toward durable analytical outputs.

## Inputs

- a trusted source table or view
- a project naming convention
- SQL logic for the intended transformation
- test expectations for the output

## Flow

1. create a model with clear materialization intent
2. keep business logic in the model, not in downstream ad hoc queries
3. add tests that prove uniqueness, not-null behavior, or relationship assumptions
4. run the model and review the DAG and test results
5. promote only after the output is stable enough for reuse

## Visual Model

<figure class="my-8 overflow-x-auto rounded-[1.5rem] border border-[var(--borderColor-default)] bg-[var(--bgColor-inset)] p-5">
  <img class="mx-auto block h-auto w-max max-w-none rounded-none border-0 bg-transparent" src="/_site/diagrams/transformation-dag.svg" alt="Diagram showing source tables passing through macros into staged and curated models with tests and run results." loading="lazy" decoding="async">
</figure>

## Verification

- the model runs successfully
- tests fail for real data quality issues, not for ambiguity in the contract
- the output grain and purpose are documented for downstream users

## Related Reference

- [Models API](/reference/generated/api/endpoints/models)
