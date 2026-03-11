---
title: Declarative Workflows
description: Understand the declarative model, schema validation, and desired-state workflows.
doc_kind: concept
audiences: [ai-agents, builders, admins]
product_areas: [declarative, governance, catalogs]
surfaces: [cli, declarative]
tasks: [understand declarative model, distinguish validation types, plan declarative workflows]
prerequisites: [basic platform familiarity]
permissions: [documentation access]
cli_commands: [validate, plan, apply]
command_groups: [validate, plan, apply]
operation_ids: []
api_tags: []
declarative_kinds: ["*"]
related_docs: [how-to/declarative-workflows, reference/declarative, how-to/catalog-and-ingestion]
keywords: [declarative model, schema validation, semantic validation]
last_verified: 2026-03-12
source_of_truth: [docs, schemas/declarative/v1/index.json]
---

# Declarative Workflows

Duck can manage catalogs, security, governance, models, and related resources as YAML documents. The declarative layer gives teams a repeatable path for review, promotion, and drift detection.

## What the Declarative Layer Does

- defines desired state as versioned config files
- validates document structure against generated JSON Schema
- validates cross-resource semantics through the CLI
- supports `validate`, `plan`, and `apply` workflows

## Schema vs Semantic Validation

- JSON Schema checks document shape and field-level rules
- CLI validation checks references, conflicts, privilege rules, and other platform semantics

Both are necessary. Passing schema validation alone does not mean a config is safe to apply.

## Practical Workflow

1. edit config files in a dedicated directory
2. run `duck validate --config-dir <path>`
3. run `duck plan --config-dir <path>`
4. review drift
5. run `duck apply --config-dir <path>`

## Next Steps

- [Work Declaratively](/how-to/declarative-workflows)
- [Advanced Declarative Reference](/reference/declarative)

## Related Reference

- [Glossary](/reference/glossary)
- [Advanced Reference](/reference/)
