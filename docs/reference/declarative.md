---
title: Advanced Declarative Reference
description: Use the generated declarative schema reference when you need field-level schema detail.
doc_kind: reference
audiences: [ai-agents, builders, admins]
product_areas: [declarative, reference]
surfaces: [declarative, cli, docs]
tasks: [inspect declarative kinds, confirm schema fields, map editor schema support]
prerequisites: [declarative workflow context]
permissions: [documentation access]
cli_commands: [validate, plan, export]
command_groups: [validate, plan, export]
operation_ids: []
api_tags: []
declarative_kinds: ["*"]
related_docs: [core-concepts/declarative, how-to/declarative-workflows, reference/index]
keywords: [declarative reference, json schema, kinds]
last_verified: 2026-03-12
source_of_truth: [docs, schemas/declarative/v1/index.json]
---

# Advanced Declarative Reference

The generated declarative reference documents the versioned JSON Schema artifacts that define supported document shapes. Treat it as the field-level companion to the declarative workflow.

## Best Entry Points

- [Generated Declarative Reference](/reference/generated/declarative/index)
- [Work Declaratively](/how-to/declarative-workflows)

## What the Generated Pages Are Best For

- checking supported kinds
- confirming field names and types
- understanding per-kind schema files
- wiring editor schema mappings

## Editor Integration

Map the union schema:

`schemas/declarative/v1/duck.declarative.schema.json`

to your config file patterns in your editor.

## When To Use Product Guides First

Use product guides first when you need:

- a safe rollout workflow
- an explanation of schema vs semantic validation
- examples of expected CLI behavior and exit codes

## Next Steps

- [Generated Declarative Reference](/reference/generated/declarative/index)
- [Declarative Workflows](/core-concepts/declarative)
