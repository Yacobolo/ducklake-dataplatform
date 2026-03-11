---
title: Work Declaratively
description: Use validate-plan-apply workflows to manage platform state as code.
doc_kind: task
audiences: [ai-agents, builders, admins]
product_areas: [declarative, governance, catalogs]
surfaces: [cli, declarative]
tasks: [validate config, plan changes, apply desired state, detect drift]
prerequisites: [declarative config files, cli access, target environment]
permissions: [apply privileges for targeted resources]
cli_commands: [validate, plan, apply, export]
command_groups: [validate, plan, apply, export]
operation_ids: []
api_tags: []
declarative_kinds: ["*"]
related_docs: [core-concepts/declarative, reference/declarative, how-to/catalog-and-ingestion]
keywords: [desired state, plan apply, drift detection]
last_verified: 2026-03-12
source_of_truth: [docs, schemas/declarative/v1/index.json]
---

# Work Declaratively

This is the standard path for teams that manage platform state as code instead of making one-off manual changes.

## Inputs

- a config directory representing desired platform state
- an authenticated admin path
- a CLI or automation path approved for your environment

## Workflow

### 1. Validate desired state

```bash
duck validate --config-dir <path>
```

Expected result: validation succeeds with no schema or semantic errors.

### 2. Plan changes

```bash
duck plan --config-dir <path>
```

Expected result:

- exit code `0` when no changes are required
- exit code `2` when drift is detected and changes are ready to apply

### 3. Apply changes

```bash
duck apply --config-dir <path>
```

Expected result: the desired state is applied successfully.

### 4. Re-run plan

```bash
duck plan --config-dir <path>
```

Expected result: no further declarative changes are required.

## Editor Integration

Use the generated JSON Schema artifacts for editor completion and early feedback. See [Advanced Declarative Reference](/reference/declarative) for the schema mapping path.

## Troubleshooting

- schema validation passes, semantic validation fails: check cross-resource references and privilege combinations
- `plan` returns `2`: treat it as drift detected, not a crash
- apply errors after a long drift period: compare the live environment with the config history before retrying

## Next Steps

- [Load Data and Build Assets](/how-to/catalog-and-ingestion)
- [Declarative Workflows](/core-concepts/declarative)

## Related Reference

- [Advanced Declarative Reference](/reference/declarative)
- [Glossary](/reference/glossary)
