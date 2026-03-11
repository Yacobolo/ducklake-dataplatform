---
title: Advanced CLI Reference
description: Use the CLI when your organization exposes command-line workflows and you need command-level detail.
doc_kind: reference
audiences: [ai-agents, builders, admins]
product_areas: [cli, reference]
surfaces: [cli, docs]
tasks: [inspect command surfaces, find command groups, script workflows]
prerequisites: [cli availability, target environment]
permissions: [cli access]
cli_commands: [commands, docs search, api search, find tables]
command_groups: ["*"]
operation_ids: []
api_tags: []
declarative_kinds: []
related_docs: [how-to/use-the-cli, reference/index, reference/feature-models-and-pipelines]
keywords: [cli reference, command groups, scripting]
last_verified: 2026-03-12
source_of_truth: [docs]
---

# Advanced CLI Reference

The CLI is an advanced access surface for users and admins who need scripted workflows, discovery, or declarative change management.

## Core Command Families

| Area | Commands to start with |
| --- | --- |
| Declarative workflows | `validate`, `plan`, `apply`, `export` |
| Auth and profiles | `auth`, `config` |
| Discovery | `commands`, `api`, `find`, `describe` |
| Generated resource management | command families under `catalog`, `security`, `models`, `storage`, and others |

## High-Value Commands

```bash
./bin/duck version
./bin/duck commands --filter query
./bin/duck api search "grant"
./bin/duck config show
./bin/duck find tables movie
```

## Output and Auth Defaults

- `--output` supports machine-friendly and human-friendly modes
- host, token, and API key can be resolved from flags, environment, or profiles
- profile-based setup is useful for shared or multi-environment work

## Best Use Cases

- declarative validation and apply workflows
- discovery of command surfaces
- scripting against API-backed operations without hand-writing HTTP

## Next Steps

- [Query and Explore Data](/how-to/use-the-cli)
- [Generated API Reference](/reference/generated/api/index)
