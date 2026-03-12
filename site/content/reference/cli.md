---
title: Advanced CLI Reference
description: Use the CLI when your organization exposes command-line workflows and you need command-level detail.
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
