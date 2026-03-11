---
title: Storage and Integrations
description: Use this feature guide for storage credentials, external locations, and external system integrations.
doc_kind: overview
audiences: [ai-agents, admins, builders]
product_areas: [storage, integrations]
surfaces: [api, cli, declarative]
tasks: [configure storage access, register external locations, connect repositories and integrations]
prerequisites: [cloud storage details, admin path]
permissions: [storage administration, integration administration]
cli_commands: [storage credentials create, storage external-locations create, apply]
command_groups: [storage, apply]
operation_ids: [createStorageCredential, createExternalLocation, createGitRepo]
api_tags: [Storage, Integrations]
declarative_kinds: [storage-credential-list, external-location-list]
related_docs: [how-to/catalog-and-ingestion, operations/configuration, reference/declarative]
keywords: [storage credentials, external locations, integrations, git repos]
last_verified: 2026-03-12
source_of_truth: [docs, api/gen/openapi.yaml, schemas/declarative/v1/index.json]
---

# Storage and Integrations

## When to use

Use this guide when workflows depend on object storage, external locations, or external system connectivity that must be governed centrally.

## Primary tasks

- create and rotate storage credentials
- register external locations used by ingestion or managed assets
- connect Git or other integration surfaces to the control plane

## Exact entry points

- Start with [Load Data and Build Assets](/how-to/catalog-and-ingestion) when storage setup is part of an ingestion workflow.
- Use [Platform Settings](/operations/configuration) when integration setup changes deployment posture.
- Use [Advanced Declarative Reference](/reference/declarative) for field-level schema details.

## Generated reference

- [Storage endpoints](/reference/generated/api/endpoints/storage)
- [Integrations endpoints](/reference/generated/api/endpoints/integrations)
