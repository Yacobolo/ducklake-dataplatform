---
title: Glossary
description: Shared terminology for the platform, API, declarative config, and operations guides.
doc_kind: reference
audiences: [ai-agents, end-users, builders, admins]
product_areas: [reference, governance, queries, declarative]
surfaces: [docs, api, cli, declarative]
tasks: [resolve platform terminology, normalize concepts for retrieval, disambiguate product language]
prerequisites: [documentation access]
permissions: [documentation access]
cli_commands: [docs show reference/glossary]
command_groups: [docs]
operation_ids: []
api_tags: []
declarative_kinds: []
related_docs: [reference/index, core-concepts/access-control, core-concepts/declarative]
keywords: [terminology, definitions, vocabulary]
last_verified: 2026-03-12
source_of_truth: [docs]
---

# Glossary

## API key

A credential presented in `X-API-Key` for authenticated API or CLI access.

## Catalog

The top-level container for schemas and related database objects.

## Column mask

A policy that rewrites or obfuscates a column value for selected principals or groups.

## Declarative config

YAML documents that define desired platform state and are applied through validate-plan-apply workflows.

## Grant

A privilege assignment on a securable object.

## Group

A collection of principals used to manage access in bulk.

## Principal

A user or service identity recognized by the platform.

## Row filter

A policy that restricts which rows are visible during query execution.

## Secure query path

The end-to-end request path where authentication, grants, row filters, and column masks are applied before results are returned.

## Semantic validation

Cross-resource validation performed by the CLI after basic schema validation passes.
