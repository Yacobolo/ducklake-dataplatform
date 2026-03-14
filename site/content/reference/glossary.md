---
title: Glossary
description: Shared terminology for the platform, data-product model, orchestration layer, semantic layer, and operations guides.
---

# Glossary

## API key

A credential presented in `X-API-Key` for authenticated API or CLI access.

## Asset

A durable operational output with dependencies, checks, freshness, and materialization behavior.

## Asset backfill

A request to reprocess a selected historical slice of an asset.

## Catalog

The top-level container for schemas and related database objects.

## Column mask

A policy that rewrites or obfuscates a column value for selected principals or groups.

## Data product

A governed package of outputs, semantic entrypoints, ownership, contract, and release metadata.

## Declarative config

YAML documents that define desired platform state and are applied through validate-plan-apply workflows.

## Domain

A business ownership boundary used to group data products and teams.

## Grant

A privilege assignment on a securable object.

## Group

A collection of principals used to manage access in bulk.

## Lineage

The provenance graph that explains upstream, downstream, and column-level derivation relationships.

## Macro

Reusable SQL logic that can be shared across models and projects.

## Metric

A named semantic definition for a business-facing measurement.

## Notebook

A document of executable SQL cells that can be explored interactively and promoted into durable outputs.

## Principal

A user or service identity recognized by the platform.

## Product contract

The published description of what a data product provides, who it serves, and how it should be used.

## Row filter

A policy that restricts which rows are visible during query execution.

## Semantic model

A reusable business-facing model that defines metrics, dimensions, relationships, and optional pre-aggregations.

## Secure query path

The end-to-end request path where authentication, grants, row filters, and column masks are applied before results are returned.

## Semantic validation

Cross-resource validation performed by the CLI after basic schema validation passes.

## Team

An ownership group responsible for building or supporting products within a domain.
