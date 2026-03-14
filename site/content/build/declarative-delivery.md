---
title: Declarative Delivery
description: Manage platform state as code with validate, plan, apply, and reviewable drift control.
---

# Declarative Delivery

Use this guide when your team wants repeatable review, promotion, and drift management across the platform surface.

## Inputs

- a config directory representing desired state
- an authenticated admin or platform automation path
- schema-aware editor support where possible

## Flow

1. model the desired state in declarative files
2. run `duck validate --config-dir <path>`
3. run `duck plan --config-dir <path>`
4. review drift and expected changes
5. run `duck apply --config-dir <path>`
6. rerun `plan` until the environment is clean

## What Belongs Here

- catalogs, schemas, tables, views, and grants
- row filters and column masks
- models, macros, notebooks, assets, semantic models, and data products
- compute routing and storage-related config where supported

## Verification

- validation passes both schema and semantic checks
- plan output matches intended changes
- a follow-up plan returns no additional drift

## Related Reference

- [Declarative Entry Guide](/reference/declarative)
- [CLI Entry Guide](/reference/cli)
