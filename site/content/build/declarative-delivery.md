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

## Strict Guarantees

- `duck plan` returns `0` only when the environment already matches the config
- `duck plan` returns `2` when drift is actionable and can be applied
- `duck plan` returns `1` when blocking declarative errors exist
- `duck apply` fails before making changes when blocking plan errors exist
- declarative reads fail closed by default; missing supported endpoints do not silently disappear from state
- the acceptance bar is `validate -> plan -> apply -> plan clean`

## What Belongs Here

- catalogs, schemas, tables, views, and grants
- row filters and column masks
- models, macros, notebooks, assets, semantic models, and data products
- compute routing and storage-related config where supported
- Git-backed notebook projects where `notebooks/*.yaml` is the reviewed source of truth

## Verification

- validation passes both schema and semantic checks
- plan output matches intended changes
- a follow-up plan returns no additional drift
- Git sync leaves linked notebooks aligned with the declarative files under the configured repo path

## Known Blocking Cases

- destructive table column type changes stay as blocking plan errors until an online migration path exists
- if a drift item cannot converge safely, the CLI reports it as blocking rather than pretending the environment is clean

## Related Reference

- [Declarative Entry Guide](/reference/declarative)
- [CLI Entry Guide](/reference/cli)
- [Notebooks And Promotion](/docs/build/notebooks-and-promotion)
