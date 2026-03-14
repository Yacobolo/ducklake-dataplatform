---
title: Reuse Macros
description: Encapsulate repeated SQL logic into shared macros with explicit scope and lifecycle.
---

# Reuse Macros

Macros keep repeated SQL behavior consistent across models and teams.

## Inputs

- repeated SQL patterns
- a clear scope decision: project, catalog-global, or system
- owner and lifecycle expectations

## Flow

1. identify repeated transformation logic worth extracting
2. create the macro with a clear name and parameter contract
3. choose visibility carefully so reuse matches ownership
4. update models to call the macro rather than copy the SQL
5. review impact before changing widely shared macros

## What Good Looks Like

- common logic has one authoritative definition
- project-specific behavior does not leak into global scope
- builders can explain macro impact before a change lands

## Related Concepts

- [Transformation Framework](/docs/concepts/transformation-framework)

## Related Reference

- [Models API](/reference/generated/api/endpoints/models)
- [Declarative Macro Kind](/reference/generated/declarative/kinds/macro)
