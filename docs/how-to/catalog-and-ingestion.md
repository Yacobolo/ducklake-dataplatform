---
title: Load Data and Build Assets
description: Bring data into Duck and build tables, models, views, and other reusable assets.
---

# Load Data and Build Assets

This flow is for teams moving from raw data access toward reusable, governed data products.

## Inputs

- a Duck environment
- storage or source data available to load
- a builder or admin path with object-management permissions

## Typical Lifecycle

1. define or choose the target catalog and schema
2. register the objects you want users to see
3. load or ingest source data
4. build downstream tables, views, models, notebooks, or assets
5. apply governance controls before broad access

## Expected Result

At the end of this flow:

- the target objects exist in the intended catalog and schema
- source data is ingested or registered
- downstream consumers can discover the governed outputs they are allowed to use

## What This Covers

- catalog and schema structure
- ingestion into managed tables
- creation of views, models, assets, notebooks, and pipelines
- governance before broad consumption

## Next Steps

- [Query and Explore Data](/how-to/use-the-cli)
- [Govern & Administer](/operations/)

## Related Reference

- [Advanced API Reference](/reference/api)
- [Build Data Products](/core-concepts/)
