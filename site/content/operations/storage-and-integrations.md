---
title: Storage And Integrations
description: Operate storage credentials, external locations, and integration lifecycles without losing control of ownership or exposure.
---

# Storage And Integrations

Use this guide when Duck needs to reach external storage or participate in a wider platform workflow such as Git-backed sync.

## Inputs

- the storage system or integration you want to enable
- the owner for that integration
- the environment boundary where it is allowed

## Flow

1. define the storage credential or integration owner
2. create external locations and path boundaries deliberately
3. keep credential scope narrow and environment-specific
4. document who can mutate the configuration and who can consume it
5. monitor failures separately from query-policy failures

## What Good Looks Like

- storage paths are explicit
- secrets do not leak into source control
- operators can distinguish storage errors from governance denials
- integration failures have an owner and an alert path

## Related Reference

- [Storage API](/reference/generated/api/endpoints/storage)
- [Integrations API](/reference/generated/api/endpoints/integrations)
