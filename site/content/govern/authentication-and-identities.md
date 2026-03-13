---
title: Authentication And Identities
description: Choose how people and services authenticate and how principals and groups are represented in Duck.
---

# Authentication And Identities

Use this guide when you are setting the foundation for access control.

## Inputs

- your identity provider choice
- service-auth requirements
- an owner for principal and group lifecycle

## Flow

1. choose the primary auth path for people
2. choose the approved auth path for automation
3. define how principals and groups are created and maintained
4. avoid broad shared credentials except for explicit service use cases

## What Good Looks Like

- people authenticate through the organization’s intended identity path
- service identities are scoped narrowly
- groups represent real access domains, not one-off convenience buckets

## Related Reference

- [Auth API](/reference/generated/api/endpoints/auth)
- [Identity API](/reference/generated/api/endpoints/identity)
