---
title: Identity and Authentication
description: Use this feature guide for sign-in, principals, groups, and API key workflows.
doc_kind: overview
audiences: [ai-agents, admins, builders]
product_areas: [auth, identity]
surfaces: [api, cli, browser, declarative]
tasks: [authenticate users, manage principals, manage groups, manage api keys]
prerequisites: [deployment URL, admin or platform user identity]
permissions: [identity administration or approved user access]
cli_commands: [auth login, auth whoami, security groups create]
command_groups: [auth, security]
operation_ids: [localLogin, listPrincipals, listGroups, createAPIKey]
api_tags: [Auth, Identity]
declarative_kinds: [api-key-list, principal-list, group-list]
related_docs: [how-to/authentication, how-to/access-control, reference/api]
keywords: [identity, authentication, groups, principals, api keys]
last_verified: 2026-03-12
source_of_truth: [docs, api/gen/openapi.yaml, schemas/declarative/v1/index.json]
---

# Identity and Authentication

## When to use

Use this guide when the workflow starts with who can sign in, which identity is acting, or how a human or service proves access.

## Primary tasks

- authenticate a user through browser sign-in or bearer token flow
- issue and rotate API keys for approved automation
- create principals and groups for shared access
- connect access-management guides to the exact API and CLI surfaces

## Exact entry points

- Start with [Access the Platform](/how-to/authentication) for auth-path selection.
- Use [Manage Access](/how-to/access-control) when identity changes must be paired with grants or policy controls.
- Use [Advanced API Reference](/reference/api) for exact `Auth` and `Identity` operation detail.

## Generated reference

- [Auth endpoints](/reference/generated/api/endpoints/auth)
- [Identity endpoints](/reference/generated/api/endpoints/identity)
- [Generated API reference](/reference/generated/api/index)
- [Generated declarative reference](/reference/generated/declarative/index)
