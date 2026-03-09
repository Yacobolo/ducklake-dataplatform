---
title: Query and Compute
description: Understand how query execution, routing, and worker boundaries fit together for platform users.
---

# Query and Compute

Queries enter through the control plane and are executed as the authenticated principal. Enforcement remains centralized even when compute is delegated to remote workers.

## Query Flow

1. the request is authenticated
2. grants and policy context are resolved
3. SQL is executed with RBAC, row filters, and column masking applied
4. results are returned through the control-plane interface

## Local vs Remote Execution

- Local execution is the default for standard platform usage and simple deployments.
- Remote compute endpoints add worker isolation and lifecycle-based execution.
- Fallback policies can keep the system available while rolling out remote routing.

## Why Users and Admins Should Care

Compute routing changes performance and failure boundaries, but it should not change who can see what. Treat policy enforcement as a control-plane responsibility and worker health as an execution concern.

## Next Steps

- [Quickstart](/start-here/quickstart)
- [Distributed Compute](/operations/distributed-compute)

## Related Reference

- [Advanced API Reference](/reference/api)
- [Glossary](/reference/glossary)
