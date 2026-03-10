---
title: Browser-Local Compute Runbook
description: Operational guidance for the browser-local DuckDB WASM execution path.
---

# Browser-Local Compute Runbook

This runbook describes the current browser-local DuckDB WASM execution model and the limits that matter for rollout and support.

## Product Model

- `BYOC_LOCAL` is a product mode, not a single runtime.
- CLI local compute uses native DuckDB plus `duck_access`.
- Browser local compute uses DuckDB WASM with the same manifest-driven policy contract.
- `SHARED_ENDPOINT` remains the managed path for heavier interactive work and unattended execution.
- `AUTO` prefers browser-local execution only when the runtime and query satisfy guardrails; otherwise it falls through to managed compute.

## Current UI Scope

- SQL editor:
  - local browser execution supported
  - `AUTO` can resolve to local or managed compute
- Notebook SQL cells:
  - single interactive cell runs can resolve locally
  - notebook `Run all` and async notebook runs remain managed-only

## Browser Runtime Guardrails

Browser-local execution currently requires:

- read-only `SELECT` or `WITH` SQL
- explicit `LIMIT`
- manifest-backed base tables only
- HTTPS presigned file URLs
- a compatible browser runtime contract version
- `WEB_SESSION` manifest auth support

The current bounded local subset also enforces:

- up to 4 referenced manifest-backed base tables
- no DDL/DML statements
- browser guidance row limit from the manifest contract
- browser guidance memory limit from the manifest contract

If these checks fail:

- `BYOC_LOCAL` fails explicitly in the UI
- `AUTO` falls through explicitly to managed compute

## Cancellation and Reset

- Local cancel currently works by resetting the DuckDB WASM runtime.
- This is a runtime-level stop, not a fine-grained query interrupt.
- If local execution appears stuck or enters a bad state, use the runtime reset control and rerun.

## Support Expectations

Operators should expect:

- some browsers or environments to fail WASM initialization
- large result sets to require managed compute
- presigned object-store URLs to require valid CORS behavior

Recommended user guidance:

- use local mode for interactive exploration and smaller bounded queries
- use shared endpoints for large joins, long-running work, and background execution
- use `AUTO` when the user wants policy-driven routing with explicit fallback behavior

## Validation Checklist

Before rollout or merge, confirm:

- SQL editor local execution works on a supported browser
- SQL editor `AUTO` resolves locally for an eligible query
- SQL editor `AUTO` falls through to managed compute for an ineligible query
- notebook SQL cell local execution works on a supported browser
- notebook `Run all` rejects local mode and stays managed-only
- manifest endpoint returns JSON errors for browser clients

## Remaining Future Hardening

- true query-scoped interrupt for WASM execution
- committed browser E2E coverage in the repo
- broader browser support matrix and operator troubleshooting docs
