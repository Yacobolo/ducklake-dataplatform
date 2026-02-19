# Semantic GA Readiness Checklist

This checklist tracks the final production-readiness items for the semantic layer v1 rollout.

## Scope

- Semantic CRUD APIs (models, metrics, relationships, pre-aggregations)
- Semantic query planner and runtime (`/metric-queries:explain`, `/metric-queries:run`)
- Declarative plan/apply/read-state for semantic resources

## Performance Readiness

- Baseline query latency on representative metric workloads:
  - cold planner path (no pre-aggregation)
  - warm planner path (same request repeated)
  - pre-aggregation hit path
- Compare semantic query latency against equivalent raw SQL baseline.
- Capture p50/p95 latency and row-count parity for each scenario.
- Confirm pre-aggregation selection appears in explain payload (`selected_pre_aggregation`).
- Validate no regression in integration tests and `task check` on release branch.

## Security and Correctness Readiness

- Verify semantic `:run` executes through secure query path and preserves RBAC/RLS/masking behavior.
- Validate deterministic join resolution and explicit ambiguity errors.
- Validate SQL-expression guardrails for metric expressions remain enforced.
- Confirm audit/query-history entries are emitted for semantic-driven execution.

## Operability and Rollout

- Feature-flag strategy:
  - default ON in local/dev
  - staged enablement in shared environments
  - explicit rollback switch documented in deployment config
- Canary rollout:
  - onboard one pilot project
  - monitor error rates and latency for semantic endpoints
  - expand to additional projects after stable observation window
- Runbook:
  - endpoint health checks
  - common failure modes (ambiguous metric, missing relationship, stale sources)
  - rollback steps and owner escalation path

## Release Gate

All of the following must be true before marking semantic v1 GA:

- `task check` is green.
- Integration tests cover semantic CRUD + explain + run + declarative lifecycle.
- Performance baseline and canary observations are recorded in the PR/release notes.
- Rollback and oncall runbook entries are published.
