# Distributed Compute Runbook

This runbook describes how to operate remote compute agents with lifecycle-based execution.

## Architecture Boundaries

- Gateway (`cmd/server`) remains the single policy enforcement point (RBAC, RLS, column masking).
- Workers (`cmd/compute-agent`) only execute already rewritten SQL.
- Gateway-to-worker transport is currently HTTP/JSON; proto contracts are staged in `internal/compute/proto/compute_worker.proto`.

## Agent Endpoints

- `POST /execute` compatibility path for synchronous execution.
- `POST /queries` submit an async query.
- `GET /queries/{queryID}` inspect lifecycle state.
- `GET /queries/{queryID}/results` fetch paged results.
- `POST /queries/{queryID}/cancel` request cancellation.
- `DELETE /queries/{queryID}` delete query state.
- `GET /health` scrape readiness and load signals.

## Agent Environment

- `AGENT_TOKEN` (required): shared auth token for gateway calls.
- `LISTEN_ADDR` (default `:9443`): HTTP listen address.
- `MAX_MEMORY_GB` (optional): DuckDB max memory setting.
- `QUERY_RESULT_TTL` (default `10m`): retention window for completed query results.
- `QUERY_CLEANUP_INTERVAL` (default `1m`): cleanup cadence for expired lifecycle jobs.

## Operational Metrics and SLO Inputs

`GET /health` reports:

- `active_queries`: currently running SQL statements.
- `queued_jobs`: lifecycle jobs accepted but not started.
- `running_jobs`: lifecycle jobs in progress.
- `completed_jobs`: retained terminal jobs.
- `stored_jobs`: total in-memory jobs currently retained.
- `cleaned_jobs`: cumulative expired-job cleanup count.
- `query_result_ttl_seconds`: active result-retention policy.

Recommended initial SLOs:

- Availability: 99.9% successful `POST /queries` responses.
- Queue latency: p95 time in `QUEUED` under 1s.
- Completion latency: p95 status transition to terminal under workload target.
- Cleanup health: `stored_jobs` should not grow unbounded for steady traffic.

## Rollout Strategy

1. Start with mixed mode (local fallback enabled on assignments).
2. Route a small set of users/groups to remote endpoints.
3. Observe health metrics and completion latency under representative load.
4. Gradually widen assignment scope and tighten fallback policy where needed.

## Failure and Recovery

- If worker health degrades, resolver honors `fallback_local` assignment policy.
- Lifecycle client automatically falls back to legacy `/execute` when lifecycle endpoints are unsupported.
- To reduce memory pressure quickly, lower `QUERY_RESULT_TTL` and/or shorten `QUERY_CLEANUP_INTERVAL`.
