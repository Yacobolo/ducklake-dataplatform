# Distributed Compute Runbook

This runbook describes how to operate remote compute agents with lifecycle-based execution.

## Architecture Boundaries

- Gateway (`cmd/server`) remains the single policy enforcement point (RBAC, RLS, column masking).
- Workers (`cmd/compute-agent`) only execute already rewritten SQL.
- Gateway-to-worker transport supports dual stack: HTTP/JSON and internal gRPC (`duckdemo.compute.v1.ComputeWorker`).

## Agent Endpoints

- Internal query execution/lifecycle APIs are exposed over gRPC (`duckdemo.compute.v1.ComputeWorker`).
- HTTP surface is observability-only:
  - `GET /health` readiness and load signals.
  - `GET /metrics` Prometheus-style operational metrics.

## Agent Environment

- `AGENT_TOKEN` (required): shared auth token for gateway calls.
- `LISTEN_ADDR` (default `:9443`): HTTP listen address.
- `GRPC_LISTEN_ADDR` (default `:9444`): gRPC listen address for internal worker transport.
- `MAX_MEMORY_GB` (optional): DuckDB max memory setting.
- `QUERY_RESULT_TTL` (default `10m`): retention window for completed query results.
- `QUERY_CLEANUP_INTERVAL` (default `1m`): cleanup cadence for expired lifecycle jobs.
- `FEATURE_CURSOR_MODE` (default `true`): kill switch for lifecycle/cursor endpoints.
- `FEATURE_INTERNAL_GRPC` (default `true`): enables internal gRPC worker API.

Gateway controls:

- `FEATURE_REMOTE_ROUTING` (default `true`): kill switch for remote endpoint routing.
- `FEATURE_ASYNC_QUEUE` (default `true`): kill switch for control-plane async query queue APIs.
- `FEATURE_CURSOR_MODE` (default `true`): kill switch for lifecycle/cursor usage in remote executor.
- `FEATURE_INTERNAL_GRPC` (default `true`): enables gRPC transport for remote execution when endpoint URL uses `grpc://` or `grpcs://`.
- `FEATURE_FLIGHT_SQL` (default `true`): enables Flight SQL listener scaffolding; full protocol compatibility is still a rollout-phase item.
- `FEATURE_PG_WIRE` (default `true`): enables PG-wire preview listener (startup + simple query protocol path); full compatibility is still a rollout-phase item.
- `REMOTE_CANARY_USERS` (optional CSV): restrict remote routing rollout to selected principals.
- `FLIGHT_SQL_LISTEN_ADDR` (default `:32010`): bind address for external Flight SQL listener when enabled.
- `PG_WIRE_LISTEN_ADDR` (default `:5433`): bind address for external PG-wire listener when enabled.

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

## Protocol Preview Notes

- Flight SQL preview currently exposes gRPC health only; query RPCs are pending.
- PG-wire preview currently supports startup handshake and simple query messages (`Q`) mapped to control-plane query execution.
- PG-wire preview maps startup `user` parameter to platform principal name; full auth negotiation and extended query flow are pending.
