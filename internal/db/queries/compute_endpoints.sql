-- name: CreateComputeEndpoint :one
INSERT INTO compute_endpoints (
    id, external_id, name, url, type, status, selection_policy, workload_class, readiness_status,
    size, max_memory_gb, max_concurrency, max_result_size_mb, recommended_for_large_queries,
    is_draining, auth_token, owner
) VALUES (?, ?, ?, ?, ?, 'INACTIVE', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
RETURNING *;

-- name: GetComputeEndpoint :one
SELECT * FROM compute_endpoints WHERE id = ?;

-- name: GetComputeEndpointByName :one
SELECT * FROM compute_endpoints WHERE name = ?;

-- name: ListComputeEndpoints :many
SELECT * FROM compute_endpoints ORDER BY name LIMIT ? OFFSET ?;

-- name: CountComputeEndpoints :one
SELECT COUNT(*) FROM compute_endpoints;

-- name: UpdateComputeEndpoint :exec
UPDATE compute_endpoints
SET url = ?,
    size = ?,
    max_memory_gb = ?,
    max_concurrency = ?,
    max_result_size_mb = ?,
    selection_policy = ?,
    workload_class = ?,
    readiness_status = ?,
    recommended_for_large_queries = ?,
    is_draining = ?,
    auth_token = ?,
    updated_at = datetime('now')
WHERE id = ?;

-- name: UpdateComputeEndpointStatus :exec
UPDATE compute_endpoints
SET status = ?,
    updated_at = datetime('now')
WHERE id = ?;

-- name: UpdateComputeEndpointHealth :exec
UPDATE compute_endpoints
SET last_health_status = ?,
    last_health_checked_at = datetime('now'),
    active_queries = ?,
    queued_jobs = ?,
    running_jobs = ?,
    completed_jobs = ?,
    stored_jobs = ?,
    cleaned_jobs = ?,
    query_result_ttl_seconds = ?,
    readiness_status = ?,
    updated_at = datetime('now')
WHERE id = ?;

-- name: DeleteComputeEndpoint :exec
DELETE FROM compute_endpoints WHERE id = ?;

-- name: CreateComputeAssignment :one
INSERT INTO compute_assignments (
    id, principal_id, principal_type, endpoint_id, is_default, fallback_local
) VALUES (?, ?, ?, ?, ?, ?)
RETURNING *;

-- name: DeleteComputeAssignment :exec
DELETE FROM compute_assignments WHERE id = ?;

-- name: ListAssignmentsForEndpoint :many
SELECT * FROM compute_assignments WHERE endpoint_id = ? ORDER BY id LIMIT ? OFFSET ?;

-- name: CountAssignmentsForEndpoint :one
SELECT COUNT(*) FROM compute_assignments WHERE endpoint_id = ?;

-- name: GetDefaultEndpointForPrincipal :one
SELECT ce.*
FROM compute_endpoints ce
JOIN compute_assignments ca ON ca.endpoint_id = ce.id
WHERE ca.principal_id = ?
  AND ca.principal_type = ?
  AND ca.is_default = 1
  AND ce.status = 'ACTIVE'
LIMIT 1;

-- name: GetAssignmentsForPrincipal :many
SELECT ce.*
FROM compute_endpoints ce
JOIN compute_assignments ca ON ca.endpoint_id = ce.id
WHERE ca.principal_id = ?
  AND ca.principal_type = ?
ORDER BY ca.is_default DESC, ce.name;

-- name: ResolveEndpointForPrincipalByName :one
SELECT ce.*
FROM compute_endpoints ce
JOIN compute_assignments ca ON ca.endpoint_id = ce.id
JOIN principals p ON p.id = ca.principal_id AND ca.principal_type = 'user'
WHERE p.name = ?
  AND ca.is_default = 1
  AND ce.status = 'ACTIVE'
LIMIT 1;
