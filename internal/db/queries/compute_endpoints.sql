-- name: CreateComputeEndpoint :one
INSERT INTO compute_endpoints (
    external_id, name, url, type, status, size, max_memory_gb, auth_token, owner
) VALUES (?, ?, ?, ?, 'INACTIVE', ?, ?, ?, ?)
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
SET url = COALESCE(?, url),
    size = COALESCE(?, size),
    max_memory_gb = COALESCE(?, max_memory_gb),
    auth_token = COALESCE(?, auth_token),
    updated_at = datetime('now')
WHERE id = ?;

-- name: UpdateComputeEndpointStatus :exec
UPDATE compute_endpoints
SET status = ?,
    updated_at = datetime('now')
WHERE id = ?;

-- name: DeleteComputeEndpoint :exec
DELETE FROM compute_endpoints WHERE id = ?;

-- name: CreateComputeAssignment :one
INSERT INTO compute_assignments (
    principal_id, principal_type, endpoint_id, is_default, fallback_local
) VALUES (?, ?, ?, ?, ?)
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
