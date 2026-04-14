-- name: CreateSemanticModel :one
INSERT INTO semantic_models (
    id, workspace_id, name, description, owner, base_relation_ref,
    default_time_dimension, tags, created_by
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
RETURNING *;

-- name: GetSemanticModelByID :one
SELECT * FROM semantic_models WHERE id = ?;

-- name: GetSemanticModelByWorkspaceAndName :one
SELECT * FROM semantic_models WHERE workspace_id = ? AND name = ?;

-- name: ListSemanticModelsByWorkspace :many
SELECT * FROM semantic_models
WHERE workspace_id = ?
ORDER BY name
LIMIT ? OFFSET ?;

-- name: CountSemanticModelsByWorkspace :one
SELECT COUNT(*) FROM semantic_models
WHERE workspace_id = ?;

-- name: ListAllSemanticModelsByWorkspace :many
SELECT * FROM semantic_models
WHERE workspace_id = ?
ORDER BY name;

-- name: ListAllSemanticModels :many
SELECT * FROM semantic_models
ORDER BY workspace_id, name;

-- name: UpdateSemanticModel :exec
UPDATE semantic_models
SET description = COALESCE(?, description),
    owner = COALESCE(?, owner),
    base_relation_ref = COALESCE(?, base_relation_ref),
    default_time_dimension = COALESCE(?, default_time_dimension),
    tags = COALESCE(?, tags),
    updated_at = datetime('now')
WHERE id = ?;

-- name: DeleteSemanticModel :exec
DELETE FROM semantic_models WHERE id = ?;
