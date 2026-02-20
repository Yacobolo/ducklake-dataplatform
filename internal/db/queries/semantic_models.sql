-- name: CreateSemanticModel :one
INSERT INTO semantic_models (
    id, project_name, name, description, owner, base_model_ref,
    default_time_dimension, tags, created_by
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
RETURNING *;

-- name: GetSemanticModelByID :one
SELECT * FROM semantic_models WHERE id = ?;

-- name: GetSemanticModelByName :one
SELECT * FROM semantic_models WHERE project_name = ? AND name = ?;

-- name: ListSemanticModels :many
SELECT * FROM semantic_models
WHERE (? = '' OR project_name = ?)
ORDER BY project_name, name
LIMIT ? OFFSET ?;

-- name: CountSemanticModels :one
SELECT COUNT(*) FROM semantic_models
WHERE (? = '' OR project_name = ?);

-- name: ListAllSemanticModels :many
SELECT * FROM semantic_models ORDER BY project_name, name;

-- name: UpdateSemanticModel :exec
UPDATE semantic_models
SET description = COALESCE(?, description),
    owner = COALESCE(?, owner),
    base_model_ref = COALESCE(?, base_model_ref),
    default_time_dimension = COALESCE(?, default_time_dimension),
    tags = COALESCE(?, tags),
    updated_at = datetime('now')
WHERE id = ?;

-- name: DeleteSemanticModel :exec
DELETE FROM semantic_models WHERE id = ?;
