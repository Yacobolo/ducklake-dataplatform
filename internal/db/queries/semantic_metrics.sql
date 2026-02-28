-- name: CreateSemanticMetric :one
INSERT INTO semantic_metrics (
    id, semantic_model_id, name, description, metric_type, expression_mode,
    expression, default_time_grain, format, owner, certification_state, created_by
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
RETURNING *;

-- name: GetSemanticMetricByID :one
SELECT * FROM semantic_metrics WHERE id = ?;

-- name: GetSemanticMetricByName :one
SELECT * FROM semantic_metrics WHERE semantic_model_id = ? AND name = ?;

-- name: ListSemanticMetricsByModel :many
SELECT * FROM semantic_metrics
WHERE semantic_model_id = ?
ORDER BY name;

-- name: UpdateSemanticMetric :exec
UPDATE semantic_metrics
SET description = COALESCE(?, description),
    metric_type = COALESCE(?, metric_type),
    expression_mode = COALESCE(?, expression_mode),
    expression = COALESCE(?, expression),
    default_time_grain = COALESCE(?, default_time_grain),
    format = COALESCE(?, format),
    owner = COALESCE(?, owner),
    certification_state = COALESCE(?, certification_state),
    updated_at = datetime('now')
WHERE id = ?;

-- name: DeleteSemanticMetric :exec
DELETE FROM semantic_metrics WHERE id = ?;
