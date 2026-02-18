-- name: CreateSemanticPreAggregation :one
INSERT INTO semantic_pre_aggregations (
    id, semantic_model_id, name, metric_set, dimension_set,
    grain, target_relation, refresh_policy, created_by
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
RETURNING *;

-- name: GetSemanticPreAggregationByID :one
SELECT * FROM semantic_pre_aggregations WHERE id = ?;

-- name: GetSemanticPreAggregationByName :one
SELECT * FROM semantic_pre_aggregations WHERE semantic_model_id = ? AND name = ?;

-- name: ListSemanticPreAggregationsByModel :many
SELECT * FROM semantic_pre_aggregations
WHERE semantic_model_id = ?
ORDER BY name;

-- name: UpdateSemanticPreAggregation :exec
UPDATE semantic_pre_aggregations
SET metric_set = COALESCE(?, metric_set),
    dimension_set = COALESCE(?, dimension_set),
    grain = COALESCE(?, grain),
    target_relation = COALESCE(?, target_relation),
    refresh_policy = COALESCE(?, refresh_policy),
    updated_at = datetime('now')
WHERE id = ?;

-- name: DeleteSemanticPreAggregation :exec
DELETE FROM semantic_pre_aggregations WHERE id = ?;
