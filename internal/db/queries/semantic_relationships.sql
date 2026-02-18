-- name: CreateSemanticRelationship :one
INSERT INTO semantic_relationships (
    id, name, from_semantic_id, to_semantic_id, relationship_type,
    join_sql, is_default, cost, max_hops, created_by
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
RETURNING *;

-- name: GetSemanticRelationshipByID :one
SELECT * FROM semantic_relationships WHERE id = ?;

-- name: GetSemanticRelationshipByName :one
SELECT * FROM semantic_relationships WHERE name = ?;

-- name: ListSemanticRelationships :many
SELECT * FROM semantic_relationships
ORDER BY name
LIMIT ? OFFSET ?;

-- name: CountSemanticRelationships :one
SELECT COUNT(*) FROM semantic_relationships;

-- name: UpdateSemanticRelationship :exec
UPDATE semantic_relationships
SET relationship_type = COALESCE(?, relationship_type),
    join_sql = COALESCE(?, join_sql),
    is_default = COALESCE(?, is_default),
    cost = COALESCE(?, cost),
    max_hops = COALESCE(?, max_hops),
    updated_at = datetime('now')
WHERE id = ?;

-- name: DeleteSemanticRelationship :exec
DELETE FROM semantic_relationships WHERE id = ?;
