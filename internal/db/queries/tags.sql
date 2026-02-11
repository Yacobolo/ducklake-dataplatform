-- name: CreateTag :one
INSERT INTO tags (key, value, created_by) VALUES (?, ?, ?) RETURNING *;

-- name: GetTag :one
SELECT * FROM tags WHERE id = ?;

-- name: ListTags :many
SELECT * FROM tags ORDER BY key, value LIMIT ? OFFSET ?;

-- name: CountTags :one
SELECT COUNT(*) as cnt FROM tags;

-- name: DeleteTag :exec
DELETE FROM tags WHERE id = ?;

-- name: CreateTagAssignment :one
INSERT INTO tag_assignments (tag_id, securable_type, securable_id, column_name, assigned_by)
VALUES (?, ?, ?, ?, ?) RETURNING *;

-- name: DeleteTagAssignment :exec
DELETE FROM tag_assignments WHERE id = ?;

-- name: ListTagsForSecurable :many
SELECT t.* FROM tags t
JOIN tag_assignments ta ON t.id = ta.tag_id
WHERE ta.securable_type = ? AND ta.securable_id = ?
  AND (? IS NULL OR ta.column_name = ?)
ORDER BY t.key, t.value;

-- name: ListAssignmentsForTag :many
SELECT * FROM tag_assignments WHERE tag_id = ?;
