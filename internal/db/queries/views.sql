-- name: CreateView :one
INSERT INTO views (schema_id, name, view_definition, comment, properties, owner, source_tables)
VALUES (?, ?, ?, ?, ?, ?, ?) RETURNING *;

-- name: GetViewByName :one
SELECT * FROM views WHERE schema_id = ? AND name = ?;

-- name: ListViews :many
SELECT * FROM views WHERE schema_id = ? ORDER BY name LIMIT ? OFFSET ?;

-- name: CountViews :one
SELECT COUNT(*) as cnt FROM views WHERE schema_id = ?;

-- name: DeleteView :exec
DELETE FROM views WHERE schema_id = ? AND name = ?;
