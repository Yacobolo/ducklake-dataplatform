-- name: CreateView :one
INSERT INTO views (id, schema_id, name, view_definition, comment, properties, owner, source_tables)
VALUES (?, ?, ?, ?, ?, ?, ?, ?) RETURNING *;

-- name: GetViewByName :one
SELECT * FROM views WHERE schema_id = ? AND name = ? AND deleted_at IS NULL;

-- name: ListViews :many
SELECT * FROM views WHERE schema_id = ? AND deleted_at IS NULL ORDER BY name LIMIT ? OFFSET ?;

-- name: CountViews :one
SELECT COUNT(*) as cnt FROM views WHERE schema_id = ? AND deleted_at IS NULL;

-- name: DeleteView :exec
UPDATE views SET deleted_at = datetime('now') WHERE schema_id = ? AND name = ?;

-- name: UpdateView :exec
UPDATE views SET comment = ?, properties = ?, view_definition = ?, source_tables = ?, updated_at = datetime('now')
WHERE schema_id = ? AND name = ?;

-- name: DeleteViewsBySchema :exec
UPDATE views SET deleted_at = datetime('now') WHERE schema_id = ?;
