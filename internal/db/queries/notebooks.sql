-- name: CreateNotebook :one
INSERT INTO notebooks (id, name, description, owner)
VALUES (?, ?, ?, ?)
RETURNING *;

-- name: GetNotebook :one
SELECT * FROM notebooks WHERE id = ?;

-- name: ListNotebooks :many
SELECT * FROM notebooks
WHERE (sqlc.narg('owner') IS NULL OR owner = sqlc.narg('owner'))
ORDER BY updated_at DESC
LIMIT sqlc.arg('limit') OFFSET sqlc.arg('offset');

-- name: CountNotebooks :one
SELECT COUNT(*) FROM notebooks
WHERE (sqlc.narg('owner') IS NULL OR owner = sqlc.narg('owner'));

-- name: UpdateNotebook :one
UPDATE notebooks
SET name = ?, description = ?, updated_at = datetime('now')
WHERE id = ?
RETURNING *;

-- name: DeleteNotebook :exec
DELETE FROM notebooks WHERE id = ?;

-- name: CreateCell :one
INSERT INTO cells (id, notebook_id, cell_type, content, position)
VALUES (?, ?, ?, ?, ?)
RETURNING *;

-- name: GetCell :one
SELECT * FROM cells WHERE id = ?;

-- name: ListCells :many
SELECT * FROM cells WHERE notebook_id = ? ORDER BY position ASC;

-- name: UpdateCell :one
UPDATE cells
SET content = ?, position = ?, updated_at = datetime('now')
WHERE id = ?
RETURNING *;

-- name: DeleteCell :exec
DELETE FROM cells WHERE id = ?;

-- name: UpdateCellResult :exec
UPDATE cells SET last_result = ?, updated_at = datetime('now') WHERE id = ?;

-- name: GetMaxCellPosition :one
SELECT COALESCE(MAX(position), -1) FROM cells WHERE notebook_id = ?;

-- name: UpdateCellPosition :exec
UPDATE cells SET position = ?, updated_at = datetime('now') WHERE id = ?;
