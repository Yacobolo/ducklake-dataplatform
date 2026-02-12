-- name: CreateCatalog :one
INSERT INTO catalogs (name, metastore_type, dsn, data_path, status, status_message, is_default, comment)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
RETURNING *;

-- name: GetCatalogByID :one
SELECT * FROM catalogs WHERE id = ?;

-- name: GetCatalogByName :one
SELECT * FROM catalogs WHERE name = ?;

-- name: ListCatalogs :many
SELECT * FROM catalogs ORDER BY name LIMIT ? OFFSET ?;

-- name: CountCatalogs :one
SELECT COUNT(*) FROM catalogs;

-- name: UpdateCatalog :one
UPDATE catalogs
SET comment = COALESCE(?, comment),
    data_path = COALESCE(?, data_path),
    dsn = COALESCE(?, dsn),
    updated_at = datetime('now')
WHERE id = ?
RETURNING *;

-- name: UpdateCatalogStatus :exec
UPDATE catalogs
SET status = ?, status_message = ?, updated_at = datetime('now')
WHERE id = ?;

-- name: DeleteCatalog :exec
DELETE FROM catalogs WHERE id = ?;

-- name: GetDefaultCatalog :one
SELECT * FROM catalogs WHERE is_default = 1;

-- name: ClearDefaultCatalog :exec
UPDATE catalogs SET is_default = 0, updated_at = datetime('now') WHERE is_default = 1;

-- name: SetDefaultCatalog :exec
UPDATE catalogs SET is_default = 1, updated_at = datetime('now') WHERE id = ?;
