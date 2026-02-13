-- name: CreateExternalTable :one
INSERT INTO external_tables (id, schema_name, table_name, file_format, source_path, location_name, comment, owner, catalog_name)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
RETURNING *;

-- name: GetExternalTableByName :one
SELECT * FROM external_tables
WHERE schema_name = ? AND table_name = ? AND deleted_at IS NULL;

-- name: GetExternalTableByID :one
SELECT * FROM external_tables
WHERE id = ? AND deleted_at IS NULL;

-- name: GetExternalTableByTableName :one
SELECT * FROM external_tables
WHERE table_name = ? AND deleted_at IS NULL;

-- name: ListExternalTables :many
SELECT * FROM external_tables
WHERE schema_name = ? AND deleted_at IS NULL
ORDER BY table_name
LIMIT ? OFFSET ?;

-- name: CountExternalTables :one
SELECT COUNT(*) FROM external_tables
WHERE schema_name = ? AND deleted_at IS NULL;

-- name: SoftDeleteExternalTable :exec
UPDATE external_tables SET deleted_at = datetime('now')
WHERE schema_name = ? AND table_name = ? AND deleted_at IS NULL;

-- name: SoftDeleteExternalTablesBySchema :exec
UPDATE external_tables SET deleted_at = datetime('now')
WHERE schema_name = ? AND deleted_at IS NULL;

-- name: UpdateExternalTable :exec
UPDATE external_tables
SET comment = CASE WHEN @set_comment = 1 THEN @comment ELSE comment END,
    owner = CASE WHEN @set_owner = 1 THEN @owner ELSE owner END,
    updated_at = datetime('now')
WHERE schema_name = @schema_name AND table_name = @table_name AND deleted_at IS NULL;

-- name: InsertExternalTableColumn :exec
INSERT INTO external_table_columns (id, external_table_id, column_name, column_type, position)
VALUES (?, ?, ?, ?, ?);

-- name: ListExternalTableColumns :many
SELECT * FROM external_table_columns
WHERE external_table_id = ?
ORDER BY position;

-- name: DeleteExternalTableColumns :exec
DELETE FROM external_table_columns
WHERE external_table_id = ?;

-- name: ListAllExternalTables :many
SELECT * FROM external_tables
WHERE deleted_at IS NULL;
