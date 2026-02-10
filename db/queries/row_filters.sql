-- name: CreateRowFilter :one
INSERT INTO row_filters (table_id, filter_sql, description)
VALUES (?, ?, ?)
RETURNING *;

-- name: GetRowFiltersForTable :many
SELECT * FROM row_filters WHERE table_id = ?;

-- name: DeleteRowFilter :exec
DELETE FROM row_filters WHERE id = ?;

-- name: BindRowFilter :exec
INSERT OR IGNORE INTO row_filter_bindings (row_filter_id, principal_id, principal_type)
VALUES (?, ?, ?);

-- name: UnbindRowFilter :exec
DELETE FROM row_filter_bindings
WHERE row_filter_id = ? AND principal_id = ? AND principal_type = ?;

-- name: GetRowFilterBindingsForFilter :many
SELECT * FROM row_filter_bindings WHERE row_filter_id = ?;

-- name: GetRowFiltersForPrincipal :many
SELECT rf.* FROM row_filters rf
JOIN row_filter_bindings rfb ON rf.id = rfb.row_filter_id
WHERE rfb.principal_id = ? AND rfb.principal_type = ?;

-- name: GetRowFiltersForTableAndPrincipal :many
SELECT rf.* FROM row_filters rf
JOIN row_filter_bindings rfb ON rf.id = rfb.row_filter_id
WHERE rf.table_id = ? AND rfb.principal_id = ? AND rfb.principal_type = ?;
