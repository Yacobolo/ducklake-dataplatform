-- name: CreateColumnMask :one
INSERT INTO column_masks (id, table_id, column_name, mask_expression, description)
VALUES (?, ?, ?, ?, ?)
RETURNING *;

-- name: GetColumnMasksForTable :many
SELECT * FROM column_masks WHERE table_id = ?;

-- name: DeleteColumnMask :exec
DELETE FROM column_masks WHERE id = ?;

-- name: BindColumnMask :exec
INSERT OR IGNORE INTO column_mask_bindings (id, column_mask_id, principal_id, principal_type, see_original)
VALUES (?, ?, ?, ?, ?);

-- name: UnbindColumnMask :exec
DELETE FROM column_mask_bindings
WHERE column_mask_id = ? AND principal_id = ? AND principal_type = ?;

-- name: GetColumnMaskBindingsForMask :many
SELECT * FROM column_mask_bindings WHERE column_mask_id = ?;

-- name: GetColumnMaskBindingsForPrincipal :many
SELECT cm.table_id, cm.column_name, cm.mask_expression, cmb.see_original
FROM column_masks cm
JOIN column_mask_bindings cmb ON cm.id = cmb.column_mask_id
WHERE cmb.principal_id = ? AND cmb.principal_type = ?;

-- name: GetColumnMaskForTableAndPrincipal :many
SELECT cm.column_name, cm.mask_expression, cmb.see_original
FROM column_masks cm
JOIN column_mask_bindings cmb ON cm.id = cmb.column_mask_id
WHERE cm.table_id = ? AND cmb.principal_id = ? AND cmb.principal_type = ?;

-- name: CountColumnMasksForTable :one
SELECT COUNT(*) as cnt FROM column_masks WHERE table_id = ?;

-- name: ListColumnMasksForTablePaginated :many
SELECT * FROM column_masks WHERE table_id = ? ORDER BY id LIMIT ? OFFSET ?;

-- name: DeleteColumnMasksByTable :exec
DELETE FROM column_masks WHERE table_id = ?;
