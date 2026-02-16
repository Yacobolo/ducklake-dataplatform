-- name: InsertColumnLineageEdge :exec
INSERT INTO column_lineage_edges (lineage_edge_id, target_column, source_schema, source_table, source_column, transform_type, function_name)
VALUES (?, ?, ?, ?, ?, ?, ?);

-- name: GetColumnLineageByEdgeID :many
SELECT id, lineage_edge_id, target_column, source_schema, source_table,
       source_column, transform_type, function_name, created_at
FROM column_lineage_edges
WHERE lineage_edge_id = ?
ORDER BY target_column, source_table, source_column;

-- name: GetColumnLineageForTable :many
SELECT cle.id, cle.lineage_edge_id, cle.target_column, cle.source_schema,
       cle.source_table, cle.source_column, cle.transform_type, cle.function_name,
       cle.created_at
FROM column_lineage_edges cle
JOIN lineage_edges le ON le.id = cle.lineage_edge_id
WHERE le.target_schema = ? AND le.target_table = ?
ORDER BY cle.target_column, cle.source_table, cle.source_column;

-- name: GetColumnLineageForSourceColumn :many
SELECT cle.id, cle.lineage_edge_id, cle.target_column, cle.source_schema,
       cle.source_table, cle.source_column, cle.transform_type, cle.function_name,
       cle.created_at
FROM column_lineage_edges cle
JOIN lineage_edges le ON le.id = cle.lineage_edge_id
WHERE cle.source_schema = ? AND cle.source_table = ? AND cle.source_column = ?
ORDER BY cle.target_column;

-- name: DeleteColumnLineageByEdgeID :exec
DELETE FROM column_lineage_edges WHERE lineage_edge_id = ?;
