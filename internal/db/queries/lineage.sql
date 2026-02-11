-- name: InsertLineageEdge :exec
INSERT INTO lineage_edges (source_table, target_table, edge_type, principal_name, query_hash)
VALUES (?, ?, ?, ?, ?);

-- name: GetUpstreamLineage :many
SELECT DISTINCT source_table, target_table, edge_type, principal_name, created_at
FROM lineage_edges
WHERE target_table = ?
ORDER BY created_at DESC
LIMIT ? OFFSET ?;

-- name: GetDownstreamLineage :many
SELECT DISTINCT source_table, target_table, edge_type, principal_name, created_at
FROM lineage_edges
WHERE source_table = ?
ORDER BY created_at DESC
LIMIT ? OFFSET ?;

-- name: CountUpstreamLineage :one
SELECT COUNT(DISTINCT source_table) as cnt FROM lineage_edges WHERE target_table = ?;

-- name: CountDownstreamLineage :one
SELECT COUNT(DISTINCT source_table || '->' || COALESCE(target_table, '')) as cnt FROM lineage_edges WHERE source_table = ?;
