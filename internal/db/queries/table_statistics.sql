-- name: UpsertTableStatistics :exec
INSERT INTO table_statistics (table_securable_name, row_count, size_bytes, column_count, last_profiled_at, profiled_by)
VALUES (?, ?, ?, ?, datetime('now'), ?)
ON CONFLICT(table_securable_name)
DO UPDATE SET row_count = excluded.row_count,
              size_bytes = excluded.size_bytes,
              column_count = excluded.column_count,
              last_profiled_at = datetime('now'),
              profiled_by = excluded.profiled_by;

-- name: GetTableStatistics :one
SELECT * FROM table_statistics WHERE table_securable_name = ?;

-- name: DeleteTableStatistics :exec
DELETE FROM table_statistics WHERE table_securable_name = ?;

-- name: DeleteTableStatisticsByPattern :exec
DELETE FROM table_statistics WHERE table_securable_name LIKE ?;
