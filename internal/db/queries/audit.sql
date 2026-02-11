-- name: InsertAuditLog :exec
INSERT INTO audit_log (principal_name, action, statement_type, original_sql, rewritten_sql, tables_accessed, status, error_message, duration_ms, rows_returned)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

-- name: ListAuditLogs :many
SELECT * FROM audit_log
WHERE (? IS NULL OR principal_name = ?)
  AND (? IS NULL OR action = ?)
  AND (? IS NULL OR status = ?)
ORDER BY created_at DESC
LIMIT ? OFFSET ?;

-- name: CountAuditLogs :one
SELECT COUNT(*) as cnt FROM audit_log
WHERE (? IS NULL OR principal_name = ?)
  AND (? IS NULL OR action = ?)
  AND (? IS NULL OR status = ?);

-- name: ListQueryHistory :many
SELECT * FROM audit_log
WHERE action = 'QUERY'
  AND (? IS NULL OR principal_name = ?)
  AND (? IS NULL OR status = ?)
  AND (? IS NULL OR created_at >= ?)
  AND (? IS NULL OR created_at <= ?)
ORDER BY created_at DESC
LIMIT ? OFFSET ?;

-- name: CountQueryHistory :one
SELECT COUNT(*) as cnt FROM audit_log
WHERE action = 'QUERY'
  AND (? IS NULL OR principal_name = ?)
  AND (? IS NULL OR status = ?)
  AND (? IS NULL OR created_at >= ?)
  AND (? IS NULL OR created_at <= ?);
