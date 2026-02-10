-- name: InsertAuditLog :exec
INSERT INTO audit_log (principal_name, action, statement_type, original_sql, rewritten_sql, tables_accessed, status, error_message, duration_ms)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);

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
