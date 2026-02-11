-- +goose Up
ALTER TABLE audit_log ADD COLUMN rows_returned INTEGER;

-- +goose Down
-- SQLite doesn't support DROP COLUMN in older versions, so we recreate the table.
CREATE TABLE audit_log_backup AS SELECT id, principal_name, action, statement_type, original_sql, rewritten_sql, tables_accessed, status, error_message, duration_ms, created_at FROM audit_log;
DROP TABLE audit_log;
ALTER TABLE audit_log_backup RENAME TO audit_log;
