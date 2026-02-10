-- +goose Up
CREATE TABLE audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    principal_name TEXT NOT NULL,
    action TEXT NOT NULL,
    statement_type TEXT,
    original_sql TEXT,
    rewritten_sql TEXT,
    tables_accessed TEXT,
    status TEXT NOT NULL,
    error_message TEXT,
    duration_ms INTEGER,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_audit_principal ON audit_log(principal_name);
CREATE INDEX idx_audit_created ON audit_log(created_at);
CREATE INDEX idx_audit_status ON audit_log(status);

-- +goose Down
DROP TABLE audit_log;
