-- +goose Up
CREATE TABLE privilege_grants (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    principal_id INTEGER NOT NULL,
    principal_type TEXT NOT NULL,
    securable_type TEXT NOT NULL,
    securable_id INTEGER NOT NULL,
    privilege TEXT NOT NULL,
    granted_by INTEGER,
    granted_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(principal_id, principal_type, securable_type, securable_id, privilege)
);

CREATE INDEX idx_grants_principal ON privilege_grants(principal_id, principal_type);
CREATE INDEX idx_grants_securable ON privilege_grants(securable_type, securable_id);

-- +goose Down
DROP TABLE privilege_grants;
