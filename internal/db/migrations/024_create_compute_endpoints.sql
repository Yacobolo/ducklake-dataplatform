-- +goose Up
CREATE TABLE compute_endpoints (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    external_id   TEXT NOT NULL UNIQUE,
    name          TEXT NOT NULL UNIQUE,
    url           TEXT NOT NULL,
    type          TEXT NOT NULL DEFAULT 'REMOTE' CHECK (type IN ('LOCAL','REMOTE')),
    status        TEXT NOT NULL DEFAULT 'INACTIVE' CHECK (status IN ('ACTIVE','INACTIVE','STARTING','STOPPING','ERROR')),
    size          TEXT NOT NULL DEFAULT '',
    max_memory_gb INTEGER,
    auth_token    TEXT NOT NULL DEFAULT '',
    owner         TEXT NOT NULL DEFAULT '',
    created_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE compute_assignments (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    principal_id   INTEGER NOT NULL,
    principal_type TEXT NOT NULL CHECK (principal_type IN ('user','group')),
    endpoint_id    INTEGER NOT NULL REFERENCES compute_endpoints(id) ON DELETE CASCADE,
    is_default     INTEGER NOT NULL DEFAULT 1,
    fallback_local INTEGER NOT NULL DEFAULT 0,
    created_at     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(principal_id, principal_type, endpoint_id)
);

-- +goose Down
DROP TABLE IF EXISTS compute_assignments;
DROP TABLE IF EXISTS compute_endpoints;
