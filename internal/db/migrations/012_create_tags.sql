-- +goose Up
CREATE TABLE IF NOT EXISTS tags (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    key         TEXT NOT NULL,
    value       TEXT,
    created_by  TEXT NOT NULL,
    created_at  TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(key, value)
);

CREATE TABLE IF NOT EXISTS tag_assignments (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    tag_id          INTEGER NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
    securable_type  TEXT NOT NULL,
    securable_id    INTEGER NOT NULL,
    column_name     TEXT,
    assigned_by     TEXT NOT NULL,
    assigned_at     TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(tag_id, securable_type, securable_id, column_name)
);
CREATE INDEX idx_tag_assignments_securable ON tag_assignments(securable_type, securable_id);

-- +goose Down
DROP TABLE IF EXISTS tag_assignments;
DROP TABLE IF EXISTS tags;
