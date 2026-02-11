-- +goose Up
CREATE TABLE IF NOT EXISTS views (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    schema_id        INTEGER NOT NULL,
    name             TEXT NOT NULL,
    view_definition  TEXT NOT NULL,
    comment          TEXT,
    properties       TEXT DEFAULT '{}',
    owner            TEXT NOT NULL,
    source_tables    TEXT,
    created_at       TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at       TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(schema_id, name)
);

-- +goose Down
DROP TABLE IF EXISTS views;
