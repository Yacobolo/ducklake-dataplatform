-- +goose Up
CREATE TABLE IF NOT EXISTS catalog_metadata (
    securable_type TEXT NOT NULL,   -- 'schema' or 'table'
    securable_name TEXT NOT NULL,   -- e.g. 'main' or 'main.titanic'
    comment        TEXT,
    properties     TEXT,            -- JSON key-value map
    owner          TEXT,
    created_at     TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at     TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(securable_type, securable_name)
);

-- +goose Down
DROP TABLE IF EXISTS catalog_metadata;
