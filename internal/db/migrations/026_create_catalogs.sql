-- +goose Up
CREATE TABLE IF NOT EXISTS catalogs (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    name           TEXT NOT NULL UNIQUE,
    metastore_type TEXT NOT NULL DEFAULT 'sqlite',
    dsn            TEXT NOT NULL,
    data_path      TEXT NOT NULL,
    status         TEXT NOT NULL DEFAULT 'DETACHED',
    status_message TEXT,
    is_default     INTEGER NOT NULL DEFAULT 0,
    comment        TEXT,
    created_at     TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at     TEXT NOT NULL DEFAULT (datetime('now'))
);
-- Partial unique index: at most one default catalog
CREATE UNIQUE INDEX IF NOT EXISTS idx_catalogs_default ON catalogs(is_default) WHERE is_default = 1;

-- +goose Down
DROP INDEX IF EXISTS idx_catalogs_default;
DROP TABLE IF EXISTS catalogs;
