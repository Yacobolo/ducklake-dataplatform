-- +goose Up
CREATE TABLE IF NOT EXISTS volumes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    schema_name TEXT NOT NULL,
    catalog_name TEXT NOT NULL DEFAULT 'lake',
    volume_type TEXT NOT NULL CHECK(volume_type IN ('MANAGED', 'EXTERNAL')),
    storage_location TEXT NOT NULL DEFAULT '',
    comment TEXT NOT NULL DEFAULT '',
    owner TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(catalog_name, schema_name, name)
);

-- +goose Down
DROP TABLE IF EXISTS volumes;
