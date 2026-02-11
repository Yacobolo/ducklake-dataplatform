-- +goose Up
CREATE TABLE IF NOT EXISTS external_locations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    url TEXT NOT NULL,
    credential_name TEXT NOT NULL REFERENCES storage_credentials(name),
    storage_type TEXT NOT NULL DEFAULT 'S3',
    comment TEXT NOT NULL DEFAULT '',
    owner TEXT NOT NULL DEFAULT '',
    read_only INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- +goose Down
DROP TABLE IF EXISTS external_locations;
