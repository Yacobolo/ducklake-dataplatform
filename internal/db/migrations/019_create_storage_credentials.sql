-- +goose Up
CREATE TABLE IF NOT EXISTS storage_credentials (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    credential_type TEXT NOT NULL DEFAULT 'S3',
    key_id_encrypted TEXT NOT NULL,
    secret_encrypted TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    region TEXT NOT NULL,
    url_style TEXT NOT NULL DEFAULT 'path',
    comment TEXT NOT NULL DEFAULT '',
    owner TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- +goose Down
DROP TABLE IF EXISTS storage_credentials;
