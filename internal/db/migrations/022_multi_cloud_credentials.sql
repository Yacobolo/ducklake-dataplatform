-- +goose Up
-- Add Azure credential fields
ALTER TABLE storage_credentials ADD COLUMN azure_account_name TEXT NOT NULL DEFAULT '';
ALTER TABLE storage_credentials ADD COLUMN azure_account_key_encrypted TEXT NOT NULL DEFAULT '';
ALTER TABLE storage_credentials ADD COLUMN azure_client_id TEXT NOT NULL DEFAULT '';
ALTER TABLE storage_credentials ADD COLUMN azure_tenant_id TEXT NOT NULL DEFAULT '';
ALTER TABLE storage_credentials ADD COLUMN azure_client_secret_encrypted TEXT NOT NULL DEFAULT '';

-- Add GCS credential fields
ALTER TABLE storage_credentials ADD COLUMN gcs_key_file_path TEXT NOT NULL DEFAULT '';

-- +goose Down
-- SQLite does not support DROP COLUMN before 3.35.0; recreate the table.
CREATE TABLE storage_credentials_backup AS SELECT
    id, name, credential_type, key_id_encrypted, secret_encrypted,
    endpoint, region, url_style, comment, owner, created_at, updated_at
FROM storage_credentials;
DROP TABLE storage_credentials;
CREATE TABLE storage_credentials (
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
INSERT INTO storage_credentials SELECT * FROM storage_credentials_backup;
DROP TABLE storage_credentials_backup;
