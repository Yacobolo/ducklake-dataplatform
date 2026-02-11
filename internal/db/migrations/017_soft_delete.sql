-- +goose Up
ALTER TABLE catalog_metadata ADD COLUMN deleted_at TEXT;
ALTER TABLE views ADD COLUMN deleted_at TEXT;

-- +goose Down
-- SQLite does not support DROP COLUMN, so no rollback for ALTER TABLE
