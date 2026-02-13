-- +goose Up
-- Add catalog_name to external_tables (mirrors volumes table pattern).
ALTER TABLE external_tables ADD COLUMN catalog_name TEXT NOT NULL DEFAULT 'lake';

-- +goose Down
-- SQLite does not support DROP COLUMN before 3.35.0; recreate table without catalog_name.
CREATE TABLE external_tables_backup AS SELECT id, schema_name, table_name, file_format, source_path, location_name, comment, owner, created_at, updated_at, deleted_at FROM external_tables;
DROP TABLE external_tables;
ALTER TABLE external_tables_backup RENAME TO external_tables;
