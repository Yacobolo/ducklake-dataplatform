-- +goose Up
ALTER TABLE lineage_edges ADD COLUMN source_schema TEXT;
ALTER TABLE lineage_edges ADD COLUMN target_schema TEXT;

-- +goose Down
-- SQLite does not support DROP COLUMN, so no rollback for ALTER TABLE
