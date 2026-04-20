-- +goose Up
ALTER TABLE project_releases ADD COLUMN snapshot_json TEXT;

-- +goose Down
-- SQLite does not support dropping columns in-place; snapshot_json remains on downgrade.
