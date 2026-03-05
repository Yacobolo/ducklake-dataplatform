-- +goose Up
ALTER TABLE asset_runs ADD COLUMN partition_from TEXT;
ALTER TABLE asset_runs ADD COLUMN partition_to TEXT;

-- +goose Down
ALTER TABLE asset_runs DROP COLUMN partition_to;
ALTER TABLE asset_runs DROP COLUMN partition_from;
