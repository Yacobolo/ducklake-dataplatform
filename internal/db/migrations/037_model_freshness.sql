-- +goose Up
ALTER TABLE models ADD COLUMN freshness_max_lag INTEGER;
ALTER TABLE models ADD COLUMN freshness_cron TEXT;

-- +goose Down
