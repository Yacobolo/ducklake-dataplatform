-- +goose Up
ALTER TABLE resource_access_events ADD COLUMN resource_path TEXT;
ALTER TABLE saved_resources ADD COLUMN resource_path TEXT;

-- +goose Down
SELECT 1;
