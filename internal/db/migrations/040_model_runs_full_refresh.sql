-- +goose Up
ALTER TABLE model_runs ADD COLUMN full_refresh INTEGER NOT NULL DEFAULT 0;

-- +goose Down
