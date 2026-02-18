-- +goose Up
ALTER TABLE model_runs ADD COLUMN compile_manifest TEXT NOT NULL DEFAULT '{}';
ALTER TABLE model_runs ADD COLUMN compile_diagnostics TEXT NOT NULL DEFAULT '{}';

-- +goose Down
