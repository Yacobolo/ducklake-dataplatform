-- +goose Up
ALTER TABLE pipeline_jobs ADD COLUMN job_type TEXT NOT NULL DEFAULT 'NOTEBOOK';
ALTER TABLE pipeline_jobs ADD COLUMN model_selector TEXT NOT NULL DEFAULT '';

-- +goose Down
