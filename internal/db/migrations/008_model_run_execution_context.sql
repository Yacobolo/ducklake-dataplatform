-- +goose Up
ALTER TABLE model_runs ADD COLUMN project_name TEXT NOT NULL DEFAULT '';
ALTER TABLE model_runs ADD COLUMN environment_name TEXT NOT NULL DEFAULT '';
ALTER TABLE model_runs ADD COLUMN build_id TEXT;

CREATE INDEX idx_model_runs_project_created ON model_runs(project_name, created_at DESC);
CREATE INDEX idx_model_runs_build ON model_runs(build_id);

-- +goose Down
DROP INDEX IF EXISTS idx_model_runs_build;
DROP INDEX IF EXISTS idx_model_runs_project_created;
