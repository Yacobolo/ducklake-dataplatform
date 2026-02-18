-- +goose Up
ALTER TABLE model_run_steps ADD COLUMN compiled_sql TEXT;
ALTER TABLE model_run_steps ADD COLUMN compiled_hash TEXT;
ALTER TABLE model_run_steps ADD COLUMN depends_on TEXT NOT NULL DEFAULT '[]';
ALTER TABLE model_run_steps ADD COLUMN vars_used TEXT NOT NULL DEFAULT '[]';
ALTER TABLE model_run_steps ADD COLUMN macros_used TEXT NOT NULL DEFAULT '[]';

-- +goose Down
