-- +goose Up
CREATE TABLE builds (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    environment_id TEXT NOT NULL REFERENCES environments(id) ON DELETE RESTRICT,
    state TEXT NOT NULL DEFAULT 'ready',
    git_ref TEXT NOT NULL,
    commit_sha TEXT,
    selector TEXT NOT NULL DEFAULT '',
    target_catalog TEXT NOT NULL,
    target_schema TEXT NOT NULL,
    source_model_run_id TEXT REFERENCES model_runs(id) ON DELETE SET NULL,
    compile_manifest TEXT NOT NULL,
    compile_diagnostics TEXT,
    created_by TEXT NOT NULL DEFAULT '',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_builds_project_created ON builds(project_id, created_at DESC);
CREATE INDEX idx_builds_environment_created ON builds(environment_id, created_at DESC);

-- +goose Down
DROP INDEX IF EXISTS idx_builds_environment_created;
DROP INDEX IF EXISTS idx_builds_project_created;
DROP TABLE IF EXISTS builds;
