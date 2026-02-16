-- +goose Up
CREATE TABLE models (
    id TEXT PRIMARY KEY,
    project_name TEXT NOT NULL,
    name TEXT NOT NULL,
    sql_body TEXT NOT NULL,
    materialization TEXT NOT NULL DEFAULT 'VIEW'
        CHECK (materialization IN ('VIEW','TABLE','INCREMENTAL','EPHEMERAL')),
    description TEXT NOT NULL DEFAULT '',
    owner TEXT NOT NULL DEFAULT '',
    tags TEXT NOT NULL DEFAULT '[]',
    depends_on TEXT NOT NULL DEFAULT '[]',
    config TEXT NOT NULL DEFAULT '{}',
    created_by TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(project_name, name)
);
CREATE INDEX idx_models_project ON models(project_name);

CREATE TABLE model_runs (
    id TEXT PRIMARY KEY,
    status TEXT NOT NULL DEFAULT 'PENDING'
        CHECK (status IN ('PENDING','RUNNING','SUCCESS','FAILED','CANCELLED')),
    trigger_type TEXT NOT NULL CHECK (trigger_type IN ('MANUAL','SCHEDULED','PIPELINE')),
    triggered_by TEXT NOT NULL,
    target_catalog TEXT NOT NULL DEFAULT '',
    target_schema TEXT NOT NULL DEFAULT '',
    model_selector TEXT NOT NULL DEFAULT '',
    variables TEXT NOT NULL DEFAULT '{}',
    started_at TEXT,
    finished_at TEXT,
    error_message TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX idx_model_runs_status ON model_runs(status);

CREATE TABLE model_run_steps (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES model_runs(id) ON DELETE CASCADE,
    model_id TEXT NOT NULL,
    model_name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING'
        CHECK (status IN ('PENDING','RUNNING','SUCCESS','FAILED','SKIPPED','CANCELLED')),
    tier INTEGER NOT NULL DEFAULT 0,
    rows_affected INTEGER,
    started_at TEXT,
    finished_at TEXT,
    error_message TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX idx_model_run_steps_run ON model_run_steps(run_id);

-- +goose Down
DROP TABLE IF EXISTS model_run_steps;
DROP TABLE IF EXISTS model_runs;
DROP TABLE IF EXISTS models;
