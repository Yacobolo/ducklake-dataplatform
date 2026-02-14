-- +goose Up
CREATE TABLE pipelines (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    description TEXT NOT NULL DEFAULT '',
    schedule_cron TEXT,
    is_paused INTEGER NOT NULL DEFAULT 0,
    concurrency_limit INTEGER NOT NULL DEFAULT 1,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE pipeline_jobs (
    id TEXT PRIMARY KEY,
    pipeline_id TEXT NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    compute_endpoint_id TEXT,
    depends_on TEXT NOT NULL DEFAULT '[]',
    notebook_id TEXT NOT NULL,
    timeout_seconds INTEGER,
    retry_count INTEGER NOT NULL DEFAULT 0,
    job_order INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(pipeline_id, name)
);

CREATE TABLE pipeline_runs (
    id TEXT PRIMARY KEY,
    pipeline_id TEXT NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    status TEXT NOT NULL DEFAULT 'PENDING'
           CHECK (status IN ('PENDING','RUNNING','SUCCESS','FAILED','CANCELLED')),
    trigger_type TEXT NOT NULL CHECK (trigger_type IN ('MANUAL','SCHEDULED')),
    triggered_by TEXT NOT NULL,
    parameters TEXT NOT NULL DEFAULT '{}',
    git_commit_hash TEXT,
    started_at TEXT,
    finished_at TEXT,
    error_message TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX idx_pipeline_runs_pipeline ON pipeline_runs(pipeline_id);
CREATE INDEX idx_pipeline_runs_status ON pipeline_runs(status);

CREATE TABLE pipeline_job_runs (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES pipeline_runs(id) ON DELETE CASCADE,
    job_id TEXT NOT NULL,
    job_name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING'
           CHECK (status IN ('PENDING','RUNNING','SUCCESS','FAILED','SKIPPED','CANCELLED')),
    started_at TEXT,
    finished_at TEXT,
    error_message TEXT,
    retry_attempt INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX idx_pipeline_job_runs_run ON pipeline_job_runs(run_id);

-- +goose Down
DROP TABLE IF EXISTS pipeline_job_runs;
DROP TABLE IF EXISTS pipeline_runs;
DROP TABLE IF EXISTS pipeline_jobs;
DROP TABLE IF EXISTS pipelines;
