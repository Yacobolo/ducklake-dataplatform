-- +goose Up
ALTER TABLE pipelines ADD COLUMN run_as_principal TEXT;
ALTER TABLE pipelines ADD COLUMN admission_mode TEXT NOT NULL DEFAULT 'REJECT' CHECK (admission_mode IN ('REJECT', 'QUEUE'));
ALTER TABLE pipelines ADD COLUMN max_run_duration_seconds INTEGER;
ALTER TABLE pipelines ADD COLUMN notification_webhooks TEXT NOT NULL DEFAULT '[]';
ALTER TABLE pipelines ADD COLUMN default_retry_count INTEGER;
ALTER TABLE pipelines ADD COLUMN default_timeout_seconds INTEGER;
ALTER TABLE pipelines ADD COLUMN default_compute_endpoint_id TEXT;

ALTER TABLE pipeline_runs ADD COLUMN effective_principal TEXT NOT NULL DEFAULT '';
ALTER TABLE pipeline_runs ADD COLUMN queued_at TEXT;
ALTER TABLE pipeline_runs ADD COLUMN queue_started_at TEXT;
ALTER TABLE pipeline_runs ADD COLUMN repaired_from_run_id TEXT REFERENCES pipeline_runs(id) ON DELETE SET NULL;
ALTER TABLE pipeline_runs ADD COLUMN provenance TEXT NOT NULL DEFAULT '{}';
ALTER TABLE pipeline_runs ADD COLUMN sla_breached_at TEXT;

UPDATE pipeline_runs
SET effective_principal = triggered_by
WHERE TRIM(effective_principal) = '';

ALTER TABLE pipeline_job_runs ADD COLUMN effective_compute_endpoint_id TEXT;
ALTER TABLE pipeline_job_runs ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE pipeline_job_runs ADD COLUMN last_error_code TEXT;

UPDATE pipeline_job_runs
SET attempt_count = retry_attempt + 1
WHERE attempt_count = 0;

CREATE TABLE pipeline_run_events (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES pipeline_runs(id) ON DELETE CASCADE,
    job_run_id TEXT REFERENCES pipeline_job_runs(id) ON DELETE CASCADE,
    event_type TEXT NOT NULL,
    message TEXT,
    error_code TEXT,
    metadata TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_pipeline_run_events_run_id_created_at ON pipeline_run_events(run_id, created_at);
CREATE INDEX idx_pipeline_runs_queue_dispatch ON pipeline_runs(pipeline_id, status, queued_at, queue_started_at, created_at);

-- +goose Down
DROP INDEX IF EXISTS idx_pipeline_runs_queue_dispatch;
DROP INDEX IF EXISTS idx_pipeline_run_events_run_id_created_at;
DROP TABLE IF EXISTS pipeline_run_events;
SELECT 1;
