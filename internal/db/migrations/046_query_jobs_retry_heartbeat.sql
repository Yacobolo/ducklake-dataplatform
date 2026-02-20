-- +goose Up
ALTER TABLE query_jobs ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE query_jobs ADD COLUMN max_attempts INTEGER NOT NULL DEFAULT 1;
ALTER TABLE query_jobs ADD COLUMN last_heartbeat_at DATETIME;
ALTER TABLE query_jobs ADD COLUMN next_retry_at DATETIME;

CREATE INDEX idx_query_jobs_next_retry_at ON query_jobs(next_retry_at);

-- +goose Down
DROP INDEX IF EXISTS idx_query_jobs_next_retry_at;
