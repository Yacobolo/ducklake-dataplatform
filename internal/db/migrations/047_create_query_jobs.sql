-- +goose Up
CREATE TABLE query_jobs (
  id TEXT PRIMARY KEY,
  principal_name TEXT NOT NULL,
  request_id TEXT NOT NULL,
  sql_text TEXT NOT NULL,
  status TEXT NOT NULL,
  columns_json TEXT,
  rows_json TEXT,
  row_count INTEGER NOT NULL DEFAULT 0,
  error_message TEXT,
  started_at DATETIME,
  completed_at DATETIME,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE (principal_name, request_id)
);

CREATE INDEX idx_query_jobs_principal_created_at ON query_jobs(principal_name, created_at DESC);
CREATE INDEX idx_query_jobs_status ON query_jobs(status);

-- +goose Down
DROP INDEX IF EXISTS idx_query_jobs_status;
DROP INDEX IF EXISTS idx_query_jobs_principal_created_at;
DROP TABLE IF EXISTS query_jobs;
