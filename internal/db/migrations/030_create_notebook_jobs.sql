-- +goose Up
CREATE TABLE notebook_jobs (
    id          TEXT PRIMARY KEY,
    notebook_id TEXT NOT NULL REFERENCES notebooks(id) ON DELETE CASCADE,
    session_id  TEXT NOT NULL,
    state       TEXT NOT NULL DEFAULT 'pending' CHECK (state IN ('pending', 'running', 'complete', 'failed')),
    result      TEXT,
    error       TEXT,
    created_at  TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_notebook_jobs_notebook ON notebook_jobs(notebook_id);

-- +goose Down
DROP TABLE IF EXISTS notebook_jobs;
