-- +goose Up
CREATE TABLE git_repos (
    id             TEXT PRIMARY KEY,
    url            TEXT NOT NULL,
    branch         TEXT NOT NULL DEFAULT 'main',
    path           TEXT NOT NULL DEFAULT '',
    auth_token     TEXT NOT NULL DEFAULT '',
    webhook_secret TEXT,
    owner          TEXT NOT NULL,
    last_sync_at   TEXT,
    last_commit    TEXT,
    created_at     TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at     TEXT NOT NULL DEFAULT (datetime('now'))
);

ALTER TABLE notebooks ADD COLUMN git_repo_id TEXT REFERENCES git_repos(id) ON DELETE SET NULL;
ALTER TABLE notebooks ADD COLUMN git_path TEXT;

-- +goose Down
-- Note: SQLite does not support DROP COLUMN. For a clean rollback, 
-- the notebooks table would need to be recreated. In practice, 
-- the added columns are harmless if left in place.
DROP TABLE IF EXISTS git_repos;
