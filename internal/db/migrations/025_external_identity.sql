-- +goose Up
ALTER TABLE principals ADD COLUMN external_id TEXT;
ALTER TABLE principals ADD COLUMN external_issuer TEXT;
CREATE UNIQUE INDEX idx_principals_external
    ON principals(external_issuer, external_id) WHERE external_id IS NOT NULL;

-- +goose Down
DROP INDEX IF EXISTS idx_principals_external;
-- SQLite doesn't support DROP COLUMN before 3.35.0, so we recreate the table
CREATE TABLE principals_backup AS SELECT id, name, type, is_admin, created_at FROM principals;
DROP TABLE principals;
CREATE TABLE principals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    type TEXT NOT NULL DEFAULT 'user',
    is_admin INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
INSERT INTO principals SELECT * FROM principals_backup;
DROP TABLE principals_backup;
