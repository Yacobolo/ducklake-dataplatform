-- +goose Up
CREATE TABLE macro_revisions (
    id TEXT PRIMARY KEY,
    macro_id TEXT NOT NULL,
    macro_name TEXT NOT NULL,
    version INTEGER NOT NULL,
    content_hash TEXT NOT NULL,
    parameters TEXT NOT NULL DEFAULT '[]',
    body TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'ACTIVE',
    created_by TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (macro_id, version),
    FOREIGN KEY (macro_id) REFERENCES macros(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_macro_revisions_name_version ON macro_revisions(macro_name, version DESC);

-- +goose Down
DROP TABLE IF EXISTS macro_revisions;
