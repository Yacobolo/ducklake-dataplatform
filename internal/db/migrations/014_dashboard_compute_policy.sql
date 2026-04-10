-- +goose Up
ALTER TABLE dashboards ADD COLUMN compute_mode TEXT NOT NULL DEFAULT 'AUTO';
ALTER TABLE dashboards ADD COLUMN compute_endpoint_name TEXT NOT NULL DEFAULT '';
ALTER TABLE dashboards ADD COLUMN compute_fallback_local INTEGER NOT NULL DEFAULT 0;

-- +goose Down
CREATE TABLE dashboards__rollback (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    owner TEXT NOT NULL,
    folder_id TEXT REFERENCES folders(id) ON DELETE SET NULL,
    semantic_project_name TEXT NOT NULL DEFAULT '',
    semantic_model_name TEXT NOT NULL DEFAULT '',
    compute_mode TEXT NOT NULL DEFAULT 'AUTO',
    compute_endpoint_name TEXT NOT NULL DEFAULT '',
    compute_fallback_local INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

INSERT INTO dashboards__rollback (
    id,
    name,
    description,
    owner,
    folder_id,
    semantic_project_name,
    semantic_model_name,
    compute_mode,
    compute_endpoint_name,
    compute_fallback_local,
    created_at,
    updated_at
)
SELECT
    id,
    name,
    description,
    owner,
    folder_id,
    semantic_project_name,
    semantic_model_name,
    compute_mode,
    compute_endpoint_name,
    compute_fallback_local,
    created_at,
    updated_at
FROM dashboards;

DROP TABLE dashboards;
ALTER TABLE dashboards__rollback RENAME TO dashboards;
CREATE INDEX idx_dashboards_owner ON dashboards(owner);
