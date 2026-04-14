-- +goose Up
PRAGMA foreign_keys = OFF;

CREATE TABLE semantic_models_new (
    id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    owner TEXT NOT NULL DEFAULT '',
    base_relation_ref TEXT NOT NULL,
    default_time_dimension TEXT NOT NULL DEFAULT '',
    tags TEXT NOT NULL DEFAULT '[]',
    created_by TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(workspace_id, name)
);

INSERT INTO semantic_models_new (
    id,
    workspace_id,
    name,
    description,
    owner,
    base_relation_ref,
    default_time_dimension,
    tags,
    created_by,
    created_at,
    updated_at
)
SELECT
    sm.id,
    COALESCE(
        (
            SELECT f.workspace_id
            FROM dashboards d
            JOIN folders f ON f.id = d.folder_id
            WHERE d.semantic_model_name = sm.name
            ORDER BY d.updated_at DESC
            LIMIT 1
        ),
        (
            SELECT ws.id
            FROM workspaces ws
            WHERE ws.owner_principal = sm.created_by
            ORDER BY ws.created_at ASC
            LIMIT 1
        ),
        (
            SELECT id
            FROM workspaces
            ORDER BY created_at ASC
            LIMIT 1
        )
    ) AS workspace_id,
    sm.name,
    sm.description,
    sm.owner,
    sm.base_relation_ref,
    sm.default_time_dimension,
    sm.tags,
    sm.created_by,
    sm.created_at,
    sm.updated_at
FROM semantic_models sm;

DROP TABLE semantic_models;
ALTER TABLE semantic_models_new RENAME TO semantic_models;
CREATE INDEX idx_semantic_models_workspace ON semantic_models(workspace_id);

PRAGMA foreign_key_check;
PRAGMA foreign_keys = ON;

-- +goose Down
SELECT 1;
