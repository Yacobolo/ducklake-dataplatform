-- +goose Up
PRAGMA foreign_keys = OFF;

CREATE TABLE semantic_models_new (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    description TEXT NOT NULL DEFAULT '',
    owner TEXT NOT NULL DEFAULT '',
    base_relation_ref TEXT NOT NULL,
    default_time_dimension TEXT NOT NULL DEFAULT '',
    tags TEXT NOT NULL DEFAULT '[]',
    created_by TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

INSERT INTO semantic_models_new (
    id,
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
    id,
    name,
    description,
    owner,
    base_relation_ref,
    default_time_dimension,
    tags,
    created_by,
    created_at,
    updated_at
FROM semantic_models;

DROP TABLE semantic_models;
ALTER TABLE semantic_models_new RENAME TO semantic_models;

PRAGMA foreign_key_check;
PRAGMA foreign_keys = ON;

-- +goose Down
SELECT 1;
