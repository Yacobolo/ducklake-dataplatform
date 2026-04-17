-- +goose Up
ALTER TABLE models ADD COLUMN config_json_v2 TEXT NOT NULL DEFAULT '{}';

CREATE TABLE IF NOT EXISTS project_dependencies (
    id                 TEXT PRIMARY KEY,
    project_id         TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    dependency_project TEXT NOT NULL,
    dependency_kind    TEXT NOT NULL DEFAULT 'project',
    position           INTEGER NOT NULL DEFAULT 0,
    created_by         TEXT NOT NULL,
    created_at         TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at         TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(project_id, dependency_project)
);

CREATE TABLE IF NOT EXISTS source_definitions (
    id                TEXT PRIMARY KEY,
    project_name      TEXT NOT NULL,
    source_name       TEXT NOT NULL,
    table_name        TEXT NOT NULL,
    relation_ref      TEXT NOT NULL,
    description       TEXT NOT NULL DEFAULT '',
    freshness_json    TEXT NOT NULL DEFAULT '{}',
    created_by        TEXT NOT NULL,
    created_at        TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at        TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(project_name, source_name, table_name)
);

CREATE TABLE IF NOT EXISTS seeds (
    id                TEXT PRIMARY KEY,
    project_name      TEXT NOT NULL,
    name              TEXT NOT NULL,
    description       TEXT NOT NULL DEFAULT '',
    input_ref         TEXT NOT NULL,
    format            TEXT NOT NULL DEFAULT 'csv',
    delimiter         TEXT NOT NULL DEFAULT ',',
    has_header        INTEGER NOT NULL DEFAULT 1,
    column_types_json TEXT NOT NULL DEFAULT '{}',
    tags_json         TEXT NOT NULL DEFAULT '[]',
    created_by        TEXT NOT NULL,
    created_at        TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at        TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(project_name, name)
);

-- +goose Down
DROP TABLE IF EXISTS seeds;
DROP TABLE IF EXISTS source_definitions;
DROP TABLE IF EXISTS project_dependencies;
