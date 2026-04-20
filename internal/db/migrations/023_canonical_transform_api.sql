-- +goose Up
ALTER TABLE project_dependencies ADD COLUMN dependency_project_id TEXT NOT NULL DEFAULT '';
ALTER TABLE project_dependencies ADD COLUMN version_constraint TEXT NOT NULL DEFAULT '';
ALTER TABLE project_dependencies ADD COLUMN resolved_release_id TEXT;

ALTER TABLE builds ADD COLUMN resolved_release_id TEXT;

CREATE TABLE IF NOT EXISTS compilations (
    id                  TEXT PRIMARY KEY,
    project_id          TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    environment_id      TEXT NOT NULL REFERENCES environments(id) ON DELETE CASCADE,
    git_ref             TEXT NOT NULL,
    commit_sha          TEXT,
    selector            TEXT NOT NULL DEFAULT '',
    target_catalog      TEXT NOT NULL,
    target_schema       TEXT NOT NULL,
    resolved_release_id TEXT,
    compile_manifest    TEXT NOT NULL,
    compile_diagnostics TEXT,
    state_snapshot      TEXT,
    created_by          TEXT NOT NULL,
    created_at          TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_compilations_project_env_created
    ON compilations(project_id, environment_id, created_at DESC);

CREATE TABLE IF NOT EXISTS project_releases (
    id                    TEXT PRIMARY KEY,
    project_id            TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    version               TEXT NOT NULL,
    resolved_build_id     TEXT REFERENCES builds(id) ON DELETE SET NULL,
    resolved_compilation_id TEXT REFERENCES compilations(id) ON DELETE SET NULL,
    created_by            TEXT NOT NULL,
    created_at            TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(project_id, version)
);

ALTER TABLE compiled_column_lineage ADD COLUMN compilation_id TEXT REFERENCES compilations(id) ON DELETE CASCADE;

CREATE INDEX IF NOT EXISTS idx_compiled_column_lineage_compilation
    ON compiled_column_lineage(compilation_id, model_name, target_column);

CREATE INDEX IF NOT EXISTS idx_compiled_column_lineage_compilation_source
    ON compiled_column_lineage(compilation_id, source_schema, source_table, source_column);

-- +goose Down
DROP INDEX IF EXISTS idx_compiled_column_lineage_compilation_source;
DROP INDEX IF EXISTS idx_compiled_column_lineage_compilation;
-- SQLite does not support dropping columns in-place; compilation_id and other added columns remain on downgrade.
DROP TABLE IF EXISTS project_releases;
DROP INDEX IF EXISTS idx_compilations_project_env_created;
DROP TABLE IF EXISTS compilations;
