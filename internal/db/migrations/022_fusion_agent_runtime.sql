-- +goose Up
ALTER TABLE builds ADD COLUMN state_snapshot TEXT;

CREATE TABLE compiled_column_lineage (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    build_id TEXT NOT NULL REFERENCES builds(id) ON DELETE CASCADE,
    project_name TEXT NOT NULL DEFAULT '',
    model_name TEXT NOT NULL DEFAULT '',
    target_catalog TEXT,
    target_schema TEXT NOT NULL DEFAULT '',
    target_table TEXT NOT NULL DEFAULT '',
    target_column TEXT NOT NULL DEFAULT '',
    transform_type TEXT NOT NULL DEFAULT 'UNKNOWN',
    function_name TEXT NOT NULL DEFAULT '',
    partial INTEGER NOT NULL DEFAULT 0,
    source_catalog TEXT,
    source_schema TEXT,
    source_table TEXT,
    source_column TEXT,
    source_kind TEXT NOT NULL DEFAULT '',
    source_model_name TEXT,
    sensitivity_status TEXT NOT NULL DEFAULT '',
    sensitivity_partial INTEGER NOT NULL DEFAULT 0,
    sensitivity_reasons_json TEXT NOT NULL DEFAULT '[]',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_compiled_column_lineage_build ON compiled_column_lineage(build_id, model_name, target_column);
CREATE INDEX idx_compiled_column_lineage_source ON compiled_column_lineage(build_id, source_schema, source_table, source_column);

-- +goose Down
DROP INDEX IF EXISTS idx_compiled_column_lineage_source;
DROP INDEX IF EXISTS idx_compiled_column_lineage_build;
DROP TABLE IF EXISTS compiled_column_lineage;

