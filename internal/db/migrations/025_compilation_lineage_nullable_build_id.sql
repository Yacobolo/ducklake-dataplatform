-- +goose Up
CREATE TABLE compiled_column_lineage_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    build_id TEXT REFERENCES builds(id) ON DELETE CASCADE,
    compilation_id TEXT REFERENCES compilations(id) ON DELETE CASCADE,
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

INSERT INTO compiled_column_lineage_new (
    id, build_id, compilation_id, project_name, model_name, target_catalog, target_schema, target_table, target_column,
    transform_type, function_name, partial, source_catalog, source_schema, source_table, source_column,
    source_kind, source_model_name, sensitivity_status, sensitivity_partial, sensitivity_reasons_json, created_at
)
SELECT
    id, NULLIF(build_id, ''), compilation_id, project_name, model_name, target_catalog, target_schema, target_table, target_column,
    transform_type, function_name, partial, source_catalog, source_schema, source_table, source_column,
    source_kind, source_model_name, sensitivity_status, sensitivity_partial, sensitivity_reasons_json, created_at
FROM compiled_column_lineage;

DROP INDEX IF EXISTS idx_compiled_column_lineage_compilation_source;
DROP INDEX IF EXISTS idx_compiled_column_lineage_compilation;
DROP INDEX IF EXISTS idx_compiled_column_lineage_source;
DROP INDEX IF EXISTS idx_compiled_column_lineage_build;

DROP TABLE compiled_column_lineage;
ALTER TABLE compiled_column_lineage_new RENAME TO compiled_column_lineage;

CREATE INDEX idx_compiled_column_lineage_build
    ON compiled_column_lineage(build_id, model_name, target_column);
CREATE INDEX idx_compiled_column_lineage_source
    ON compiled_column_lineage(build_id, source_schema, source_table, source_column);
CREATE INDEX idx_compiled_column_lineage_compilation
    ON compiled_column_lineage(compilation_id, model_name, target_column);
CREATE INDEX idx_compiled_column_lineage_compilation_source
    ON compiled_column_lineage(compilation_id, source_schema, source_table, source_column);

-- +goose Down
CREATE TABLE compiled_column_lineage_old (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    build_id TEXT NOT NULL REFERENCES builds(id) ON DELETE CASCADE,
    compilation_id TEXT REFERENCES compilations(id) ON DELETE CASCADE,
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

INSERT INTO compiled_column_lineage_old (
    id, build_id, compilation_id, project_name, model_name, target_catalog, target_schema, target_table, target_column,
    transform_type, function_name, partial, source_catalog, source_schema, source_table, source_column,
    source_kind, source_model_name, sensitivity_status, sensitivity_partial, sensitivity_reasons_json, created_at
)
SELECT
    id, build_id, compilation_id, project_name, model_name, target_catalog, target_schema, target_table, target_column,
    transform_type, function_name, partial, source_catalog, source_schema, source_table, source_column,
    source_kind, source_model_name, sensitivity_status, sensitivity_partial, sensitivity_reasons_json, created_at
FROM compiled_column_lineage
WHERE build_id IS NOT NULL;

DROP INDEX IF EXISTS idx_compiled_column_lineage_compilation_source;
DROP INDEX IF EXISTS idx_compiled_column_lineage_compilation;
DROP INDEX IF EXISTS idx_compiled_column_lineage_source;
DROP INDEX IF EXISTS idx_compiled_column_lineage_build;

DROP TABLE compiled_column_lineage;
ALTER TABLE compiled_column_lineage_old RENAME TO compiled_column_lineage;

CREATE INDEX idx_compiled_column_lineage_build
    ON compiled_column_lineage(build_id, model_name, target_column);
CREATE INDEX idx_compiled_column_lineage_source
    ON compiled_column_lineage(build_id, source_schema, source_table, source_column);
