-- +goose Up
CREATE TABLE IF NOT EXISTS external_tables (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    schema_name   TEXT NOT NULL,
    table_name    TEXT NOT NULL,
    file_format   TEXT NOT NULL DEFAULT 'parquet',
    source_path   TEXT NOT NULL,
    location_name TEXT NOT NULL,
    comment       TEXT NOT NULL DEFAULT '',
    owner         TEXT NOT NULL DEFAULT '',
    created_at    TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at    TEXT NOT NULL DEFAULT (datetime('now')),
    deleted_at    TEXT,
    UNIQUE(schema_name, table_name)
);

CREATE TABLE IF NOT EXISTS external_table_columns (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    external_table_id INTEGER NOT NULL REFERENCES external_tables(id) ON DELETE CASCADE,
    column_name       TEXT NOT NULL,
    column_type       TEXT NOT NULL,
    position          INTEGER NOT NULL,
    UNIQUE(external_table_id, column_name)
);

-- +goose Down
DROP TABLE IF EXISTS external_table_columns;
DROP TABLE IF EXISTS external_tables;
