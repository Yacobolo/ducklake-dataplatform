-- +goose Up
CREATE TABLE column_lineage_edges (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    lineage_edge_id TEXT NOT NULL REFERENCES lineage_edges(id) ON DELETE CASCADE,
    target_column   TEXT NOT NULL,
    source_schema   TEXT NOT NULL,
    source_table    TEXT NOT NULL,
    source_column   TEXT NOT NULL,
    transform_type  TEXT NOT NULL CHECK (transform_type IN ('DIRECT', 'EXPRESSION')),
    function_name   TEXT NOT NULL DEFAULT '',
    created_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_col_lineage_edge_id ON column_lineage_edges(lineage_edge_id);
CREATE INDEX idx_col_lineage_source ON column_lineage_edges(source_schema, source_table, source_column);

-- +goose Down
DROP TABLE IF EXISTS column_lineage_edges;
