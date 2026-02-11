-- +goose Up
CREATE TABLE IF NOT EXISTS lineage_edges (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source_table    TEXT NOT NULL,
    target_table    TEXT,
    edge_type       TEXT NOT NULL,
    principal_name  TEXT NOT NULL,
    query_hash      TEXT,
    created_at      TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX idx_lineage_source ON lineage_edges(source_table);
CREATE INDEX idx_lineage_target ON lineage_edges(target_table);
CREATE INDEX idx_lineage_created ON lineage_edges(created_at);

-- +goose Down
DROP TABLE IF EXISTS lineage_edges;
