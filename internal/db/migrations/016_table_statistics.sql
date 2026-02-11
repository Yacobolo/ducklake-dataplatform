-- +goose Up
CREATE TABLE table_statistics (
    table_securable_name TEXT NOT NULL UNIQUE,
    row_count            INTEGER,
    size_bytes           INTEGER,
    column_count         INTEGER,
    last_profiled_at     TEXT DEFAULT (datetime('now')),
    profiled_by          TEXT
);

-- +goose Down
DROP TABLE IF EXISTS table_statistics;
