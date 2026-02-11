-- +goose Up
CREATE TABLE column_metadata (
    table_securable_name TEXT NOT NULL,
    column_name          TEXT NOT NULL,
    comment              TEXT,
    properties           TEXT,
    updated_at           TEXT DEFAULT (datetime('now')),
    UNIQUE(table_securable_name, column_name)
);

-- +goose Down
DROP TABLE IF EXISTS column_metadata;
