-- +goose Up
CREATE TABLE row_filters (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    table_id INTEGER NOT NULL,
    filter_sql TEXT NOT NULL,
    description TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(table_id)
);

CREATE TABLE row_filter_bindings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    row_filter_id INTEGER NOT NULL REFERENCES row_filters(id) ON DELETE CASCADE,
    principal_id INTEGER NOT NULL,
    principal_type TEXT NOT NULL,
    UNIQUE(row_filter_id, principal_id, principal_type)
);

-- +goose Down
DROP TABLE row_filter_bindings;
DROP TABLE row_filters;
