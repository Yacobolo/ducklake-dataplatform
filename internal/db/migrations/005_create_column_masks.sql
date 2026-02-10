-- +goose Up
CREATE TABLE column_masks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    table_id INTEGER NOT NULL,
    column_name TEXT NOT NULL,
    mask_expression TEXT NOT NULL,
    description TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(table_id, column_name)
);

CREATE TABLE column_mask_bindings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    column_mask_id INTEGER NOT NULL REFERENCES column_masks(id) ON DELETE CASCADE,
    principal_id INTEGER NOT NULL,
    principal_type TEXT NOT NULL,
    see_original INTEGER NOT NULL DEFAULT 0,
    UNIQUE(column_mask_id, principal_id, principal_type)
);

-- +goose Down
DROP TABLE column_mask_bindings;
DROP TABLE column_masks;
