-- +goose Up
CREATE TABLE macros (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    macro_type TEXT NOT NULL CHECK (macro_type IN ('SCALAR','TABLE')),
    parameters TEXT NOT NULL DEFAULT '[]',
    body TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    created_by TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- +goose Down
DROP TABLE IF EXISTS macros;
