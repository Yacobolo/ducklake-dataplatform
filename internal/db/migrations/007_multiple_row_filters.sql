-- +goose Up
-- SQLite doesn't support DROP CONSTRAINT, so recreate the table without UNIQUE(table_id)
CREATE TABLE row_filters_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    table_id INTEGER NOT NULL,
    filter_sql TEXT NOT NULL,
    description TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

INSERT INTO row_filters_new SELECT * FROM row_filters;
DROP TABLE row_filters;
ALTER TABLE row_filters_new RENAME TO row_filters;

-- +goose Down
CREATE UNIQUE INDEX IF NOT EXISTS idx_row_filters_table ON row_filters(table_id);
