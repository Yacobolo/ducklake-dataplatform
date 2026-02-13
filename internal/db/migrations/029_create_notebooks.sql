-- +goose Up
CREATE TABLE notebooks (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    description TEXT,
    owner       TEXT NOT NULL,
    created_at  TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_notebooks_owner ON notebooks(owner);

CREATE TABLE cells (
    id          TEXT PRIMARY KEY,
    notebook_id TEXT NOT NULL REFERENCES notebooks(id) ON DELETE CASCADE,
    cell_type   TEXT NOT NULL CHECK (cell_type IN ('sql', 'markdown')),
    content     TEXT NOT NULL DEFAULT '',
    position    INTEGER NOT NULL DEFAULT 0,
    last_result TEXT,
    created_at  TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_cells_notebook ON cells(notebook_id, position);

-- +goose Down
DROP TABLE IF EXISTS cells;
DROP TABLE IF EXISTS notebooks;
