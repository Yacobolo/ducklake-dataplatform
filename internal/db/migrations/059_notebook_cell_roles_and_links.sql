-- +goose Up
ALTER TABLE cells ADD COLUMN name TEXT;
ALTER TABLE cells ADD COLUMN role TEXT NOT NULL DEFAULT 'transform' CHECK (role IN ('transform','output','test','markdown'));
ALTER TABLE cells ADD COLUMN disabled INTEGER NOT NULL DEFAULT 0 CHECK (disabled IN (0,1));
ALTER TABLE cells ADD COLUMN test_config TEXT NOT NULL DEFAULT '{}';

UPDATE cells
SET role = CASE
    WHEN cell_type = 'markdown' THEN 'markdown'
    ELSE 'transform'
END
WHERE role = 'transform';

CREATE UNIQUE INDEX idx_cells_notebook_name_unique
ON cells(notebook_id, name)
WHERE name IS NOT NULL AND name <> '';

CREATE UNIQUE INDEX idx_cells_notebook_output_unique
ON cells(notebook_id)
WHERE role = 'output';

CREATE TABLE notebook_model_links (
    id TEXT PRIMARY KEY,
    notebook_id TEXT NOT NULL REFERENCES notebooks(id) ON DELETE CASCADE,
    model_id TEXT NOT NULL REFERENCES models(id) ON DELETE CASCADE,
    output_cell_id TEXT NOT NULL REFERENCES cells(id) ON DELETE RESTRICT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(notebook_id),
    UNIQUE(model_id)
);

CREATE INDEX idx_notebook_model_links_notebook_id ON notebook_model_links(notebook_id);
CREATE INDEX idx_notebook_model_links_model_id ON notebook_model_links(model_id);

-- +goose Down
DROP INDEX IF EXISTS idx_notebook_model_links_model_id;
DROP INDEX IF EXISTS idx_notebook_model_links_notebook_id;
DROP TABLE IF EXISTS notebook_model_links;
DROP INDEX IF EXISTS idx_cells_notebook_output_unique;
DROP INDEX IF EXISTS idx_cells_notebook_name_unique;

-- SQLite does not support dropping columns directly; keep columns in place on down migration.
