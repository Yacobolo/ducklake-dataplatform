-- +goose Up
ALTER TABLE macros ADD COLUMN catalog_name TEXT NOT NULL DEFAULT '';
ALTER TABLE macros ADD COLUMN project_name TEXT NOT NULL DEFAULT '';
ALTER TABLE macros ADD COLUMN visibility TEXT NOT NULL DEFAULT 'project';
ALTER TABLE macros ADD COLUMN owner TEXT NOT NULL DEFAULT '';
ALTER TABLE macros ADD COLUMN properties TEXT NOT NULL DEFAULT '{}';
ALTER TABLE macros ADD COLUMN tags TEXT NOT NULL DEFAULT '[]';
ALTER TABLE macros ADD COLUMN status TEXT NOT NULL DEFAULT 'ACTIVE';

CREATE INDEX IF NOT EXISTS idx_macros_visibility ON macros(visibility);
CREATE INDEX IF NOT EXISTS idx_macros_project ON macros(project_name);

-- +goose Down
