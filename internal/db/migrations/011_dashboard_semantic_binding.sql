-- +goose Up
ALTER TABLE dashboards ADD COLUMN semantic_project_name TEXT NOT NULL DEFAULT '';
ALTER TABLE dashboards ADD COLUMN semantic_model_name TEXT NOT NULL DEFAULT '';

-- +goose Down
CREATE TABLE dashboards__rollback (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    owner TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

INSERT INTO dashboards__rollback (id, name, description, owner, created_at, updated_at)
SELECT id, name, description, owner, created_at, updated_at
FROM dashboards;

DROP TABLE dashboards;
ALTER TABLE dashboards__rollback RENAME TO dashboards;
CREATE INDEX idx_dashboards_owner ON dashboards(owner);
