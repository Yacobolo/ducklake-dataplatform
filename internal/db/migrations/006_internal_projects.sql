-- +goose Up
CREATE TABLE projects (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    kind TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    owner_team_id TEXT REFERENCES teams(id) ON DELETE RESTRICT,
    owner_principal TEXT,
    product_id TEXT REFERENCES data_products(id) ON DELETE SET NULL,
    default_branch TEXT NOT NULL DEFAULT 'main',
    created_by TEXT NOT NULL DEFAULT '',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE environments (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    kind TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    target_catalog TEXT NOT NULL,
    target_schema TEXT NOT NULL,
    compute_endpoint TEXT,
    defer_to_environment TEXT,
    variables_json TEXT NOT NULL DEFAULT '{}',
    source_overrides_json TEXT NOT NULL DEFAULT '{}',
    created_by TEXT NOT NULL DEFAULT '',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(project_id, name)
);

CREATE INDEX idx_projects_owner_team ON projects(owner_team_id);
CREATE INDEX idx_projects_product ON projects(product_id);
CREATE INDEX idx_environments_project ON environments(project_id);

-- +goose Down
DROP INDEX IF EXISTS idx_environments_project;
DROP INDEX IF EXISTS idx_projects_product;
DROP INDEX IF EXISTS idx_projects_owner_team;
DROP TABLE IF EXISTS environments;
DROP TABLE IF EXISTS projects;
