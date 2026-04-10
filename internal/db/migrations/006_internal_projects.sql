-- +goose Up
CREATE TABLE workspaces (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    kind TEXT NOT NULL,
    owner_team_id TEXT REFERENCES teams(id) ON DELETE RESTRICT,
    owner_principal TEXT,
    default_project_id TEXT,
    default_environment_id TEXT,
    git_repo_id TEXT REFERENCES git_repos(id) ON DELETE SET NULL,
    git_root_path TEXT,
    created_by TEXT NOT NULL DEFAULT '',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE workspace_members (
    workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    principal_name TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'viewer' CHECK (role IN ('viewer', 'editor', 'manager')),
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (workspace_id, principal_name)
);

CREATE TABLE projects (
    id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
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

CREATE INDEX idx_workspaces_owner_team ON workspaces(owner_team_id);
CREATE INDEX idx_workspaces_owner_principal ON workspaces(owner_principal);
CREATE INDEX idx_workspace_members_principal ON workspace_members(principal_name);
CREATE INDEX idx_projects_workspace ON projects(workspace_id);
CREATE INDEX idx_projects_owner_team ON projects(owner_team_id);
CREATE INDEX idx_projects_product ON projects(product_id);
CREATE INDEX idx_environments_project ON environments(project_id);

-- +goose Down
DROP INDEX IF EXISTS idx_environments_project;
DROP INDEX IF EXISTS idx_projects_workspace;
DROP INDEX IF EXISTS idx_projects_product;
DROP INDEX IF EXISTS idx_projects_owner_team;
DROP INDEX IF EXISTS idx_workspace_members_principal;
DROP INDEX IF EXISTS idx_workspaces_owner_principal;
DROP INDEX IF EXISTS idx_workspaces_owner_team;
DROP TABLE IF EXISTS environments;
DROP TABLE IF EXISTS projects;
DROP TABLE IF EXISTS workspace_members;
DROP TABLE IF EXISTS workspaces;
