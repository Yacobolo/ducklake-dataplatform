-- +goose Up
CREATE TABLE folders (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    owner TEXT NOT NULL,
    parent_folder_id TEXT REFERENCES folders(id) ON DELETE CASCADE,
    path TEXT NOT NULL,
    depth INTEGER NOT NULL DEFAULT 0,
    system_role TEXT,
    git_repo_id TEXT REFERENCES git_repos(id) ON DELETE SET NULL,
    git_root_path TEXT,
    default_project_id TEXT REFERENCES projects(id) ON DELETE SET NULL,
    default_environment_id TEXT REFERENCES environments(id) ON DELETE SET NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX idx_folders_path ON folders(path);
CREATE INDEX idx_folders_parent ON folders(parent_folder_id);
CREATE INDEX idx_folders_owner ON folders(owner);
CREATE INDEX idx_folders_git_repo ON folders(git_repo_id);
CREATE INDEX idx_folders_project ON folders(default_project_id);

CREATE TABLE folder_shares (
    id TEXT PRIMARY KEY,
    folder_id TEXT NOT NULL REFERENCES folders(id) ON DELETE CASCADE,
    principal_name TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'viewer' CHECK (role IN ('viewer', 'editor', 'manager')),
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(folder_id, principal_name)
);

CREATE INDEX idx_folder_shares_folder ON folder_shares(folder_id);
CREATE INDEX idx_folder_shares_principal ON folder_shares(principal_name);

CREATE TABLE notebook_shares (
    id TEXT PRIMARY KEY,
    notebook_id TEXT NOT NULL REFERENCES notebooks(id) ON DELETE CASCADE,
    principal_name TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'viewer' CHECK (role IN ('viewer', 'editor', 'manager')),
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(notebook_id, principal_name)
);

CREATE INDEX idx_notebook_shares_notebook ON notebook_shares(notebook_id);
CREATE INDEX idx_notebook_shares_principal ON notebook_shares(principal_name);

ALTER TABLE notebooks ADD COLUMN folder_id TEXT REFERENCES folders(id) ON DELETE SET NULL;
ALTER TABLE notebooks ADD COLUMN project_override_id TEXT REFERENCES projects(id) ON DELETE SET NULL;
ALTER TABLE notebooks ADD COLUMN environment_override_id TEXT REFERENCES environments(id) ON DELETE SET NULL;

WITH distinct_owners AS (
    SELECT DISTINCT owner
    FROM notebooks
    WHERE owner <> ''
),
personal_roots AS (
    SELECT
        lower(hex(randomblob(16))) AS id,
        owner
    FROM distinct_owners
)
INSERT INTO folders (
    id,
    name,
    owner,
    path,
    depth,
    system_role
)
SELECT
    id,
    'My notebooks',
    owner,
    '/' || id,
    0,
    'PERSONAL_ROOT'
FROM personal_roots;

UPDATE notebooks
SET folder_id = (
    SELECT f.id
    FROM folders f
    WHERE f.owner = notebooks.owner
      AND f.system_role = 'PERSONAL_ROOT'
    LIMIT 1
)
WHERE folder_id IS NULL;

CREATE INDEX idx_notebooks_folder ON notebooks(folder_id);
CREATE INDEX idx_notebooks_project_override ON notebooks(project_override_id);
CREATE INDEX idx_notebooks_environment_override ON notebooks(environment_override_id);

-- +goose Down
DROP INDEX IF EXISTS idx_notebooks_environment_override;
DROP INDEX IF EXISTS idx_notebooks_project_override;
DROP INDEX IF EXISTS idx_notebooks_folder;
DROP INDEX IF EXISTS idx_notebook_shares_principal;
DROP INDEX IF EXISTS idx_notebook_shares_notebook;
DROP INDEX IF EXISTS idx_folder_shares_principal;
DROP INDEX IF EXISTS idx_folder_shares_folder;
DROP INDEX IF EXISTS idx_folders_project;
DROP INDEX IF EXISTS idx_folders_git_repo;
DROP INDEX IF EXISTS idx_folders_owner;
DROP INDEX IF EXISTS idx_folders_parent;
DROP INDEX IF EXISTS idx_folders_path;
DROP TABLE IF EXISTS notebook_shares;
DROP TABLE IF EXISTS folder_shares;
DROP TABLE IF EXISTS folders;
