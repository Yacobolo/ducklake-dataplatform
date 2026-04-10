-- +goose Up
ALTER TABLE dashboards ADD COLUMN folder_id TEXT REFERENCES folders(id) ON DELETE SET NULL;
ALTER TABLE pipelines ADD COLUMN folder_id TEXT REFERENCES folders(id) ON DELETE SET NULL;

UPDATE dashboards
SET folder_id = (
    SELECT f.id
    FROM folders f
    JOIN workspaces w ON w.id = f.workspace_id
    WHERE f.owner = dashboards.owner
      AND f.system_role = 'WORKSPACE_ROOT'
      AND w.kind = 'personal'
      AND w.owner_principal = dashboards.owner
    LIMIT 1
)
WHERE folder_id IS NULL;

UPDATE pipelines
SET folder_id = (
    SELECT f.id
    FROM folders f
    JOIN workspaces w ON w.id = f.workspace_id
    WHERE f.owner = pipelines.created_by
      AND f.system_role = 'WORKSPACE_ROOT'
      AND w.kind = 'personal'
      AND w.owner_principal = pipelines.created_by
    LIMIT 1
)
WHERE folder_id IS NULL;

INSERT OR IGNORE INTO privilege_grants (
    id,
    principal_id,
    principal_type,
    securable_type,
    securable_id,
    privilege,
    granted_by
)
SELECT
    lower(hex(randomblob(16))),
    fs.principal_name,
    'user',
    'folder',
    fs.folder_id,
    CASE fs.role
        WHEN 'viewer' THEN 'SELECT'
        WHEN 'editor' THEN 'MODIFY'
        WHEN 'manager' THEN 'MANAGE'
    END,
    NULL
FROM folder_shares fs
WHERE fs.role IN ('viewer', 'editor', 'manager');

CREATE INDEX idx_dashboards_folder ON dashboards(folder_id);
CREATE INDEX idx_pipelines_folder ON pipelines(folder_id);
CREATE INDEX idx_privilege_grants_folder ON privilege_grants(securable_type, securable_id);

-- +goose Down
DROP INDEX IF EXISTS idx_privilege_grants_folder;
DROP INDEX IF EXISTS idx_pipelines_folder;
DROP INDEX IF EXISTS idx_dashboards_folder;
