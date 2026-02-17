-- name: CreateMacro :one
INSERT INTO macros (id, name, macro_type, parameters, body, description, catalog_name, project_name, visibility, owner, properties, tags, status, created_by)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
RETURNING *;

-- name: GetMacroByName :one
SELECT * FROM macros WHERE name = ?;

-- name: ListMacros :many
SELECT * FROM macros ORDER BY name LIMIT ? OFFSET ?;

-- name: CountMacros :one
SELECT COUNT(*) FROM macros;

-- name: ListAllMacros :many
SELECT * FROM macros ORDER BY name;

-- name: UpdateMacro :exec
UPDATE macros
SET body = ?,
    description = ?,
    parameters = ?,
    status = ?,
    catalog_name = ?,
    project_name = ?,
    visibility = ?,
    owner = ?,
    properties = ?,
    tags = ?,
    updated_at = datetime('now')
WHERE name = ?;

-- name: DeleteMacro :exec
DELETE FROM macros WHERE name = ?;

-- name: CreateMacroRevision :one
INSERT INTO macro_revisions (id, macro_id, macro_name, version, content_hash, parameters, body, description, status, created_by)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
RETURNING *;

-- name: ListMacroRevisions :many
SELECT * FROM macro_revisions WHERE macro_name = ? ORDER BY version DESC;

-- name: GetMacroRevisionByVersion :one
SELECT * FROM macro_revisions WHERE macro_name = ? AND version = ?;

-- name: GetLatestMacroRevisionVersion :one
SELECT CAST(COALESCE(MAX(version), 0) AS INTEGER) FROM macro_revisions WHERE macro_id = ?;
