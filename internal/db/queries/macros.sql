-- name: CreateMacro :one
INSERT INTO macros (id, name, macro_type, parameters, body, description, created_by)
VALUES (?, ?, ?, ?, ?, ?, ?)
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
SET body = COALESCE(?, body),
    description = COALESCE(?, description),
    parameters = COALESCE(?, parameters),
    updated_at = datetime('now')
WHERE name = ?;

-- name: DeleteMacro :exec
DELETE FROM macros WHERE name = ?;
