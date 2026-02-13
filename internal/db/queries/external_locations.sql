-- name: CreateExternalLocation :one
INSERT INTO external_locations (id, name, url, credential_name, storage_type, comment, owner, read_only)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
RETURNING *;

-- name: GetExternalLocation :one
SELECT * FROM external_locations WHERE id = ?;

-- name: GetExternalLocationByName :one
SELECT * FROM external_locations WHERE name = ?;

-- name: ListExternalLocations :many
SELECT * FROM external_locations ORDER BY name LIMIT ? OFFSET ?;

-- name: CountExternalLocations :one
SELECT COUNT(*) FROM external_locations;

-- name: UpdateExternalLocation :exec
UPDATE external_locations
SET url = COALESCE(?, url),
    credential_name = COALESCE(?, credential_name),
    comment = COALESCE(?, comment),
    owner = COALESCE(?, owner),
    read_only = COALESCE(?, read_only),
    updated_at = datetime('now')
WHERE id = ?;

-- name: DeleteExternalLocation :exec
DELETE FROM external_locations WHERE id = ?;
