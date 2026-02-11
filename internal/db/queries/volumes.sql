-- name: CreateVolume :one
INSERT INTO volumes (name, schema_name, catalog_name, volume_type, storage_location, comment, owner)
VALUES (?, ?, ?, ?, ?, ?, ?)
RETURNING *;

-- name: GetVolumeByName :one
SELECT * FROM volumes WHERE schema_name = ? AND name = ?;

-- name: ListVolumes :many
SELECT * FROM volumes WHERE schema_name = ? ORDER BY name LIMIT ? OFFSET ?;

-- name: CountVolumes :one
SELECT COUNT(*) FROM volumes WHERE schema_name = ?;

-- name: UpdateVolume :exec
UPDATE volumes
SET name = COALESCE(?, name),
    comment = COALESCE(?, comment),
    owner = COALESCE(?, owner),
    updated_at = datetime('now')
WHERE id = ?;

-- name: DeleteVolume :exec
DELETE FROM volumes WHERE id = ?;

-- name: DeleteVolumesBySchema :exec
DELETE FROM volumes WHERE schema_name = ?;
