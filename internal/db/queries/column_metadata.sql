-- name: GetColumnMetadata :one
SELECT * FROM column_metadata
WHERE table_securable_name = ? AND column_name = ?;

-- name: UpsertColumnMetadata :exec
INSERT INTO column_metadata (table_securable_name, column_name, comment, properties)
VALUES (?, ?, ?, ?)
ON CONFLICT(table_securable_name, column_name)
DO UPDATE SET comment = COALESCE(excluded.comment, comment),
              properties = COALESCE(excluded.properties, properties),
              updated_at = datetime('now');

-- name: DeleteColumnMetadataByTable :exec
DELETE FROM column_metadata WHERE table_securable_name = ?;

-- name: DeleteColumnMetadataByTablePattern :exec
DELETE FROM column_metadata WHERE table_securable_name LIKE ?;
