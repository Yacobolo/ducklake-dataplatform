-- name: GetCatalogMetadata :one
SELECT * FROM catalog_metadata
WHERE securable_type = ? AND securable_name = ?;

-- name: UpsertCatalogMetadata :exec
INSERT INTO catalog_metadata (securable_type, securable_name, comment, properties, owner)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT(securable_type, securable_name)
DO UPDATE SET comment = COALESCE(excluded.comment, comment),
              properties = COALESCE(excluded.properties, properties),
              owner = COALESCE(excluded.owner, owner),
              deleted_at = NULL,
              updated_at = datetime('now');

-- name: InsertOrReplaceCatalogMetadata :exec
INSERT OR REPLACE INTO catalog_metadata (securable_type, securable_name, comment, owner)
VALUES (?, ?, ?, ?);

-- name: SoftDeleteCatalogMetadata :exec
UPDATE catalog_metadata SET deleted_at = datetime('now')
WHERE securable_type = ? AND securable_name = ?;

-- name: SoftDeleteCatalogMetadataByPattern :exec
UPDATE catalog_metadata SET deleted_at = datetime('now')
WHERE securable_type = ? AND securable_name LIKE ?;

-- name: DeleteCatalogMetadataByTypeAndName :exec
DELETE FROM catalog_metadata
WHERE securable_type = ? AND securable_name = ?;

-- name: DeleteCatalogMetadataByTypeAndPattern :exec
DELETE FROM catalog_metadata
WHERE securable_type = ? AND securable_name LIKE ?;
