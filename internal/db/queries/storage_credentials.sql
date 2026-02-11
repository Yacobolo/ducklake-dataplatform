-- name: CreateStorageCredential :one
INSERT INTO storage_credentials (
    name, credential_type,
    key_id_encrypted, secret_encrypted, endpoint, region, url_style,
    azure_account_name, azure_account_key_encrypted, azure_client_id, azure_tenant_id, azure_client_secret_encrypted,
    gcs_key_file_path,
    comment, owner
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
RETURNING *;

-- name: GetStorageCredential :one
SELECT * FROM storage_credentials WHERE id = ?;

-- name: GetStorageCredentialByName :one
SELECT * FROM storage_credentials WHERE name = ?;

-- name: ListStorageCredentials :many
SELECT * FROM storage_credentials ORDER BY name LIMIT ? OFFSET ?;

-- name: CountStorageCredentials :one
SELECT COUNT(*) FROM storage_credentials;

-- name: UpdateStorageCredential :exec
UPDATE storage_credentials
SET key_id_encrypted = COALESCE(?, key_id_encrypted),
    secret_encrypted = COALESCE(?, secret_encrypted),
    endpoint = COALESCE(?, endpoint),
    region = COALESCE(?, region),
    url_style = COALESCE(?, url_style),
    azure_account_name = COALESCE(?, azure_account_name),
    azure_account_key_encrypted = COALESCE(?, azure_account_key_encrypted),
    azure_client_id = COALESCE(?, azure_client_id),
    azure_tenant_id = COALESCE(?, azure_tenant_id),
    azure_client_secret_encrypted = COALESCE(?, azure_client_secret_encrypted),
    gcs_key_file_path = COALESCE(?, gcs_key_file_path),
    comment = COALESCE(?, comment),
    updated_at = datetime('now')
WHERE id = ?;

-- name: DeleteStorageCredential :exec
DELETE FROM storage_credentials WHERE id = ?;
