-- name: CreateAPIKey :one
INSERT INTO api_keys (id, key_hash, key_prefix, principal_id, name, expires_at)
VALUES (?, ?, ?, ?, ?, ?)
RETURNING *;

-- name: GetAPIKeyByHash :one
SELECT ak.*, p.name as principal_name FROM api_keys ak
JOIN principals p ON ak.principal_id = p.id
WHERE ak.key_hash = ? AND (ak.expires_at IS NULL OR ak.expires_at > datetime('now', 'localtime'));

-- name: ListAPIKeysForPrincipal :many
SELECT * FROM api_keys WHERE principal_id = ? ORDER BY created_at DESC;

-- name: CountAPIKeysForPrincipal :one
SELECT COUNT(*) as cnt FROM api_keys WHERE principal_id = ?;

-- name: ListAPIKeysForPrincipalPaginated :many
SELECT * FROM api_keys WHERE principal_id = ? ORDER BY created_at DESC LIMIT ? OFFSET ?;

-- name: DeleteAPIKey :exec
DELETE FROM api_keys WHERE id = ?;

-- name: GetAPIKeyByID :one
SELECT * FROM api_keys WHERE id = ?;

-- name: ListAllAPIKeysPaginated :many
SELECT * FROM api_keys ORDER BY created_at DESC LIMIT ? OFFSET ?;

-- name: CountAllAPIKeys :one
SELECT COUNT(*) as cnt FROM api_keys;

-- name: DeleteExpiredKeys :execresult
DELETE FROM api_keys WHERE expires_at IS NOT NULL AND expires_at <= datetime('now', 'localtime');
