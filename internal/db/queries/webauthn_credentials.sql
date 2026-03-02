-- name: CreateWebauthnCredential :one
INSERT INTO webauthn_credentials (
  id, principal_id, credential_id, public_key, sign_count, transports, backup_eligible, backup_state
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
RETURNING *;

-- name: ListWebauthnCredentialsByPrincipal :many
SELECT *
FROM webauthn_credentials
WHERE principal_id = ?
ORDER BY created_at DESC;

-- name: GetWebauthnCredentialByCredentialID :one
SELECT *
FROM webauthn_credentials
WHERE credential_id = ?
LIMIT 1;

-- name: UpdateWebauthnCredentialCounter :exec
UPDATE webauthn_credentials
SET sign_count = ?, last_used_at = CURRENT_TIMESTAMP
WHERE credential_id = ?;

-- name: DeleteWebauthnCredential :exec
DELETE FROM webauthn_credentials
WHERE id = ?;
