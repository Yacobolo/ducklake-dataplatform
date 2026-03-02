-- name: CreateAuthRecoveryCode :one
INSERT INTO auth_recovery_codes (
  id, principal_id, code_hash, expires_at
)
VALUES (?, ?, ?, ?)
RETURNING *;

-- name: ListAuthRecoveryCodesByPrincipal :many
SELECT *
FROM auth_recovery_codes
WHERE principal_id = ?
ORDER BY created_at DESC;

-- name: GetUnusedAuthRecoveryCodeByHash :one
SELECT *
FROM auth_recovery_codes
WHERE code_hash = ?
  AND used_at IS NULL
  AND expires_at > CURRENT_TIMESTAMP
LIMIT 1;

-- name: MarkAuthRecoveryCodeUsed :exec
UPDATE auth_recovery_codes
SET used_at = CURRENT_TIMESTAMP
WHERE id = ?;

-- name: DeleteExpiredAuthRecoveryCodes :execrows
DELETE FROM auth_recovery_codes
WHERE used_at IS NOT NULL OR expires_at <= CURRENT_TIMESTAMP;
