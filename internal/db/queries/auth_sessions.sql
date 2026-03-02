-- name: CreateAuthSession :one
INSERT INTO auth_sessions (
  id, principal_id, session_hash, auth_method, user_agent, ip_address,
  expires_at, idle_expires_at, last_seen_at
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
RETURNING *;

-- name: GetActiveAuthSessionByHash :one
SELECT *
FROM auth_sessions
WHERE session_hash = ?
  AND revoked_at IS NULL
  AND expires_at > CURRENT_TIMESTAMP
  AND idle_expires_at > CURRENT_TIMESTAMP
LIMIT 1;

-- name: TouchAuthSession :exec
UPDATE auth_sessions
SET idle_expires_at = ?, last_seen_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
WHERE id = ?;

-- name: RevokeAuthSession :exec
UPDATE auth_sessions
SET revoked_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
WHERE id = ?;

-- name: RevokeAuthSessionByHash :exec
UPDATE auth_sessions
SET revoked_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
WHERE session_hash = ?;

-- name: DeleteExpiredOrRevokedAuthSessions :execrows
DELETE FROM auth_sessions
WHERE revoked_at IS NOT NULL
   OR expires_at <= CURRENT_TIMESTAMP
   OR idle_expires_at <= CURRENT_TIMESTAMP;
