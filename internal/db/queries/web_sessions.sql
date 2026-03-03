-- name: CreateWebSession :one
INSERT INTO web_sessions (
  id, principal_id, session_hash, auth_method, user_agent, ip_address,
  expires_at, idle_expires_at, last_seen_at
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
RETURNING *;

-- name: GetActiveWebSessionByHash :one
SELECT *
FROM web_sessions
WHERE session_hash = ?
  AND revoked_at IS NULL
  AND expires_at > CURRENT_TIMESTAMP
  AND idle_expires_at > CURRENT_TIMESTAMP
LIMIT 1;

-- name: TouchWebSession :exec
UPDATE web_sessions
SET idle_expires_at = ?, last_seen_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
WHERE id = ?;

-- name: RevokeWebSession :exec
UPDATE web_sessions
SET revoked_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
WHERE id = ?;

-- name: RevokeWebSessionByHash :exec
UPDATE web_sessions
SET revoked_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
WHERE session_hash = ?;

-- name: RevokeWebSessionsByPrincipal :exec
UPDATE web_sessions
SET revoked_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
WHERE principal_id = ?
  AND revoked_at IS NULL;

-- name: DeleteExpiredOrRevokedWebSessions :execrows
DELETE FROM web_sessions
WHERE revoked_at IS NOT NULL
   OR expires_at <= CURRENT_TIMESTAMP
   OR idle_expires_at <= CURRENT_TIMESTAMP;
