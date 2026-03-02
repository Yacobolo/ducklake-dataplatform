-- name: InsertAuthLoginAttempt :exec
INSERT INTO auth_login_attempts (id, username, ip_address, success, reason)
VALUES (?, ?, ?, ?, ?);

-- name: CountRecentFailedAuthLoginAttemptsByUsername :one
SELECT COUNT(*)
FROM auth_login_attempts
WHERE username = ?
  AND success = 0
  AND created_at >= ?;

-- name: CountRecentFailedAuthLoginAttemptsByIP :one
SELECT COUNT(*)
FROM auth_login_attempts
WHERE ip_address = ?
  AND success = 0
  AND created_at >= ?;
