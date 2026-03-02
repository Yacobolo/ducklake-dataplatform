-- name: UpsertLocalCredential :exec
INSERT INTO local_credentials (
  principal_id, username, password_hash, password_changed_at, must_change_password, updated_at
)
VALUES (?, ?, ?, CURRENT_TIMESTAMP, ?, CURRENT_TIMESTAMP)
ON CONFLICT(principal_id) DO UPDATE SET
  username = excluded.username,
  password_hash = excluded.password_hash,
  password_changed_at = CURRENT_TIMESTAMP,
  must_change_password = excluded.must_change_password,
  updated_at = CURRENT_TIMESTAMP;

-- name: GetLocalCredentialByUsername :one
SELECT *
FROM local_credentials
WHERE username = ?
LIMIT 1;

-- name: GetLocalCredentialByPrincipalID :one
SELECT *
FROM local_credentials
WHERE principal_id = ?
LIMIT 1;

-- name: DeleteLocalCredential :exec
DELETE FROM local_credentials
WHERE principal_id = ?;
