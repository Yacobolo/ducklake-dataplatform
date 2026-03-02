-- name: CreateAuthIdentity :one
INSERT INTO auth_identities (
  id, principal_id, provider, issuer, subject, email, email_verified
)
VALUES (?, ?, ?, ?, ?, ?, ?)
RETURNING *;

-- name: GetAuthIdentityByProviderSubject :one
SELECT *
FROM auth_identities
WHERE provider = ? AND issuer IS ? AND subject = ?
LIMIT 1;

-- name: ListAuthIdentitiesByPrincipal :many
SELECT *
FROM auth_identities
WHERE principal_id = ?
ORDER BY created_at DESC;

-- name: DeleteAuthIdentity :exec
DELETE FROM auth_identities
WHERE id = ?;
