-- name: CreatePrincipal :one
INSERT INTO principals (id, name, type, is_admin)
VALUES (?, ?, ?, ?)
RETURNING *;

-- name: CreatePrincipalWithExternalID :one
INSERT INTO principals (id, name, type, is_admin, external_id, external_issuer)
VALUES (?, ?, ?, ?, ?, ?)
RETURNING *;

-- name: GetPrincipal :one
SELECT * FROM principals WHERE id = ?;

-- name: GetPrincipalByName :one
SELECT * FROM principals WHERE name = ?;

-- name: GetPrincipalByExternalID :one
SELECT * FROM principals
WHERE external_issuer = ? AND external_id = ?
LIMIT 1;

-- name: ListPrincipals :many
SELECT * FROM principals ORDER BY name;

-- name: DeletePrincipal :exec
DELETE FROM principals WHERE id = ?;

-- name: SetAdmin :exec
UPDATE principals SET is_admin = ? WHERE id = ?;

-- name: BindExternalID :exec
UPDATE principals SET external_id = ?, external_issuer = ? WHERE id = ?;

-- name: CountPrincipals :one
SELECT COUNT(*) as cnt FROM principals;

-- name: ListPrincipalsPaginated :many
SELECT * FROM principals ORDER BY id LIMIT ? OFFSET ?;
