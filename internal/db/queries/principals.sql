-- name: CreatePrincipal :one
INSERT INTO principals (name, type, is_admin)
VALUES (?, ?, ?)
RETURNING *;

-- name: GetPrincipal :one
SELECT * FROM principals WHERE id = ?;

-- name: GetPrincipalByName :one
SELECT * FROM principals WHERE name = ?;

-- name: ListPrincipals :many
SELECT * FROM principals ORDER BY name;

-- name: DeletePrincipal :exec
DELETE FROM principals WHERE id = ?;

-- name: SetAdmin :exec
UPDATE principals SET is_admin = ? WHERE id = ?;
