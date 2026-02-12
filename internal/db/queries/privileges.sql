-- name: GrantPrivilege :one
INSERT INTO privilege_grants (principal_id, principal_type, securable_type, securable_id, privilege, granted_by)
VALUES (?, ?, ?, ?, ?, ?)
RETURNING *;

-- name: RevokePrivilege :exec
DELETE FROM privilege_grants
WHERE principal_id = ? AND principal_type = ? AND securable_type = ? AND securable_id = ? AND privilege = ?;

-- name: RevokePrivilegeByID :exec
DELETE FROM privilege_grants WHERE id = ?;

-- name: ListGrantsForPrincipal :many
SELECT * FROM privilege_grants
WHERE principal_id = ? AND principal_type = ?;

-- name: ListGrantsForSecurable :many
SELECT * FROM privilege_grants
WHERE securable_type = ? AND securable_id = ?;

-- name: CheckDirectGrant :one
SELECT COUNT(*) as cnt FROM privilege_grants
WHERE principal_id = ? AND principal_type = ? AND securable_type = ? AND securable_id = ? AND privilege = ?;

-- name: CheckDirectGrantAny :one
SELECT COUNT(*) as cnt FROM privilege_grants
WHERE principal_id = ? AND principal_type = ? AND securable_type = ? AND securable_id = ?
  AND privilege IN ('ALL_PRIVILEGES', ?);

-- name: ListGrantsForPrincipalOnSecurable :many
SELECT * FROM privilege_grants
WHERE principal_id = ? AND principal_type = ? AND securable_type = ? AND securable_id = ?;

-- name: ListAllGrantsForIdentities :many
SELECT * FROM privilege_grants
WHERE (principal_type = 'user' AND principal_id = ?)
   OR (principal_type = 'group' AND principal_id IN (
       SELECT group_id FROM group_members WHERE member_type = 'user' AND member_id = ?
   ));

-- name: CountGrantsForPrincipal :one
SELECT COUNT(*) as cnt FROM privilege_grants
WHERE principal_id = ? AND principal_type = ?;

-- name: ListGrantsForPrincipalPaginated :many
SELECT * FROM privilege_grants
WHERE principal_id = ? AND principal_type = ?
ORDER BY id LIMIT ? OFFSET ?;

-- name: CountGrantsForSecurable :one
SELECT COUNT(*) as cnt FROM privilege_grants
WHERE securable_type = ? AND securable_id = ?;

-- name: ListGrantsForSecurablePaginated :many
SELECT * FROM privilege_grants
WHERE securable_type = ? AND securable_id = ?
ORDER BY id LIMIT ? OFFSET ?;
