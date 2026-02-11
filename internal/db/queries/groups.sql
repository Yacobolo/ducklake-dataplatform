-- name: CreateGroup :one
INSERT INTO groups (name, description)
VALUES (?, ?)
RETURNING *;

-- name: GetGroup :one
SELECT * FROM groups WHERE id = ?;

-- name: GetGroupByName :one
SELECT * FROM groups WHERE name = ?;

-- name: ListGroups :many
SELECT * FROM groups ORDER BY name;

-- name: DeleteGroup :exec
DELETE FROM groups WHERE id = ?;

-- name: AddGroupMember :exec
INSERT OR IGNORE INTO group_members (group_id, member_type, member_id)
VALUES (?, ?, ?);

-- name: RemoveGroupMember :exec
DELETE FROM group_members
WHERE group_id = ? AND member_type = ? AND member_id = ?;

-- name: ListGroupMembers :many
SELECT * FROM group_members WHERE group_id = ?;

-- name: GetGroupsForMember :many
SELECT g.* FROM groups g
JOIN group_members gm ON g.id = gm.group_id
WHERE gm.member_type = ? AND gm.member_id = ?;

-- name: CountGroups :one
SELECT COUNT(*) as cnt FROM groups;

-- name: ListGroupsPaginated :many
SELECT * FROM groups ORDER BY id LIMIT ? OFFSET ?;

-- name: CountGroupMembers :one
SELECT COUNT(*) as cnt FROM group_members WHERE group_id = ?;

-- name: ListGroupMembersPaginated :many
SELECT * FROM group_members WHERE group_id = ? ORDER BY member_id LIMIT ? OFFSET ?;
