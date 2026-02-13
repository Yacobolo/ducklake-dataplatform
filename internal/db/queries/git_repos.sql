-- name: CreateGitRepo :one
INSERT INTO git_repos (id, url, branch, path, auth_token, webhook_secret, owner)
VALUES (?, ?, ?, ?, ?, ?, ?)
RETURNING *;

-- name: GetGitRepo :one
SELECT * FROM git_repos WHERE id = ?;

-- name: ListGitRepos :many
SELECT * FROM git_repos ORDER BY created_at DESC LIMIT ? OFFSET ?;

-- name: CountGitRepos :one
SELECT COUNT(*) FROM git_repos;

-- name: DeleteGitRepo :exec
DELETE FROM git_repos WHERE id = ?;

-- name: UpdateGitRepoSyncStatus :exec
UPDATE git_repos
SET last_commit = ?, last_sync_at = ?, updated_at = datetime('now')
WHERE id = ?;
