-- name: CreateNotebookJob :one
INSERT INTO notebook_jobs (id, notebook_id, session_id, state)
VALUES (?, ?, ?, ?)
RETURNING *;

-- name: GetNotebookJob :one
SELECT * FROM notebook_jobs WHERE id = ?;

-- name: ListNotebookJobs :many
SELECT * FROM notebook_jobs
WHERE notebook_id = ?
ORDER BY created_at DESC
LIMIT ? OFFSET ?;

-- name: CountNotebookJobs :one
SELECT COUNT(*) FROM notebook_jobs WHERE notebook_id = ?;

-- name: UpdateNotebookJobState :exec
UPDATE notebook_jobs
SET state = ?, result = ?, error = ?, updated_at = datetime('now')
WHERE id = ?;
