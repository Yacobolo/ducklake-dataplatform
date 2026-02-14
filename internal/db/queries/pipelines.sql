-- name: CreatePipeline :one
INSERT INTO pipelines (id, name, description, schedule_cron, is_paused, concurrency_limit, created_by)
VALUES (?, ?, ?, ?, ?, ?, ?)
RETURNING *;

-- name: GetPipelineByID :one
SELECT * FROM pipelines WHERE id = ?;

-- name: GetPipelineByName :one
SELECT * FROM pipelines WHERE name = ?;

-- name: ListPipelines :many
SELECT * FROM pipelines ORDER BY name LIMIT ? OFFSET ?;

-- name: CountPipelines :one
SELECT COUNT(*) FROM pipelines;

-- name: UpdatePipeline :exec
UPDATE pipelines
SET description = COALESCE(?, description),
    schedule_cron = COALESCE(?, schedule_cron),
    is_paused = COALESCE(?, is_paused),
    concurrency_limit = COALESCE(?, concurrency_limit),
    updated_at = datetime('now')
WHERE id = ?;

-- name: DeletePipeline :exec
DELETE FROM pipelines WHERE id = ?;

-- name: ListScheduledPipelines :many
SELECT * FROM pipelines WHERE schedule_cron IS NOT NULL AND is_paused = 0;

-- name: CreatePipelineJob :one
INSERT INTO pipeline_jobs (id, pipeline_id, name, compute_endpoint_id, depends_on, notebook_id, timeout_seconds, retry_count, job_order)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
RETURNING *;

-- name: GetPipelineJobByID :one
SELECT * FROM pipeline_jobs WHERE id = ?;

-- name: ListPipelineJobsByPipeline :many
SELECT * FROM pipeline_jobs WHERE pipeline_id = ? ORDER BY job_order, name;

-- name: DeletePipelineJob :exec
DELETE FROM pipeline_jobs WHERE id = ?;

-- name: DeletePipelineJobsByPipeline :exec
DELETE FROM pipeline_jobs WHERE pipeline_id = ?;

-- name: CreatePipelineRun :one
INSERT INTO pipeline_runs (id, pipeline_id, status, trigger_type, triggered_by, parameters, git_commit_hash)
VALUES (?, ?, ?, ?, ?, ?, ?)
RETURNING *;

-- name: GetPipelineRunByID :one
SELECT * FROM pipeline_runs WHERE id = ?;

-- name: ListPipelineRuns :many
SELECT * FROM pipeline_runs
WHERE (? = '' OR pipeline_id = ?)
  AND (? = '' OR status = ?)
ORDER BY created_at DESC
LIMIT ? OFFSET ?;

-- name: CountPipelineRuns :one
SELECT COUNT(*) FROM pipeline_runs
WHERE (? = '' OR pipeline_id = ?)
  AND (? = '' OR status = ?);

-- name: UpdatePipelineRunStatus :exec
UPDATE pipeline_runs SET status = ?, error_message = ? WHERE id = ?;

-- name: UpdatePipelineRunStarted :exec
UPDATE pipeline_runs SET status = 'RUNNING', started_at = datetime('now') WHERE id = ?;

-- name: UpdatePipelineRunFinished :exec
UPDATE pipeline_runs SET status = ?, finished_at = datetime('now'), error_message = ? WHERE id = ?;

-- name: CountActivePipelineRuns :one
SELECT COUNT(*) FROM pipeline_runs WHERE pipeline_id = ? AND status IN ('PENDING', 'RUNNING');

-- name: CancelPendingPipelineRuns :exec
UPDATE pipeline_runs SET status = 'CANCELLED' WHERE pipeline_id = ? AND status = 'PENDING';

-- name: CreatePipelineJobRun :one
INSERT INTO pipeline_job_runs (id, run_id, job_id, job_name, status, retry_attempt)
VALUES (?, ?, ?, ?, ?, ?)
RETURNING *;

-- name: GetPipelineJobRunByID :one
SELECT * FROM pipeline_job_runs WHERE id = ?;

-- name: ListPipelineJobRunsByRun :many
SELECT * FROM pipeline_job_runs WHERE run_id = ? ORDER BY created_at;

-- name: UpdatePipelineJobRunStatus :exec
UPDATE pipeline_job_runs SET status = ?, error_message = ? WHERE id = ?;

-- name: UpdatePipelineJobRunStarted :exec
UPDATE pipeline_job_runs SET status = 'RUNNING', started_at = datetime('now') WHERE id = ?;

-- name: UpdatePipelineJobRunFinished :exec
UPDATE pipeline_job_runs SET status = ?, finished_at = datetime('now'), error_message = ? WHERE id = ?;
