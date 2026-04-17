-- name: CreatePipeline :one
INSERT INTO pipelines (
  id,
  name,
  description,
  schedule_cron,
  is_paused,
  concurrency_limit,
  run_as_principal,
  admission_mode,
  max_run_duration_seconds,
  notification_webhooks,
  default_retry_count,
  default_timeout_seconds,
  default_compute_endpoint_id,
  created_by,
  folder_id
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
RETURNING *;

-- name: GetPipelineByID :one
SELECT * FROM pipelines WHERE id = ?;

-- name: GetPipelineByName :one
SELECT * FROM pipelines WHERE name = ?;

-- name: ListPipelines :many
SELECT * FROM pipelines ORDER BY name LIMIT ? OFFSET ?;

-- name: CountPipelines :one
SELECT COUNT(*) FROM pipelines;

-- name: ListPipelinesByFolders :many
SELECT * FROM pipelines
WHERE folder_id IN (sqlc.slice('folder_ids'))
ORDER BY name;

-- name: UpdatePipeline :exec
UPDATE pipelines
SET description = COALESCE(?, description),
    schedule_cron = COALESCE(?, schedule_cron),
    is_paused = COALESCE(?, is_paused),
    concurrency_limit = COALESCE(?, concurrency_limit),
    run_as_principal = COALESCE(?, run_as_principal),
    admission_mode = COALESCE(?, admission_mode),
    max_run_duration_seconds = COALESCE(?, max_run_duration_seconds),
    notification_webhooks = COALESCE(?, notification_webhooks),
    default_retry_count = COALESCE(?, default_retry_count),
    default_timeout_seconds = COALESCE(?, default_timeout_seconds),
    default_compute_endpoint_id = COALESCE(?, default_compute_endpoint_id),
    folder_id = COALESCE(?, folder_id),
    updated_at = datetime('now')
WHERE id = ?;

-- name: DeletePipeline :exec
DELETE FROM pipelines WHERE id = ?;

-- name: ListScheduledPipelines :many
SELECT * FROM pipelines WHERE schedule_cron IS NOT NULL AND is_paused = 0;

-- name: CreatePipelineJob :one
INSERT INTO pipeline_jobs (id, pipeline_id, name, compute_endpoint_id, depends_on, notebook_id, timeout_seconds, retry_count, job_order, job_type, model_selector)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
INSERT INTO pipeline_runs (
  id,
  pipeline_id,
  status,
  trigger_type,
  triggered_by,
  effective_principal,
  parameters,
  git_commit_hash,
  queued_at,
  queue_started_at,
  repaired_from_run_id,
  provenance,
  sla_breached_at
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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

-- name: MarkPipelineRunSLABreached :exec
UPDATE pipeline_runs
SET sla_breached_at = datetime('now'),
    error_message = COALESCE(?, error_message)
WHERE id = ?;

-- name: UpdatePipelineRunQueueStarted :exec
UPDATE pipeline_runs
SET queue_started_at = COALESCE(queue_started_at, datetime('now'))
WHERE id = ?;

-- name: UpdatePipelineRunStarted :exec
UPDATE pipeline_runs SET status = 'RUNNING', started_at = datetime('now') WHERE id = ?;

-- name: UpdatePipelineRunFinished :exec
UPDATE pipeline_runs SET status = ?, finished_at = datetime('now'), error_message = ? WHERE id = ?;

-- name: CountActivePipelineRuns :one
SELECT COUNT(*) FROM pipeline_runs
WHERE pipeline_id = ?
  AND status IN ('PENDING', 'RUNNING')
  AND (queue_started_at IS NOT NULL OR queued_at IS NULL);

-- name: ListQueuedPipelineRuns :many
SELECT * FROM pipeline_runs
WHERE pipeline_id = ?
  AND status = 'PENDING'
  AND queued_at IS NOT NULL
  AND queue_started_at IS NULL
ORDER BY queued_at, created_at
LIMIT ?;

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
UPDATE pipeline_job_runs
SET status = 'RUNNING',
    started_at = datetime('now'),
    effective_compute_endpoint_id = sqlc.arg(effective_compute_endpoint_id),
    attempt_count = sqlc.arg(attempt_count)
WHERE id = sqlc.arg(id);

-- name: UpdatePipelineJobRunFinished :exec
UPDATE pipeline_job_runs
SET status = sqlc.arg(status),
    finished_at = datetime('now'),
    error_message = sqlc.arg(error_message),
    last_error_code = sqlc.arg(last_error_code),
    attempt_count = sqlc.arg(attempt_count),
    retry_attempt = CASE
        WHEN sqlc.arg(attempt_count) > 0 THEN sqlc.arg(attempt_count) - 1
        ELSE 0
    END
WHERE id = sqlc.arg(id);

-- name: CreatePipelineRunEvent :one
INSERT INTO pipeline_run_events (id, run_id, job_run_id, event_type, message, error_code, metadata)
VALUES (?, ?, ?, ?, ?, ?, ?)
RETURNING *;

-- name: ListPipelineRunEvents :many
SELECT * FROM pipeline_run_events
WHERE run_id = ?
ORDER BY created_at ASC
LIMIT ? OFFSET ?;

-- name: CountPipelineRunEvents :one
SELECT COUNT(*) FROM pipeline_run_events WHERE run_id = ?;
