-- name: CreateModel :one
INSERT INTO models (id, project_name, name, sql_body, materialization, description, owner, tags, depends_on, config, created_by, contract, freshness_max_lag, freshness_cron)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
RETURNING *;

-- name: GetModelByID :one
SELECT * FROM models WHERE id = ?;

-- name: GetModelByName :one
SELECT * FROM models WHERE project_name = ? AND name = ?;

-- name: ListModels :many
SELECT * FROM models
WHERE (? = '' OR project_name = ?)
ORDER BY project_name, name
LIMIT ? OFFSET ?;

-- name: CountModels :one
SELECT COUNT(*) FROM models
WHERE (? = '' OR project_name = ?);

-- name: ListAllModels :many
SELECT * FROM models ORDER BY project_name, name;

-- name: UpdateModel :exec
UPDATE models
SET sql_body = COALESCE(?, sql_body),
    materialization = COALESCE(?, materialization),
    description = COALESCE(?, description),
    tags = COALESCE(?, tags),
    config = COALESCE(?, config),
    contract = COALESCE(?, contract),
    freshness_max_lag = ?,
    freshness_cron = ?,
    updated_at = datetime('now')
WHERE id = ?;

-- name: UpdateModelDependencies :exec
UPDATE models SET depends_on = ?, updated_at = datetime('now') WHERE id = ?;

-- name: DeleteModel :exec
DELETE FROM models WHERE id = ?;

-- name: CreateModelRun :one
INSERT INTO model_runs (id, status, trigger_type, triggered_by, target_catalog, target_schema, model_selector, variables, full_refresh)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
RETURNING *;

-- name: GetModelRunByID :one
SELECT * FROM model_runs WHERE id = ?;

-- name: ListModelRuns :many
SELECT * FROM model_runs
WHERE (? = '' OR status = ?)
ORDER BY created_at DESC
LIMIT ? OFFSET ?;

-- name: CountModelRuns :one
SELECT COUNT(*) FROM model_runs
WHERE (? = '' OR status = ?);

-- name: UpdateModelRunStarted :exec
UPDATE model_runs SET status = 'RUNNING', started_at = datetime('now') WHERE id = ?;

-- name: UpdateModelRunFinished :exec
UPDATE model_runs SET status = ?, finished_at = datetime('now'), error_message = ? WHERE id = ?;

-- name: CreateModelRunStep :one
INSERT INTO model_run_steps (id, run_id, model_id, model_name, compiled_sql, compiled_hash, depends_on, vars_used, macros_used, status, tier)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
RETURNING *;

-- name: ListModelRunStepsByRun :many
SELECT * FROM model_run_steps WHERE run_id = ? ORDER BY tier, model_name;

-- name: UpdateModelRunStepStarted :exec
UPDATE model_run_steps SET status = 'RUNNING', started_at = datetime('now') WHERE id = ?;

-- name: UpdateModelRunStepFinished :exec
UPDATE model_run_steps SET status = ?, finished_at = datetime('now'), rows_affected = ?, error_message = ? WHERE id = ?;
