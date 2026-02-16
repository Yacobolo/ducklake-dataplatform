-- name: CreateModelTest :one
INSERT INTO model_tests (id, model_id, name, test_type, column_name, config)
VALUES (?, ?, ?, ?, ?, ?)
RETURNING *;

-- name: GetModelTestByID :one
SELECT * FROM model_tests WHERE id = ?;

-- name: ListModelTestsByModel :many
SELECT * FROM model_tests WHERE model_id = ? ORDER BY name;

-- name: DeleteModelTest :exec
DELETE FROM model_tests WHERE id = ?;

-- name: CreateModelTestResult :one
INSERT INTO model_test_results (id, run_step_id, test_id, test_name, status, rows_returned, error_message)
VALUES (?, ?, ?, ?, ?, ?, ?)
RETURNING *;

-- name: ListModelTestResultsByStep :many
SELECT * FROM model_test_results WHERE run_step_id = ? ORDER BY test_name;
