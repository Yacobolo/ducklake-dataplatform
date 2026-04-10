-- name: CreateDashboard :one
INSERT INTO dashboards (id, name, description, owner, folder_id, semantic_project_name, semantic_model_name, compute_mode, compute_endpoint_name, compute_fallback_local)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
RETURNING *;

-- name: GetDashboard :one
SELECT * FROM dashboards WHERE id = ?;

-- name: ListDashboards :many
SELECT * FROM dashboards
WHERE (sqlc.narg('owner') IS NULL OR owner = sqlc.narg('owner'))
  AND (sqlc.narg('folder_id') IS NULL OR folder_id = sqlc.narg('folder_id'))
ORDER BY updated_at DESC
LIMIT sqlc.arg('limit') OFFSET sqlc.arg('offset');

-- name: CountDashboards :one
SELECT COUNT(*) FROM dashboards
WHERE (sqlc.narg('owner') IS NULL OR owner = sqlc.narg('owner'))
  AND (sqlc.narg('folder_id') IS NULL OR folder_id = sqlc.narg('folder_id'));

-- name: ListDashboardsByFolders :many
SELECT * FROM dashboards
WHERE folder_id IN (sqlc.slice('folder_ids'))
ORDER BY updated_at DESC;

-- name: UpdateDashboard :one
UPDATE dashboards
SET name = ?,
    description = ?,
    owner = ?,
    folder_id = ?,
    semantic_project_name = ?,
    semantic_model_name = ?,
    compute_mode = ?,
    compute_endpoint_name = ?,
    compute_fallback_local = ?,
    updated_at = datetime('now')
WHERE id = ?
RETURNING *;

-- name: DeleteDashboard :exec
DELETE FROM dashboards WHERE id = ?;

-- name: CreateDashboardWidget :one
INSERT INTO dashboard_widgets (
    id, dashboard_id, filter_origin_key, page_name, name, description, source_json, visual_spec,
    layout_x, layout_y, layout_w, layout_h
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
RETURNING *;

-- name: GetDashboardWidget :one
SELECT * FROM dashboard_widgets WHERE id = ?;

-- name: ListDashboardWidgetsByDashboard :many
SELECT * FROM dashboard_widgets WHERE dashboard_id = ? ORDER BY layout_y, layout_x, created_at;

-- name: UpdateDashboardWidget :one
UPDATE dashboard_widgets
SET filter_origin_key = ?,
    page_name = ?,
    name = ?,
    description = ?,
    source_json = ?,
    visual_spec = ?,
    layout_x = ?,
    layout_y = ?,
    layout_w = ?,
    layout_h = ?,
    updated_at = datetime('now')
WHERE id = ?
RETURNING *;

-- name: DeleteDashboardWidget :exec
DELETE FROM dashboard_widgets WHERE id = ?;
