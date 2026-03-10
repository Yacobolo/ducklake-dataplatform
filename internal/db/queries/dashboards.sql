-- name: CreateDashboard :one
INSERT INTO dashboards (id, name, description, owner)
VALUES (?, ?, ?, ?)
RETURNING *;

-- name: GetDashboard :one
SELECT * FROM dashboards WHERE id = ?;

-- name: ListDashboards :many
SELECT * FROM dashboards
WHERE (sqlc.narg('owner') IS NULL OR owner = sqlc.narg('owner'))
ORDER BY updated_at DESC
LIMIT sqlc.arg('limit') OFFSET sqlc.arg('offset');

-- name: CountDashboards :one
SELECT COUNT(*) FROM dashboards
WHERE (sqlc.narg('owner') IS NULL OR owner = sqlc.narg('owner'));

-- name: UpdateDashboard :one
UPDATE dashboards
SET name = ?, description = ?, updated_at = datetime('now')
WHERE id = ?
RETURNING *;

-- name: DeleteDashboard :exec
DELETE FROM dashboards WHERE id = ?;

-- name: CreateDashboardWidget :one
INSERT INTO dashboard_widgets (
    id, dashboard_id, name, description, source_json, visual_spec,
    layout_x, layout_y, layout_w, layout_h
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
RETURNING *;

-- name: GetDashboardWidget :one
SELECT * FROM dashboard_widgets WHERE id = ?;

-- name: ListDashboardWidgetsByDashboard :many
SELECT * FROM dashboard_widgets WHERE dashboard_id = ? ORDER BY layout_y, layout_x, created_at;

-- name: UpdateDashboardWidget :one
UPDATE dashboard_widgets
SET name = ?,
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
