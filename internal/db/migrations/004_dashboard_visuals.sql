-- +goose Up
ALTER TABLE cells ADD COLUMN visual_spec TEXT NOT NULL DEFAULT '';

ALTER TABLE semantic_metrics ADD COLUMN label TEXT NOT NULL DEFAULT '';
ALTER TABLE semantic_metrics ADD COLUMN filter_sql TEXT NOT NULL DEFAULT '';

CREATE TABLE dashboards (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    owner TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE dashboard_widgets (
    id TEXT PRIMARY KEY,
    dashboard_id TEXT NOT NULL REFERENCES dashboards(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    source_json TEXT NOT NULL,
    visual_spec TEXT NOT NULL DEFAULT '',
    layout_x INTEGER NOT NULL DEFAULT 0,
    layout_y INTEGER NOT NULL DEFAULT 0,
    layout_w INTEGER NOT NULL DEFAULT 4,
    layout_h INTEGER NOT NULL DEFAULT 3,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_dashboards_owner ON dashboards(owner);
CREATE INDEX idx_dashboard_widgets_dashboard ON dashboard_widgets(dashboard_id);

-- +goose Down
DROP TABLE IF EXISTS dashboard_widgets;
DROP TABLE IF EXISTS dashboards;
