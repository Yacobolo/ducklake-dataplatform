-- +goose Up
CREATE TABLE IF NOT EXISTS compute_cluster_templates (
    id                       TEXT PRIMARY KEY,
    name                     TEXT NOT NULL UNIQUE,
    provider                 TEXT NOT NULL,
    workload_class           TEXT NOT NULL,
    size                     TEXT NOT NULL DEFAULT '',
    min_replicas             INTEGER NOT NULL DEFAULT 0,
    max_replicas             INTEGER NOT NULL DEFAULT 1,
    idle_auto_stop_seconds   INTEGER NOT NULL DEFAULT 0,
    scaling_policy           TEXT NOT NULL DEFAULT '',
    storage_profile          TEXT NOT NULL DEFAULT '',
    result_retention_seconds INTEGER NOT NULL DEFAULT 0,
    created_at               TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at               TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS managed_compute_clusters (
    id               TEXT PRIMARY KEY,
    name             TEXT NOT NULL UNIQUE,
    template_id      TEXT NOT NULL REFERENCES compute_cluster_templates(id) ON DELETE RESTRICT,
    endpoint_id      TEXT NOT NULL UNIQUE REFERENCES compute_endpoints(id) ON DELETE CASCADE,
    provider         TEXT NOT NULL,
    external_id      TEXT NOT NULL UNIQUE,
    desired_state    TEXT NOT NULL,
    observed_state   TEXT NOT NULL,
    min_replicas     INTEGER NOT NULL DEFAULT 0,
    max_replicas     INTEGER NOT NULL DEFAULT 1,
    is_draining      INTEGER NOT NULL DEFAULT 0,
    last_activity_at TEXT,
    endpoint_url     TEXT,
    created_at       TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at       TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_managed_compute_clusters_template_id ON managed_compute_clusters(template_id);
CREATE INDEX IF NOT EXISTS idx_managed_compute_clusters_endpoint_id ON managed_compute_clusters(endpoint_id);

-- +goose Down
DROP INDEX IF EXISTS idx_managed_compute_clusters_endpoint_id;
DROP INDEX IF EXISTS idx_managed_compute_clusters_template_id;
DROP TABLE IF EXISTS managed_compute_clusters;
DROP TABLE IF EXISTS compute_cluster_templates;
