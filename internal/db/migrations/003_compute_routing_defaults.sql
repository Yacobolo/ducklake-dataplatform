-- +goose Up
CREATE TABLE compute_routing_defaults (
    id               INTEGER PRIMARY KEY CHECK (id = 1),
    interactive_mode TEXT NOT NULL DEFAULT 'BYOC_LOCAL',
    scheduled_mode   TEXT NOT NULL DEFAULT 'SHARED_ENDPOINT',
    notebook_mode    TEXT NOT NULL DEFAULT 'SHARED_ENDPOINT',
    updated_at       DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (interactive_mode IN ('AUTO', 'BYOC_LOCAL', 'SHARED_ENDPOINT')),
    CHECK (scheduled_mode IN ('AUTO', 'BYOC_LOCAL', 'SHARED_ENDPOINT')),
    CHECK (notebook_mode IN ('AUTO', 'BYOC_LOCAL', 'SHARED_ENDPOINT'))
);

INSERT INTO compute_routing_defaults (id) VALUES (1);

ALTER TABLE compute_endpoints ADD COLUMN selection_policy TEXT NOT NULL DEFAULT 'ALLOWED_USERS';
ALTER TABLE compute_endpoints ADD COLUMN workload_class TEXT NOT NULL DEFAULT 'MIXED';
ALTER TABLE compute_endpoints ADD COLUMN readiness_status TEXT NOT NULL DEFAULT 'READY';
ALTER TABLE compute_endpoints ADD COLUMN max_concurrency INTEGER;
ALTER TABLE compute_endpoints ADD COLUMN max_result_size_mb INTEGER;
ALTER TABLE compute_endpoints ADD COLUMN recommended_for_large_queries INTEGER NOT NULL DEFAULT 0;
ALTER TABLE compute_endpoints ADD COLUMN is_draining INTEGER NOT NULL DEFAULT 0;
ALTER TABLE compute_endpoints ADD COLUMN last_health_status TEXT;
ALTER TABLE compute_endpoints ADD COLUMN last_health_checked_at DATETIME;
ALTER TABLE compute_endpoints ADD COLUMN active_queries INTEGER;
ALTER TABLE compute_endpoints ADD COLUMN queued_jobs INTEGER;
ALTER TABLE compute_endpoints ADD COLUMN running_jobs INTEGER;
ALTER TABLE compute_endpoints ADD COLUMN completed_jobs INTEGER;
ALTER TABLE compute_endpoints ADD COLUMN stored_jobs INTEGER;
ALTER TABLE compute_endpoints ADD COLUMN cleaned_jobs INTEGER;
ALTER TABLE compute_endpoints ADD COLUMN query_result_ttl_seconds INTEGER;

ALTER TABLE query_jobs ADD COLUMN compute_mode TEXT NOT NULL DEFAULT '';
ALTER TABLE query_jobs ADD COLUMN endpoint_name TEXT;
ALTER TABLE query_jobs ADD COLUMN resolved_mode TEXT;
ALTER TABLE query_jobs ADD COLUMN resolved_endpoint_name TEXT;
ALTER TABLE query_jobs ADD COLUMN workload_type TEXT NOT NULL DEFAULT 'INTERACTIVE';

-- +goose Down
DROP TABLE compute_routing_defaults;
