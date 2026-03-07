-- +goose Up
CREATE TABLE data_assets (
    id TEXT PRIMARY KEY,
    asset_key TEXT NOT NULL UNIQUE,
    asset_type TEXT NOT NULL,
    owner TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    tags_json TEXT NOT NULL DEFAULT '[]',
    schema_json TEXT NOT NULL DEFAULT '{}',
    partition_definition_json TEXT,
    freshness_policy_json TEXT,
    materialization_policy_json TEXT,
    auto_materialize_policy_json TEXT,
    io_profile TEXT NOT NULL DEFAULT '',
    is_active INTEGER NOT NULL DEFAULT 1,
    created_by TEXT NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_data_assets_type_active ON data_assets(asset_type, is_active);
CREATE INDEX idx_data_assets_owner ON data_assets(owner);

CREATE TABLE asset_dependencies (
    id TEXT PRIMARY KEY,
    asset_id TEXT NOT NULL REFERENCES data_assets(id) ON DELETE CASCADE,
    upstream_asset_id TEXT NOT NULL REFERENCES data_assets(id) ON DELETE CASCADE,
    dependency_type TEXT NOT NULL DEFAULT 'HARD',
    partition_mapping_json TEXT NOT NULL DEFAULT '{}',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(asset_id, upstream_asset_id, dependency_type)
);

CREATE INDEX idx_asset_dependencies_asset ON asset_dependencies(asset_id);
CREATE INDEX idx_asset_dependencies_upstream ON asset_dependencies(upstream_asset_id);

CREATE TABLE asset_partitions (
    id TEXT PRIMARY KEY,
    asset_id TEXT NOT NULL REFERENCES data_assets(id) ON DELETE CASCADE,
    partition_key TEXT NOT NULL,
    partition_time DATETIME,
    status TEXT NOT NULL DEFAULT 'MISSING'
        CHECK (status IN ('MISSING', 'MATERIALIZED', 'FAILED', 'STALE')),
    last_materialized_at DATETIME,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(asset_id, partition_key)
);

CREATE INDEX idx_asset_partitions_asset_status ON asset_partitions(asset_id, status);

CREATE TABLE asset_runs (
    id TEXT PRIMARY KEY,
    asset_id TEXT NOT NULL REFERENCES data_assets(id) ON DELETE CASCADE,
    run_group_id TEXT,
    partition_key TEXT,
    status TEXT NOT NULL DEFAULT 'QUEUED'
        CHECK (status IN ('QUEUED', 'PLANNING', 'RUNNING', 'RETRYING', 'SUCCESS', 'FAILED', 'CANCELLED', 'SKIPPED', 'STALE')),
    trigger_type TEXT NOT NULL,
    triggered_by TEXT NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 1,
    started_at DATETIME,
    finished_at DATETIME,
    error_message TEXT,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_asset_runs_asset_created ON asset_runs(asset_id, created_at DESC);
CREATE INDEX idx_asset_runs_status ON asset_runs(status);
CREATE INDEX idx_asset_runs_group ON asset_runs(run_group_id);

CREATE TABLE asset_run_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL REFERENCES asset_runs(id) ON DELETE CASCADE,
    event_type TEXT NOT NULL,
    event_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    message TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    check_results_json TEXT NOT NULL DEFAULT '{}',
    stats_json TEXT NOT NULL DEFAULT '{}',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_asset_run_events_run_created ON asset_run_events(run_id, created_at DESC);

CREATE TABLE asset_materializations (
    id TEXT PRIMARY KEY,
    asset_id TEXT NOT NULL REFERENCES data_assets(id) ON DELETE CASCADE,
    run_id TEXT REFERENCES asset_runs(id) ON DELETE SET NULL,
    partition_key TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    row_count INTEGER,
    schema_hash TEXT,
    materialized_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_asset_materializations_asset_created ON asset_materializations(asset_id, created_at DESC);
CREATE INDEX idx_asset_materializations_run ON asset_materializations(run_id);

CREATE TABLE asset_checks (
    id TEXT PRIMARY KEY,
    asset_id TEXT NOT NULL REFERENCES data_assets(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    check_type TEXT NOT NULL,
    severity TEXT NOT NULL DEFAULT 'ERROR',
    config_json TEXT NOT NULL DEFAULT '{}',
    enabled INTEGER NOT NULL DEFAULT 1,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(asset_id, name)
);

CREATE INDEX idx_asset_checks_asset ON asset_checks(asset_id);

CREATE TABLE asset_check_results (
    id TEXT PRIMARY KEY,
    check_id TEXT NOT NULL REFERENCES asset_checks(id) ON DELETE CASCADE,
    run_id TEXT REFERENCES asset_runs(id) ON DELETE SET NULL,
    partition_key TEXT,
    status TEXT NOT NULL CHECK (status IN ('PASS', 'FAIL', 'ERROR')),
    message TEXT,
    metrics_json TEXT NOT NULL DEFAULT '{}',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_asset_check_results_check_created ON asset_check_results(check_id, created_at DESC);
CREATE INDEX idx_asset_check_results_run ON asset_check_results(run_id);

-- +goose Down
DROP INDEX IF EXISTS idx_asset_check_results_run;
DROP INDEX IF EXISTS idx_asset_check_results_check_created;
DROP TABLE IF EXISTS asset_check_results;

DROP INDEX IF EXISTS idx_asset_checks_asset;
DROP TABLE IF EXISTS asset_checks;

DROP INDEX IF EXISTS idx_asset_materializations_run;
DROP INDEX IF EXISTS idx_asset_materializations_asset_created;
DROP TABLE IF EXISTS asset_materializations;

DROP INDEX IF EXISTS idx_asset_run_events_run_created;
DROP TABLE IF EXISTS asset_run_events;

DROP INDEX IF EXISTS idx_asset_runs_group;
DROP INDEX IF EXISTS idx_asset_runs_status;
DROP INDEX IF EXISTS idx_asset_runs_asset_created;
DROP TABLE IF EXISTS asset_runs;

DROP INDEX IF EXISTS idx_asset_partitions_asset_status;
DROP TABLE IF EXISTS asset_partitions;

DROP INDEX IF EXISTS idx_asset_dependencies_upstream;
DROP INDEX IF EXISTS idx_asset_dependencies_asset;
DROP TABLE IF EXISTS asset_dependencies;

DROP INDEX IF EXISTS idx_data_assets_owner;
DROP INDEX IF EXISTS idx_data_assets_type_active;
DROP TABLE IF EXISTS data_assets;
