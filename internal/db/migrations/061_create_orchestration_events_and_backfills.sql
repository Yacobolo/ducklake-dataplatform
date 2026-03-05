-- +goose Up
CREATE TABLE orchestration_events (
    id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    asset_id TEXT REFERENCES data_assets(id) ON DELETE SET NULL,
    partition_key TEXT,
    payload_json TEXT NOT NULL DEFAULT '{}',
    status TEXT NOT NULL DEFAULT 'PENDING'
        CHECK (status IN ('PENDING', 'PROCESSING', 'PROCESSED', 'FAILED')),
    attempt_count INTEGER NOT NULL DEFAULT 0,
    available_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_error TEXT,
    idempotency_key TEXT,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(idempotency_key)
);

CREATE INDEX idx_orchestration_events_status_available ON orchestration_events(status, available_at);

CREATE TABLE backfill_requests (
    id TEXT PRIMARY KEY,
    asset_id TEXT NOT NULL REFERENCES data_assets(id) ON DELETE CASCADE,
    partition_from TEXT NOT NULL,
    partition_to TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING'
        CHECK (status IN ('PENDING', 'RUNNING', 'SUCCESS', 'FAILED', 'CANCELLED')),
    requested_by TEXT NOT NULL,
    max_parallelism INTEGER NOT NULL DEFAULT 1,
    error_message TEXT,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at DATETIME,
    finished_at DATETIME
);

CREATE INDEX idx_backfill_requests_asset_status ON backfill_requests(asset_id, status);

CREATE TABLE backfill_slices (
    id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL REFERENCES backfill_requests(id) ON DELETE CASCADE,
    asset_id TEXT NOT NULL REFERENCES data_assets(id) ON DELETE CASCADE,
    partition_key TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING'
        CHECK (status IN ('PENDING', 'RUNNING', 'SUCCESS', 'FAILED', 'CANCELLED')),
    run_id TEXT REFERENCES asset_runs(id) ON DELETE SET NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 1,
    error_message TEXT,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at DATETIME,
    finished_at DATETIME,
    UNIQUE(request_id, partition_key)
);

CREATE INDEX idx_backfill_slices_request_status ON backfill_slices(request_id, status);

-- +goose Down
DROP INDEX IF EXISTS idx_backfill_slices_request_status;
DROP TABLE IF EXISTS backfill_slices;

DROP INDEX IF EXISTS idx_backfill_requests_asset_status;
DROP TABLE IF EXISTS backfill_requests;

DROP INDEX IF EXISTS idx_orchestration_events_status_available;
DROP TABLE IF EXISTS orchestration_events;
