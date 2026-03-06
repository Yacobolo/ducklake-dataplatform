-- +goose Up
CREATE INDEX IF NOT EXISTS idx_orchestration_events_status_updated
    ON orchestration_events(status, updated_at);

-- +goose Down
DROP INDEX IF EXISTS idx_orchestration_events_status_updated;
