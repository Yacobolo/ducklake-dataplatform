-- +goose Up
CREATE TABLE resource_access_events (
    id TEXT PRIMARY KEY,
    principal_id TEXT NOT NULL REFERENCES principals(id) ON DELETE CASCADE,
    resource_type TEXT NOT NULL,
    resource_key TEXT NOT NULL,
    display_name TEXT NOT NULL,
    section TEXT NOT NULL,
    accessed_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_resource_access_events_principal_accessed
    ON resource_access_events(principal_id, accessed_at DESC);
CREATE INDEX idx_resource_access_events_principal_resource
    ON resource_access_events(principal_id, resource_type, resource_key, accessed_at DESC);

CREATE TABLE saved_resources (
    principal_id TEXT NOT NULL REFERENCES principals(id) ON DELETE CASCADE,
    resource_type TEXT NOT NULL,
    resource_key TEXT NOT NULL,
    display_name TEXT NOT NULL,
    section TEXT NOT NULL,
    saved_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (principal_id, resource_type, resource_key)
);

CREATE INDEX idx_saved_resources_principal_saved
    ON saved_resources(principal_id, saved_at DESC);

-- +goose Down
DROP INDEX IF EXISTS idx_saved_resources_principal_saved;
DROP TABLE IF EXISTS saved_resources;
DROP INDEX IF EXISTS idx_resource_access_events_principal_resource;
DROP INDEX IF EXISTS idx_resource_access_events_principal_accessed;
DROP TABLE IF EXISTS resource_access_events;
