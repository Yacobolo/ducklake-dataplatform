-- +goose Up
CREATE TABLE domains (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    description TEXT NOT NULL DEFAULT '',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE teams (
    id TEXT PRIMARY KEY,
    domain_id TEXT NOT NULL REFERENCES domains(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    contact_channel TEXT NOT NULL DEFAULT '',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(domain_id, name)
);

CREATE TABLE data_products (
    id TEXT PRIMARY KEY,
    slug TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    domain_id TEXT NOT NULL REFERENCES domains(id) ON DELETE RESTRICT,
    owner_team_id TEXT NOT NULL REFERENCES teams(id) ON DELETE RESTRICT,
    steward_principal TEXT NOT NULL DEFAULT '',
    contact_channel TEXT NOT NULL DEFAULT '',
    visibility TEXT NOT NULL DEFAULT '',
    consumer_audience TEXT NOT NULL DEFAULT '',
    docs_url TEXT NOT NULL DEFAULT '',
    access_request_path TEXT NOT NULL DEFAULT '',
    business_definitions_json TEXT NOT NULL DEFAULT '{}',
    contract_json TEXT NOT NULL DEFAULT '{}',
    slo_json TEXT NOT NULL DEFAULT '{}',
    publication_intent TEXT NOT NULL DEFAULT 'DRAFT',
    created_by TEXT NOT NULL DEFAULT '',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE data_product_versions (
    id TEXT PRIMARY KEY,
    product_id TEXT NOT NULL REFERENCES data_products(id) ON DELETE CASCADE,
    version INTEGER NOT NULL,
    release_state TEXT NOT NULL DEFAULT 'DRAFT',
    compatibility_level TEXT NOT NULL DEFAULT 'BACKWARD_COMPATIBLE',
    contract_json TEXT NOT NULL DEFAULT '{}',
    slo_json TEXT NOT NULL DEFAULT '{}',
    docs_url TEXT NOT NULL DEFAULT '',
    access_request_path TEXT NOT NULL DEFAULT '',
    created_by TEXT NOT NULL DEFAULT '',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(product_id, version)
);

CREATE TABLE data_product_status (
    product_id TEXT PRIMARY KEY REFERENCES data_products(id) ON DELETE CASCADE,
    publication_state TEXT NOT NULL DEFAULT 'DRAFT',
    certification_state TEXT NOT NULL DEFAULT 'DRAFT',
    freshness_status TEXT NOT NULL DEFAULT 'UNKNOWN',
    quality_status TEXT NOT NULL DEFAULT 'UNKNOWN',
    last_successful_update_at DATETIME,
    failing_checks_count INTEGER NOT NULL DEFAULT 0,
    lineage_coverage REAL,
    adoption_metrics_json TEXT NOT NULL DEFAULT '{}',
    open_warnings_json TEXT NOT NULL DEFAULT '[]',
    replacement_product_id TEXT REFERENCES data_products(id) ON DELETE SET NULL,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE product_outputs (
    id TEXT PRIMARY KEY,
    product_version_id TEXT NOT NULL REFERENCES data_product_versions(id) ON DELETE CASCADE,
    asset_id TEXT NOT NULL REFERENCES data_assets(id) ON DELETE CASCADE,
    is_primary INTEGER NOT NULL DEFAULT 0,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(product_version_id, asset_id)
);

CREATE TABLE product_semantic_entrypoints (
    id TEXT PRIMARY KEY,
    product_version_id TEXT NOT NULL REFERENCES data_product_versions(id) ON DELETE CASCADE,
    semantic_model_id TEXT NOT NULL REFERENCES semantic_models(id) ON DELETE CASCADE,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(product_version_id, semantic_model_id)
);

CREATE TABLE product_dependencies (
    id TEXT PRIMARY KEY,
    product_id TEXT NOT NULL REFERENCES data_products(id) ON DELETE CASCADE,
    depends_on_product_id TEXT NOT NULL REFERENCES data_products(id) ON DELETE CASCADE,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(product_id, depends_on_product_id)
);

CREATE TABLE product_subscriptions (
    id TEXT PRIMARY KEY,
    product_id TEXT NOT NULL REFERENCES data_products(id) ON DELETE CASCADE,
    principal_name TEXT NOT NULL,
    event_type TEXT NOT NULL,
    channel TEXT NOT NULL DEFAULT 'inbox',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(product_id, principal_name, event_type, channel)
);

CREATE TABLE product_events (
    id TEXT PRIMARY KEY,
    product_id TEXT NOT NULL REFERENCES data_products(id) ON DELETE CASCADE,
    event_type TEXT NOT NULL,
    title TEXT NOT NULL DEFAULT '',
    description TEXT NOT NULL DEFAULT '',
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_teams_domain ON teams(domain_id);
CREATE INDEX idx_products_domain ON data_products(domain_id);
CREATE INDEX idx_products_owner_team ON data_products(owner_team_id);
CREATE INDEX idx_product_versions_product ON data_product_versions(product_id, version DESC);
CREATE INDEX idx_product_outputs_version ON product_outputs(product_version_id);
CREATE INDEX idx_product_outputs_asset ON product_outputs(asset_id);
CREATE INDEX idx_product_semantic_entrypoints_version ON product_semantic_entrypoints(product_version_id);
CREATE INDEX idx_product_semantic_entrypoints_model ON product_semantic_entrypoints(semantic_model_id);
CREATE INDEX idx_product_dependencies_product ON product_dependencies(product_id);
CREATE INDEX idx_product_dependencies_depends_on ON product_dependencies(depends_on_product_id);
CREATE INDEX idx_product_subscriptions_product ON product_subscriptions(product_id);
CREATE INDEX idx_product_events_product_created ON product_events(product_id, created_at DESC);

ALTER TABLE data_assets ADD COLUMN product_id TEXT REFERENCES data_products(id) ON DELETE RESTRICT;
CREATE INDEX idx_data_assets_product ON data_assets(product_id);

-- +goose Down
DROP INDEX IF EXISTS idx_data_assets_product;
DROP INDEX IF EXISTS idx_product_events_product_created;
DROP TABLE IF EXISTS product_events;
DROP TABLE IF EXISTS product_subscriptions;
DROP TABLE IF EXISTS product_dependencies;
DROP TABLE IF EXISTS product_semantic_entrypoints;
DROP TABLE IF EXISTS product_outputs;
DROP TABLE IF EXISTS data_product_status;
DROP TABLE IF EXISTS data_product_versions;
DROP TABLE IF EXISTS data_products;
DROP TABLE IF EXISTS teams;
DROP TABLE IF EXISTS domains;
