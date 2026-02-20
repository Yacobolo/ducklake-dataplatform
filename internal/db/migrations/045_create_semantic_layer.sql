-- +goose Up
CREATE TABLE semantic_models (
    id TEXT PRIMARY KEY,
    project_name TEXT NOT NULL,
    name TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    owner TEXT NOT NULL DEFAULT '',
    base_model_ref TEXT NOT NULL,
    default_time_dimension TEXT NOT NULL DEFAULT '',
    tags TEXT NOT NULL DEFAULT '[]',
    created_by TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(project_name, name)
);
CREATE INDEX idx_semantic_models_project ON semantic_models(project_name);

CREATE TABLE semantic_metrics (
    id TEXT PRIMARY KEY,
    semantic_model_id TEXT NOT NULL REFERENCES semantic_models(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    metric_type TEXT NOT NULL
        CHECK (metric_type IN ('SUM','COUNT','COUNT_DISTINCT','AVG','MIN','MAX','RATIO')),
    expression_mode TEXT NOT NULL DEFAULT 'DSL'
        CHECK (expression_mode IN ('DSL','SQL')),
    expression TEXT NOT NULL,
    default_time_grain TEXT NOT NULL DEFAULT '',
    format TEXT NOT NULL DEFAULT '',
    owner TEXT NOT NULL DEFAULT '',
    certification_state TEXT NOT NULL DEFAULT 'DRAFT'
        CHECK (certification_state IN ('DRAFT','CERTIFIED','DEPRECATED')),
    created_by TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(semantic_model_id, name)
);
CREATE INDEX idx_semantic_metrics_model ON semantic_metrics(semantic_model_id);

CREATE TABLE semantic_relationships (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    from_semantic_id TEXT NOT NULL REFERENCES semantic_models(id) ON DELETE CASCADE,
    to_semantic_id TEXT NOT NULL REFERENCES semantic_models(id) ON DELETE CASCADE,
    relationship_type TEXT NOT NULL
        CHECK (relationship_type IN ('ONE_TO_ONE','ONE_TO_MANY','MANY_TO_ONE','MANY_TO_MANY')),
    join_sql TEXT NOT NULL,
    is_default INTEGER NOT NULL DEFAULT 0,
    cost INTEGER NOT NULL DEFAULT 0,
    max_hops INTEGER NOT NULL DEFAULT 0,
    created_by TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    CHECK (cost >= 0),
    CHECK (max_hops >= 0)
);
CREATE INDEX idx_semantic_relationships_from ON semantic_relationships(from_semantic_id);
CREATE INDEX idx_semantic_relationships_to ON semantic_relationships(to_semantic_id);

CREATE TABLE semantic_pre_aggregations (
    id TEXT PRIMARY KEY,
    semantic_model_id TEXT NOT NULL REFERENCES semantic_models(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    metric_set TEXT NOT NULL DEFAULT '[]',
    dimension_set TEXT NOT NULL DEFAULT '[]',
    grain TEXT NOT NULL DEFAULT '',
    target_relation TEXT NOT NULL,
    refresh_policy TEXT NOT NULL DEFAULT '',
    created_by TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(semantic_model_id, name)
);
CREATE INDEX idx_semantic_pre_aggs_model ON semantic_pre_aggregations(semantic_model_id);

-- +goose Down
DROP TABLE IF EXISTS semantic_pre_aggregations;
DROP TABLE IF EXISTS semantic_relationships;
DROP TABLE IF EXISTS semantic_metrics;
DROP TABLE IF EXISTS semantic_models;
