-- +goose Up
ALTER TABLE semantic_metrics ADD COLUMN relationship_names TEXT NOT NULL DEFAULT '[]';

PRAGMA foreign_keys=off;

ALTER TABLE semantic_relationships RENAME TO semantic_relationships_old;

CREATE TABLE semantic_relationships (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    from_semantic_id TEXT NOT NULL REFERENCES semantic_models(id) ON DELETE CASCADE,
    to_semantic_id TEXT NOT NULL REFERENCES semantic_models(id) ON DELETE CASCADE,
    relationship_type TEXT NOT NULL
        CHECK (relationship_type IN ('ONE_TO_ONE','ONE_TO_MANY','MANY_TO_ONE','MANY_TO_MANY')),
    join_sql TEXT NOT NULL,
    cost INTEGER NOT NULL DEFAULT 0,
    max_hops INTEGER NOT NULL DEFAULT 0,
    created_by TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    CHECK (cost >= 0),
    CHECK (max_hops >= 0),
    UNIQUE(from_semantic_id, name)
);

INSERT INTO semantic_relationships (
    id, name, from_semantic_id, to_semantic_id, relationship_type, join_sql, cost, max_hops, created_by, created_at, updated_at
)
SELECT
    id, name, from_semantic_id, to_semantic_id, relationship_type, join_sql, cost, max_hops, created_by, created_at, updated_at
FROM semantic_relationships_old;

DROP TABLE semantic_relationships_old;

CREATE INDEX idx_semantic_relationships_from ON semantic_relationships(from_semantic_id);
CREATE INDEX idx_semantic_relationships_to ON semantic_relationships(to_semantic_id);

PRAGMA foreign_keys=on;

-- +goose Down
SELECT 1;
