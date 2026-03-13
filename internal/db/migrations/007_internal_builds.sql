-- +goose Up
CREATE TABLE builds (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    product_id TEXT REFERENCES data_products(id) ON DELETE SET NULL,
    environment_id TEXT NOT NULL REFERENCES environments(id) ON DELETE RESTRICT,
    state TEXT NOT NULL DEFAULT 'ready',
    git_ref TEXT NOT NULL,
    commit_sha TEXT,
    selector TEXT NOT NULL DEFAULT '',
    target_catalog TEXT NOT NULL,
    target_schema TEXT NOT NULL,
    source_model_run_id TEXT REFERENCES model_runs(id) ON DELETE SET NULL,
    compile_manifest TEXT NOT NULL,
    compile_diagnostics TEXT,
    created_by TEXT NOT NULL DEFAULT '',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE data_product_versions ADD COLUMN producing_build_id TEXT;

CREATE INDEX idx_builds_project_created ON builds(project_id, created_at DESC);
CREATE INDEX idx_builds_environment_created ON builds(environment_id, created_at DESC);
CREATE INDEX idx_builds_product_created ON builds(product_id, created_at DESC);
CREATE INDEX idx_product_versions_producing_build ON data_product_versions(producing_build_id);

-- +goose Down
DROP INDEX IF EXISTS idx_product_versions_producing_build;
DROP INDEX IF EXISTS idx_builds_product_created;
DROP INDEX IF EXISTS idx_builds_environment_created;
DROP INDEX IF EXISTS idx_builds_project_created;
DROP TABLE IF EXISTS builds;
