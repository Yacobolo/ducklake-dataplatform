-- +goose Up
CREATE TABLE model_tests (
    id TEXT PRIMARY KEY,
    model_id TEXT NOT NULL REFERENCES models(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    test_type TEXT NOT NULL
        CHECK (test_type IN ('not_null','unique','accepted_values','relationships','custom_sql')),
    column_name TEXT NOT NULL DEFAULT '',
    config TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(model_id, name)
);
CREATE INDEX idx_model_tests_model ON model_tests(model_id);

CREATE TABLE model_test_results (
    id TEXT PRIMARY KEY,
    run_step_id TEXT NOT NULL REFERENCES model_run_steps(id) ON DELETE CASCADE,
    test_id TEXT NOT NULL,
    test_name TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('PASS','FAIL','ERROR')),
    rows_returned INTEGER,
    error_message TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX idx_model_test_results_step ON model_test_results(run_step_id);

ALTER TABLE models ADD COLUMN contract TEXT NOT NULL DEFAULT '{}';

-- +goose Down
DROP TABLE IF EXISTS model_test_results;
DROP TABLE IF EXISTS model_tests;
