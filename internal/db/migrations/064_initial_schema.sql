-- +goose Up
CREATE TABLE catalog_metadata (
    securable_type TEXT NOT NULL,   -- 'schema' or 'table'
    securable_name TEXT NOT NULL,   -- e.g. 'main' or 'main.titanic'
    comment        TEXT,
    properties     TEXT,            -- JSON key-value map
    owner          TEXT,
    created_at     TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at     TEXT NOT NULL DEFAULT (datetime('now')), deleted_at TEXT,
    UNIQUE(securable_type, securable_name)
);
CREATE TABLE column_metadata (
    table_securable_name TEXT NOT NULL,
    column_name          TEXT NOT NULL,
    comment              TEXT,
    properties           TEXT,
    updated_at           TEXT DEFAULT (datetime('now')),
    UNIQUE(table_securable_name, column_name)
);
CREATE TABLE table_statistics (
    table_securable_name TEXT NOT NULL UNIQUE,
    row_count            INTEGER,
    size_bytes           INTEGER,
    column_count         INTEGER,
    last_profiled_at     TEXT DEFAULT (datetime('now')),
    profiled_by          TEXT
);
CREATE TABLE IF NOT EXISTS "principals" (
    id              TEXT PRIMARY KEY,
    name            TEXT NOT NULL UNIQUE,
    type            TEXT NOT NULL DEFAULT 'user',
    is_admin        INTEGER NOT NULL DEFAULT 0,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    external_id     TEXT,
    external_issuer TEXT
);
CREATE TABLE IF NOT EXISTS "groups" (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL UNIQUE,
    description TEXT,
    created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS "group_members" (
    group_id    TEXT NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
    member_type TEXT NOT NULL,
    member_id   TEXT NOT NULL,
    PRIMARY KEY (group_id, member_type, member_id)
);
CREATE TABLE IF NOT EXISTS "privilege_grants" (
    id             TEXT PRIMARY KEY,
    principal_id   TEXT NOT NULL,
    principal_type TEXT NOT NULL,
    securable_type TEXT NOT NULL,
    securable_id   TEXT NOT NULL,
    privilege      TEXT NOT NULL,
    granted_by     TEXT,
    granted_at     TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(principal_id, principal_type, securable_type, securable_id, privilege)
);
CREATE TABLE IF NOT EXISTS "row_filters" (
    id         TEXT PRIMARY KEY,
    table_id   TEXT NOT NULL,
    filter_sql TEXT NOT NULL,
    description TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
, name TEXT);
CREATE TABLE IF NOT EXISTS "row_filter_bindings" (
    id             TEXT PRIMARY KEY,
    row_filter_id  TEXT NOT NULL REFERENCES row_filters(id) ON DELETE CASCADE,
    principal_id   TEXT NOT NULL,
    principal_type TEXT NOT NULL,
    UNIQUE(row_filter_id, principal_id, principal_type)
);
CREATE TABLE IF NOT EXISTS "column_masks" (
    id              TEXT PRIMARY KEY,
    table_id        TEXT NOT NULL,
    column_name     TEXT NOT NULL,
    mask_expression TEXT NOT NULL,
    description     TEXT,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')), name TEXT,
    UNIQUE(table_id, column_name)
);
CREATE TABLE IF NOT EXISTS "column_mask_bindings" (
    id             TEXT PRIMARY KEY,
    column_mask_id TEXT NOT NULL REFERENCES column_masks(id) ON DELETE CASCADE,
    principal_id   TEXT NOT NULL,
    principal_type TEXT NOT NULL,
    see_original   INTEGER NOT NULL DEFAULT 0,
    UNIQUE(column_mask_id, principal_id, principal_type)
);
CREATE TABLE IF NOT EXISTS "audit_log" (
    id            TEXT PRIMARY KEY,
    principal_name TEXT NOT NULL,
    action        TEXT NOT NULL,
    statement_type TEXT,
    original_sql  TEXT,
    rewritten_sql TEXT,
    tables_accessed TEXT,
    status        TEXT NOT NULL,
    error_message TEXT,
    duration_ms   INTEGER,
    created_at    TEXT NOT NULL DEFAULT (datetime('now')),
    rows_returned INTEGER
);
CREATE TABLE IF NOT EXISTS "api_keys" (
    id           TEXT PRIMARY KEY,
    key_hash     TEXT NOT NULL UNIQUE,
    principal_id TEXT NOT NULL REFERENCES principals(id) ON DELETE CASCADE,
    name         TEXT NOT NULL,
    expires_at   TEXT,
    created_at   TEXT NOT NULL DEFAULT (datetime('now'))
, key_prefix TEXT);
CREATE TABLE IF NOT EXISTS "lineage_edges" (
    id             TEXT PRIMARY KEY,
    source_table   TEXT NOT NULL,
    target_table   TEXT,
    edge_type      TEXT NOT NULL,
    principal_name TEXT NOT NULL,
    query_hash     TEXT,
    created_at     TEXT NOT NULL DEFAULT (datetime('now')),
    source_schema  TEXT,
    target_schema  TEXT
);
CREATE TABLE IF NOT EXISTS "tags" (
    id         TEXT PRIMARY KEY,
    key        TEXT NOT NULL,
    value      TEXT,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(key, value)
);
CREATE TABLE IF NOT EXISTS "tag_assignments" (
    id             TEXT PRIMARY KEY,
    tag_id         TEXT NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
    securable_type TEXT NOT NULL,
    securable_id   TEXT NOT NULL,
    column_name    TEXT,
    assigned_by    TEXT NOT NULL,
    assigned_at    TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(tag_id, securable_type, securable_id, column_name)
);
CREATE TABLE IF NOT EXISTS "views" (
    id              TEXT PRIMARY KEY,
    schema_id       TEXT NOT NULL,
    name            TEXT NOT NULL,
    view_definition TEXT NOT NULL,
    comment         TEXT,
    properties      TEXT DEFAULT '{}',
    owner           TEXT NOT NULL,
    source_tables   TEXT,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at      TEXT NOT NULL DEFAULT (datetime('now')),
    deleted_at      TEXT,
    UNIQUE(schema_id, name)
);
CREATE TABLE IF NOT EXISTS "storage_credentials" (
    id                           TEXT PRIMARY KEY,
    name                         TEXT NOT NULL UNIQUE,
    credential_type              TEXT NOT NULL DEFAULT 'S3',
    key_id_encrypted             TEXT NOT NULL,
    secret_encrypted             TEXT NOT NULL,
    endpoint                     TEXT NOT NULL,
    region                       TEXT NOT NULL,
    url_style                    TEXT NOT NULL DEFAULT 'path',
    comment                      TEXT NOT NULL DEFAULT '',
    owner                        TEXT NOT NULL DEFAULT '',
    created_at                   TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at                   TEXT NOT NULL DEFAULT (datetime('now')),
    azure_account_name           TEXT NOT NULL DEFAULT '',
    azure_account_key_encrypted  TEXT NOT NULL DEFAULT '',
    azure_client_id              TEXT NOT NULL DEFAULT '',
    azure_tenant_id              TEXT NOT NULL DEFAULT '',
    azure_client_secret_encrypted TEXT NOT NULL DEFAULT '',
    gcs_key_file_path            TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS "external_locations" (
    id              TEXT PRIMARY KEY,
    name            TEXT NOT NULL UNIQUE,
    url             TEXT NOT NULL,
    credential_name TEXT NOT NULL REFERENCES storage_credentials(name),
    storage_type    TEXT NOT NULL DEFAULT 'S3',
    comment         TEXT NOT NULL DEFAULT '',
    owner           TEXT NOT NULL DEFAULT '',
    read_only       INTEGER NOT NULL DEFAULT 0,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS "external_tables" (
    id            TEXT PRIMARY KEY,
    schema_name   TEXT NOT NULL,
    table_name    TEXT NOT NULL,
    file_format   TEXT NOT NULL DEFAULT 'parquet',
    source_path   TEXT NOT NULL,
    location_name TEXT NOT NULL,
    comment       TEXT NOT NULL DEFAULT '',
    owner         TEXT NOT NULL DEFAULT '',
    created_at    TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at    TEXT NOT NULL DEFAULT (datetime('now')),
    deleted_at    TEXT, catalog_name TEXT NOT NULL DEFAULT 'lake',
    UNIQUE(schema_name, table_name)
);
CREATE TABLE IF NOT EXISTS "external_table_columns" (
    id                TEXT PRIMARY KEY,
    external_table_id TEXT NOT NULL REFERENCES external_tables(id) ON DELETE CASCADE,
    column_name       TEXT NOT NULL,
    column_type       TEXT NOT NULL,
    position          INTEGER NOT NULL,
    UNIQUE(external_table_id, column_name)
);
CREATE TABLE IF NOT EXISTS "volumes" (
    id               TEXT PRIMARY KEY,
    name             TEXT NOT NULL,
    schema_name      TEXT NOT NULL,
    catalog_name     TEXT NOT NULL DEFAULT 'lake',
    volume_type      TEXT NOT NULL CHECK(volume_type IN ('MANAGED', 'EXTERNAL')),
    storage_location TEXT NOT NULL DEFAULT '',
    comment          TEXT NOT NULL DEFAULT '',
    owner            TEXT NOT NULL DEFAULT '',
    created_at       TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at       TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(catalog_name, schema_name, name)
);
CREATE TABLE IF NOT EXISTS "compute_endpoints" (
    id            TEXT PRIMARY KEY,
    external_id   TEXT NOT NULL UNIQUE,
    name          TEXT NOT NULL UNIQUE,
    url           TEXT NOT NULL,
    type          TEXT NOT NULL DEFAULT 'REMOTE' CHECK (type IN ('LOCAL','REMOTE')),
    status        TEXT NOT NULL DEFAULT 'INACTIVE' CHECK (status IN ('ACTIVE','INACTIVE','STARTING','STOPPING','ERROR')),
    size          TEXT NOT NULL DEFAULT '',
    max_memory_gb INTEGER,
    auth_token    TEXT NOT NULL DEFAULT '',
    owner         TEXT NOT NULL DEFAULT '',
    created_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS "compute_assignments" (
    id             TEXT PRIMARY KEY,
    principal_id   TEXT NOT NULL,
    principal_type TEXT NOT NULL CHECK (principal_type IN ('user','group')),
    endpoint_id    TEXT NOT NULL REFERENCES compute_endpoints(id) ON DELETE CASCADE,
    is_default     INTEGER NOT NULL DEFAULT 1,
    fallback_local INTEGER NOT NULL DEFAULT 0,
    created_at     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(principal_id, principal_type, endpoint_id)
);
CREATE TABLE IF NOT EXISTS "catalogs" (
    id             TEXT PRIMARY KEY,
    name           TEXT NOT NULL UNIQUE,
    metastore_type TEXT NOT NULL DEFAULT 'sqlite',
    dsn            TEXT NOT NULL,
    data_path      TEXT NOT NULL,
    status         TEXT NOT NULL DEFAULT 'DETACHED',
    status_message TEXT,
    is_default     INTEGER NOT NULL DEFAULT 0,
    comment        TEXT,
    created_at     TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at     TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE TABLE notebooks (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    description TEXT,
    owner       TEXT NOT NULL,
    created_at  TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
, git_repo_id TEXT REFERENCES git_repos(id) ON DELETE SET NULL, git_path TEXT);
CREATE TABLE cells (
    id          TEXT PRIMARY KEY,
    notebook_id TEXT NOT NULL REFERENCES notebooks(id) ON DELETE CASCADE,
    cell_type   TEXT NOT NULL CHECK (cell_type IN ('sql', 'markdown')),
    content     TEXT NOT NULL DEFAULT '',
    position    INTEGER NOT NULL DEFAULT 0,
    last_result TEXT,
    created_at  TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
, name TEXT, role TEXT NOT NULL DEFAULT 'transform' CHECK (role IN ('transform','output','test','markdown')), disabled INTEGER NOT NULL DEFAULT 0 CHECK (disabled IN (0,1)), test_config TEXT NOT NULL DEFAULT '{}');
CREATE TABLE notebook_jobs (
    id          TEXT PRIMARY KEY,
    notebook_id TEXT NOT NULL REFERENCES notebooks(id) ON DELETE CASCADE,
    session_id  TEXT NOT NULL,
    state       TEXT NOT NULL DEFAULT 'pending' CHECK (state IN ('pending', 'running', 'complete', 'failed')),
    result      TEXT,
    error       TEXT,
    created_at  TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE TABLE git_repos (
    id             TEXT PRIMARY KEY,
    url            TEXT NOT NULL,
    branch         TEXT NOT NULL DEFAULT 'main',
    path           TEXT NOT NULL DEFAULT '',
    auth_token     TEXT NOT NULL DEFAULT '',
    webhook_secret TEXT,
    owner          TEXT NOT NULL,
    last_sync_at   TEXT,
    last_commit    TEXT,
    created_at     TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at     TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE TABLE pipelines (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    description TEXT NOT NULL DEFAULT '',
    schedule_cron TEXT,
    is_paused INTEGER NOT NULL DEFAULT 0,
    concurrency_limit INTEGER NOT NULL DEFAULT 1,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE TABLE pipeline_jobs (
    id TEXT PRIMARY KEY,
    pipeline_id TEXT NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    compute_endpoint_id TEXT,
    depends_on TEXT NOT NULL DEFAULT '[]',
    notebook_id TEXT NOT NULL,
    timeout_seconds INTEGER,
    retry_count INTEGER NOT NULL DEFAULT 0,
    job_order INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now')), job_type TEXT NOT NULL DEFAULT 'NOTEBOOK', model_selector TEXT NOT NULL DEFAULT '',
    UNIQUE(pipeline_id, name)
);
CREATE TABLE pipeline_runs (
    id TEXT PRIMARY KEY,
    pipeline_id TEXT NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    status TEXT NOT NULL DEFAULT 'PENDING'
           CHECK (status IN ('PENDING','RUNNING','SUCCESS','FAILED','CANCELLED')),
    trigger_type TEXT NOT NULL CHECK (trigger_type IN ('MANUAL','SCHEDULED')),
    triggered_by TEXT NOT NULL,
    parameters TEXT NOT NULL DEFAULT '{}',
    git_commit_hash TEXT,
    started_at TEXT,
    finished_at TEXT,
    error_message TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE TABLE pipeline_job_runs (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES pipeline_runs(id) ON DELETE CASCADE,
    job_id TEXT NOT NULL,
    job_name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING'
           CHECK (status IN ('PENDING','RUNNING','SUCCESS','FAILED','SKIPPED','CANCELLED')),
    started_at TEXT,
    finished_at TEXT,
    error_message TEXT,
    retry_attempt INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE TABLE column_lineage_edges (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    lineage_edge_id TEXT NOT NULL REFERENCES lineage_edges(id) ON DELETE CASCADE,
    target_column   TEXT NOT NULL,
    source_schema   TEXT NOT NULL,
    source_table    TEXT NOT NULL,
    source_column   TEXT NOT NULL,
    transform_type  TEXT NOT NULL CHECK (transform_type IN ('DIRECT', 'EXPRESSION')),
    function_name   TEXT NOT NULL DEFAULT '',
    created_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE models (
    id TEXT PRIMARY KEY,
    project_name TEXT NOT NULL,
    name TEXT NOT NULL,
    sql_body TEXT NOT NULL,
    materialization TEXT NOT NULL DEFAULT 'VIEW'
        CHECK (materialization IN ('VIEW','TABLE','INCREMENTAL','EPHEMERAL')),
    description TEXT NOT NULL DEFAULT '',
    owner TEXT NOT NULL DEFAULT '',
    tags TEXT NOT NULL DEFAULT '[]',
    depends_on TEXT NOT NULL DEFAULT '[]',
    config TEXT NOT NULL DEFAULT '{}',
    created_by TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')), contract TEXT NOT NULL DEFAULT '{}', freshness_max_lag INTEGER, freshness_cron TEXT,
    UNIQUE(project_name, name)
);
CREATE TABLE model_runs (
    id TEXT PRIMARY KEY,
    status TEXT NOT NULL DEFAULT 'PENDING'
        CHECK (status IN ('PENDING','RUNNING','SUCCESS','FAILED','CANCELLED')),
    trigger_type TEXT NOT NULL CHECK (trigger_type IN ('MANUAL','SCHEDULED','PIPELINE')),
    triggered_by TEXT NOT NULL,
    target_catalog TEXT NOT NULL DEFAULT '',
    target_schema TEXT NOT NULL DEFAULT '',
    model_selector TEXT NOT NULL DEFAULT '',
    variables TEXT NOT NULL DEFAULT '{}',
    started_at TEXT,
    finished_at TEXT,
    error_message TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
, full_refresh INTEGER NOT NULL DEFAULT 0, compile_manifest TEXT NOT NULL DEFAULT '{}', compile_diagnostics TEXT NOT NULL DEFAULT '{}');
CREATE TABLE model_run_steps (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES model_runs(id) ON DELETE CASCADE,
    model_id TEXT NOT NULL,
    model_name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING'
        CHECK (status IN ('PENDING','RUNNING','SUCCESS','FAILED','SKIPPED','CANCELLED')),
    tier INTEGER NOT NULL DEFAULT 0,
    rows_affected INTEGER,
    started_at TEXT,
    finished_at TEXT,
    error_message TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
, compiled_sql TEXT, compiled_hash TEXT, depends_on TEXT NOT NULL DEFAULT '[]', vars_used TEXT NOT NULL DEFAULT '[]', macros_used TEXT NOT NULL DEFAULT '[]');
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
CREATE TABLE macros (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    macro_type TEXT NOT NULL CHECK (macro_type IN ('SCALAR','TABLE')),
    parameters TEXT NOT NULL DEFAULT '[]',
    body TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    created_by TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
, catalog_name TEXT NOT NULL DEFAULT '', project_name TEXT NOT NULL DEFAULT '', visibility TEXT NOT NULL DEFAULT 'project', owner TEXT NOT NULL DEFAULT '', properties TEXT NOT NULL DEFAULT '{}', tags TEXT NOT NULL DEFAULT '[]', status TEXT NOT NULL DEFAULT 'ACTIVE');
CREATE TABLE macro_revisions (
    id TEXT PRIMARY KEY,
    macro_id TEXT NOT NULL,
    macro_name TEXT NOT NULL,
    version INTEGER NOT NULL,
    content_hash TEXT NOT NULL,
    parameters TEXT NOT NULL DEFAULT '[]',
    body TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'ACTIVE',
    created_by TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (macro_id, version),
    FOREIGN KEY (macro_id) REFERENCES macros(id) ON DELETE CASCADE
);
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
CREATE TABLE query_jobs (
  id TEXT PRIMARY KEY,
  principal_name TEXT NOT NULL,
  request_id TEXT NOT NULL,
  sql_text TEXT NOT NULL,
  status TEXT NOT NULL,
  columns_json TEXT,
  rows_json TEXT,
  row_count INTEGER NOT NULL DEFAULT 0,
  error_message TEXT,
  started_at DATETIME,
  completed_at DATETIME,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, attempt_count INTEGER NOT NULL DEFAULT 0, max_attempts INTEGER NOT NULL DEFAULT 1, last_heartbeat_at DATETIME, next_retry_at DATETIME,
  UNIQUE (principal_name, request_id)
);
CREATE TABLE auth_identities (
  id TEXT PRIMARY KEY,
  principal_id TEXT NOT NULL,
  provider TEXT NOT NULL,
  issuer TEXT,
  subject TEXT NOT NULL,
  email TEXT,
  email_verified INTEGER NOT NULL DEFAULT 0,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE (provider, issuer, subject),
  FOREIGN KEY (principal_id) REFERENCES principals(id) ON DELETE CASCADE
);
CREATE TABLE local_credentials (
  principal_id TEXT PRIMARY KEY,
  username TEXT NOT NULL UNIQUE,
  password_hash TEXT NOT NULL,
  password_changed_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  must_change_password INTEGER NOT NULL DEFAULT 0,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (principal_id) REFERENCES principals(id) ON DELETE CASCADE
);
CREATE TABLE auth_login_attempts (
  id TEXT PRIMARY KEY,
  username TEXT,
  ip_address TEXT,
  success INTEGER NOT NULL,
  reason TEXT,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE auth_recovery_codes (
  id TEXT PRIMARY KEY,
  principal_id TEXT NOT NULL,
  code_hash TEXT NOT NULL UNIQUE,
  used_at DATETIME,
  expires_at DATETIME NOT NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (principal_id) REFERENCES principals(id) ON DELETE CASCADE
);
CREATE TABLE setup_state (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  setup_completed INTEGER NOT NULL DEFAULT 0,
  setup_completed_at DATETIME,
  setup_completed_by TEXT,
  bootstrap_token_hash TEXT,
  bootstrap_token_expires_at DATETIME,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (setup_completed_by) REFERENCES principals(id) ON DELETE SET NULL
);
CREATE TABLE auth_providers (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  oidc_enabled INTEGER NOT NULL DEFAULT 0,
  oidc_issuer_url TEXT,
  oidc_jwks_url TEXT,
  oidc_audience TEXT,
  oidc_client_id TEXT,
  oidc_client_secret_enc TEXT,
  oidc_scopes TEXT,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE webauthn_credentials (
  id TEXT PRIMARY KEY,
  principal_id TEXT NOT NULL,
  credential_id TEXT NOT NULL UNIQUE,
  public_key TEXT NOT NULL,
  sign_count INTEGER NOT NULL DEFAULT 0,
  transports TEXT,
  backup_eligible INTEGER NOT NULL DEFAULT 0,
  backup_state INTEGER NOT NULL DEFAULT 0,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  last_used_at DATETIME,
  FOREIGN KEY (principal_id) REFERENCES principals(id) ON DELETE CASCADE
);
CREATE TABLE web_sessions (
  id TEXT PRIMARY KEY,
  principal_id TEXT NOT NULL,
  session_hash TEXT NOT NULL UNIQUE,
  auth_method TEXT NOT NULL,
  user_agent TEXT,
  ip_address TEXT,
  expires_at DATETIME NOT NULL,
  idle_expires_at DATETIME NOT NULL,
  last_seen_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  revoked_at DATETIME,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (principal_id) REFERENCES principals(id) ON DELETE CASCADE
);
CREATE TABLE notebook_model_links (
    id TEXT PRIMARY KEY,
    notebook_id TEXT NOT NULL REFERENCES notebooks(id) ON DELETE CASCADE,
    model_id TEXT NOT NULL REFERENCES models(id) ON DELETE CASCADE,
    output_cell_id TEXT NOT NULL REFERENCES cells(id) ON DELETE RESTRICT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(notebook_id),
    UNIQUE(model_id)
);
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
CREATE TABLE asset_dependencies (
    id TEXT PRIMARY KEY,
    asset_id TEXT NOT NULL REFERENCES data_assets(id) ON DELETE CASCADE,
    upstream_asset_id TEXT NOT NULL REFERENCES data_assets(id) ON DELETE CASCADE,
    dependency_type TEXT NOT NULL DEFAULT 'HARD',
    partition_mapping_json TEXT NOT NULL DEFAULT '{}',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(asset_id, upstream_asset_id, dependency_type)
);
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
, partition_from TEXT, partition_to TEXT);
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
CREATE UNIQUE INDEX idx_principals_external
    ON principals(external_issuer, external_id) WHERE external_id IS NOT NULL;
CREATE INDEX idx_grants_principal ON privilege_grants(principal_id, principal_type);
CREATE INDEX idx_grants_securable ON privilege_grants(securable_type, securable_id);
CREATE INDEX idx_audit_principal ON audit_log(principal_name);
CREATE INDEX idx_audit_created ON audit_log(created_at);
CREATE INDEX idx_audit_status ON audit_log(status);
CREATE INDEX idx_api_keys_hash ON api_keys(key_hash);
CREATE INDEX idx_lineage_source ON lineage_edges(source_table);
CREATE INDEX idx_lineage_target ON lineage_edges(target_table);
CREATE INDEX idx_lineage_created ON lineage_edges(created_at);
CREATE INDEX idx_tag_assignments_securable ON tag_assignments(securable_type, securable_id);
CREATE UNIQUE INDEX idx_catalogs_default ON catalogs(is_default) WHERE is_default = 1;
CREATE INDEX idx_notebooks_owner ON notebooks(owner);
CREATE INDEX idx_cells_notebook ON cells(notebook_id, position);
CREATE INDEX idx_notebook_jobs_notebook ON notebook_jobs(notebook_id);
CREATE INDEX idx_pipeline_runs_pipeline ON pipeline_runs(pipeline_id);
CREATE INDEX idx_pipeline_runs_status ON pipeline_runs(status);
CREATE INDEX idx_pipeline_job_runs_run ON pipeline_job_runs(run_id);
CREATE INDEX idx_col_lineage_edge_id ON column_lineage_edges(lineage_edge_id);
CREATE INDEX idx_col_lineage_source ON column_lineage_edges(source_schema, source_table, source_column);
CREATE INDEX idx_models_project ON models(project_name);
CREATE INDEX idx_model_runs_status ON model_runs(status);
CREATE INDEX idx_model_run_steps_run ON model_run_steps(run_id);
CREATE INDEX idx_model_tests_model ON model_tests(model_id);
CREATE INDEX idx_model_test_results_step ON model_test_results(run_step_id);
CREATE INDEX idx_macros_visibility ON macros(visibility);
CREATE INDEX idx_macros_project ON macros(project_name);
CREATE INDEX idx_macro_revisions_name_version ON macro_revisions(macro_name, version DESC);
CREATE INDEX idx_semantic_models_project ON semantic_models(project_name);
CREATE INDEX idx_semantic_metrics_model ON semantic_metrics(semantic_model_id);
CREATE INDEX idx_semantic_relationships_from ON semantic_relationships(from_semantic_id);
CREATE INDEX idx_semantic_relationships_to ON semantic_relationships(to_semantic_id);
CREATE INDEX idx_semantic_pre_aggs_model ON semantic_pre_aggregations(semantic_model_id);
CREATE INDEX idx_query_jobs_principal_created_at ON query_jobs(principal_name, created_at DESC);
CREATE INDEX idx_query_jobs_status ON query_jobs(status);
CREATE INDEX idx_query_jobs_next_retry_at ON query_jobs(next_retry_at);
CREATE INDEX idx_auth_identities_principal_id ON auth_identities(principal_id);
CREATE INDEX idx_auth_login_attempts_username_created_at ON auth_login_attempts(username, created_at DESC);
CREATE INDEX idx_auth_login_attempts_ip_created_at ON auth_login_attempts(ip_address, created_at DESC);
CREATE INDEX idx_auth_recovery_codes_principal_id ON auth_recovery_codes(principal_id);
CREATE INDEX idx_webauthn_credentials_principal_id ON webauthn_credentials(principal_id);
CREATE INDEX idx_web_sessions_principal_active
  ON web_sessions(principal_id, revoked_at, expires_at, idle_expires_at);
CREATE INDEX idx_web_sessions_reaper
  ON web_sessions(revoked_at, expires_at, idle_expires_at);
CREATE UNIQUE INDEX idx_cells_notebook_name_unique
ON cells(notebook_id, name)
WHERE name IS NOT NULL AND name <> '';
CREATE UNIQUE INDEX idx_cells_notebook_output_unique
ON cells(notebook_id)
WHERE role = 'output';
CREATE INDEX idx_notebook_model_links_notebook_id ON notebook_model_links(notebook_id);
CREATE INDEX idx_notebook_model_links_model_id ON notebook_model_links(model_id);
CREATE INDEX idx_data_assets_type_active ON data_assets(asset_type, is_active);
CREATE INDEX idx_data_assets_owner ON data_assets(owner);
CREATE INDEX idx_asset_dependencies_asset ON asset_dependencies(asset_id);
CREATE INDEX idx_asset_dependencies_upstream ON asset_dependencies(upstream_asset_id);
CREATE INDEX idx_asset_partitions_asset_status ON asset_partitions(asset_id, status);
CREATE INDEX idx_asset_runs_asset_created ON asset_runs(asset_id, created_at DESC);
CREATE INDEX idx_asset_runs_status ON asset_runs(status);
CREATE INDEX idx_asset_runs_group ON asset_runs(run_group_id);
CREATE INDEX idx_asset_run_events_run_created ON asset_run_events(run_id, created_at DESC);
CREATE INDEX idx_asset_materializations_asset_created ON asset_materializations(asset_id, created_at DESC);
CREATE INDEX idx_asset_materializations_run ON asset_materializations(run_id);
CREATE INDEX idx_asset_checks_asset ON asset_checks(asset_id);
CREATE INDEX idx_asset_check_results_check_created ON asset_check_results(check_id, created_at DESC);
CREATE INDEX idx_asset_check_results_run ON asset_check_results(run_id);
CREATE INDEX idx_orchestration_events_status_available ON orchestration_events(status, available_at);
CREATE INDEX idx_backfill_requests_asset_status ON backfill_requests(asset_id, status);
CREATE INDEX idx_backfill_slices_request_status ON backfill_slices(request_id, status);
CREATE INDEX idx_orchestration_events_status_updated
    ON orchestration_events(status, updated_at);
CREATE UNIQUE INDEX idx_row_filters_table_name ON row_filters(table_id, name);
CREATE UNIQUE INDEX idx_column_masks_table_name ON column_masks(table_id, name);

INSERT INTO tags (id, key, value, created_by) VALUES
    ('1', 'classification', 'pii', 'system'),
    ('2', 'classification', 'sensitive', 'system'),
    ('3', 'classification', 'confidential', 'system'),
    ('4', 'classification', 'public', 'system'),
    ('5', 'classification', 'personal_data', 'system'),
    ('6', 'sensitivity', 'high', 'system'),
    ('7', 'sensitivity', 'medium', 'system'),
    ('8', 'sensitivity', 'low', 'system');

INSERT INTO setup_state (id, setup_completed) VALUES (1, 0);
INSERT INTO auth_providers (id, oidc_enabled) VALUES (1, 0);

-- +goose Down
PRAGMA foreign_keys = OFF;
DROP TABLE IF EXISTS backfill_slices;
DROP TABLE IF EXISTS backfill_requests;
DROP TABLE IF EXISTS orchestration_events;
DROP TABLE IF EXISTS asset_check_results;
DROP TABLE IF EXISTS asset_checks;
DROP TABLE IF EXISTS asset_materializations;
DROP TABLE IF EXISTS asset_run_events;
DROP TABLE IF EXISTS asset_runs;
DROP TABLE IF EXISTS asset_partitions;
DROP TABLE IF EXISTS asset_dependencies;
DROP TABLE IF EXISTS data_assets;
DROP TABLE IF EXISTS notebook_model_links;
DROP TABLE IF EXISTS web_sessions;
DROP TABLE IF EXISTS webauthn_credentials;
DROP TABLE IF EXISTS auth_providers;
DROP TABLE IF EXISTS setup_state;
DROP TABLE IF EXISTS auth_recovery_codes;
DROP TABLE IF EXISTS auth_login_attempts;
DROP TABLE IF EXISTS local_credentials;
DROP TABLE IF EXISTS auth_identities;
DROP TABLE IF EXISTS query_jobs;
DROP TABLE IF EXISTS semantic_pre_aggregations;
DROP TABLE IF EXISTS semantic_relationships;
DROP TABLE IF EXISTS semantic_metrics;
DROP TABLE IF EXISTS semantic_models;
DROP TABLE IF EXISTS macro_revisions;
DROP TABLE IF EXISTS macros;
DROP TABLE IF EXISTS model_test_results;
DROP TABLE IF EXISTS model_tests;
DROP TABLE IF EXISTS model_run_steps;
DROP TABLE IF EXISTS model_runs;
DROP TABLE IF EXISTS models;
DROP TABLE IF EXISTS column_lineage_edges;
DROP TABLE IF EXISTS pipeline_job_runs;
DROP TABLE IF EXISTS pipeline_runs;
DROP TABLE IF EXISTS pipeline_jobs;
DROP TABLE IF EXISTS pipelines;
DROP TABLE IF EXISTS git_repos;
DROP TABLE IF EXISTS notebook_jobs;
DROP TABLE IF EXISTS cells;
DROP TABLE IF EXISTS notebooks;
DROP TABLE IF EXISTS compute_assignments;
DROP TABLE IF EXISTS compute_endpoints;
DROP TABLE IF EXISTS volumes;
DROP TABLE IF EXISTS external_table_columns;
DROP TABLE IF EXISTS external_tables;
DROP TABLE IF EXISTS external_locations;
DROP TABLE IF EXISTS storage_credentials;
DROP TABLE IF EXISTS views;
DROP TABLE IF EXISTS tag_assignments;
DROP TABLE IF EXISTS tags;
DROP TABLE IF EXISTS lineage_edges;
DROP TABLE IF EXISTS api_keys;
DROP TABLE IF EXISTS audit_log;
DROP TABLE IF EXISTS column_mask_bindings;
DROP TABLE IF EXISTS column_masks;
DROP TABLE IF EXISTS row_filter_bindings;
DROP TABLE IF EXISTS row_filters;
DROP TABLE IF EXISTS privilege_grants;
DROP TABLE IF EXISTS group_members;
DROP TABLE IF EXISTS groups;
DROP TABLE IF EXISTS principals;
DROP TABLE IF EXISTS table_statistics;
DROP TABLE IF EXISTS column_metadata;
DROP TABLE IF EXISTS catalogs;
DROP TABLE IF EXISTS catalog_metadata;
