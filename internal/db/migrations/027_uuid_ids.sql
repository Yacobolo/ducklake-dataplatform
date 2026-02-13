-- +goose Up
PRAGMA foreign_keys = OFF;

-- ============================================================
-- 1. principals
-- ============================================================
CREATE TABLE principals_new (
    id              TEXT PRIMARY KEY,
    name            TEXT NOT NULL UNIQUE,
    type            TEXT NOT NULL DEFAULT 'user',
    is_admin        INTEGER NOT NULL DEFAULT 0,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    external_id     TEXT,
    external_issuer TEXT
);
INSERT INTO principals_new
    SELECT printf('%d', id), name, type, is_admin, created_at, external_id, external_issuer
    FROM principals;
DROP TABLE principals;
ALTER TABLE principals_new RENAME TO principals;
CREATE UNIQUE INDEX idx_principals_external
    ON principals(external_issuer, external_id) WHERE external_id IS NOT NULL;

-- ============================================================
-- 2. groups
-- ============================================================
CREATE TABLE groups_new (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL UNIQUE,
    description TEXT,
    created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);
INSERT INTO groups_new
    SELECT printf('%d', id), name, description, created_at
    FROM groups;
DROP TABLE groups;
ALTER TABLE groups_new RENAME TO groups;

-- ============================================================
-- 3. group_members (composite key, no autoincrement PK)
-- ============================================================
CREATE TABLE group_members_new (
    group_id    TEXT NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
    member_type TEXT NOT NULL,
    member_id   TEXT NOT NULL,
    PRIMARY KEY (group_id, member_type, member_id)
);
INSERT INTO group_members_new
    SELECT printf('%d', group_id), member_type, printf('%d', member_id)
    FROM group_members;
DROP TABLE group_members;
ALTER TABLE group_members_new RENAME TO group_members;

-- ============================================================
-- 4. privilege_grants
-- ============================================================
CREATE TABLE privilege_grants_new (
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
INSERT INTO privilege_grants_new
    SELECT printf('%d', id),
           printf('%d', principal_id),
           principal_type,
           securable_type,
           printf('%d', securable_id),
           privilege,
           CASE WHEN granted_by IS NOT NULL THEN printf('%d', granted_by) END,
           granted_at
    FROM privilege_grants;
DROP TABLE privilege_grants;
ALTER TABLE privilege_grants_new RENAME TO privilege_grants;
CREATE INDEX idx_grants_principal ON privilege_grants(principal_id, principal_type);
CREATE INDEX idx_grants_securable ON privilege_grants(securable_type, securable_id);

-- ============================================================
-- 5. row_filters
-- ============================================================
CREATE TABLE row_filters_new (
    id         TEXT PRIMARY KEY,
    table_id   TEXT NOT NULL,
    filter_sql TEXT NOT NULL,
    description TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
INSERT INTO row_filters_new
    SELECT printf('%d', id), printf('%d', table_id), filter_sql, description, created_at
    FROM row_filters;
DROP TABLE row_filters;
ALTER TABLE row_filters_new RENAME TO row_filters;

-- ============================================================
-- 6. row_filter_bindings
-- ============================================================
CREATE TABLE row_filter_bindings_new (
    id             TEXT PRIMARY KEY,
    row_filter_id  TEXT NOT NULL REFERENCES row_filters(id) ON DELETE CASCADE,
    principal_id   TEXT NOT NULL,
    principal_type TEXT NOT NULL,
    UNIQUE(row_filter_id, principal_id, principal_type)
);
INSERT INTO row_filter_bindings_new
    SELECT printf('%d', id),
           printf('%d', row_filter_id),
           printf('%d', principal_id),
           principal_type
    FROM row_filter_bindings;
DROP TABLE row_filter_bindings;
ALTER TABLE row_filter_bindings_new RENAME TO row_filter_bindings;

-- ============================================================
-- 7. column_masks
-- ============================================================
CREATE TABLE column_masks_new (
    id              TEXT PRIMARY KEY,
    table_id        TEXT NOT NULL,
    column_name     TEXT NOT NULL,
    mask_expression TEXT NOT NULL,
    description     TEXT,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(table_id, column_name)
);
INSERT INTO column_masks_new
    SELECT printf('%d', id),
           printf('%d', table_id),
           column_name,
           mask_expression,
           description,
           created_at
    FROM column_masks;
DROP TABLE column_masks;
ALTER TABLE column_masks_new RENAME TO column_masks;

-- ============================================================
-- 8. column_mask_bindings
-- ============================================================
CREATE TABLE column_mask_bindings_new (
    id             TEXT PRIMARY KEY,
    column_mask_id TEXT NOT NULL REFERENCES column_masks(id) ON DELETE CASCADE,
    principal_id   TEXT NOT NULL,
    principal_type TEXT NOT NULL,
    see_original   INTEGER NOT NULL DEFAULT 0,
    UNIQUE(column_mask_id, principal_id, principal_type)
);
INSERT INTO column_mask_bindings_new
    SELECT printf('%d', id),
           printf('%d', column_mask_id),
           printf('%d', principal_id),
           principal_type,
           see_original
    FROM column_mask_bindings;
DROP TABLE column_mask_bindings;
ALTER TABLE column_mask_bindings_new RENAME TO column_mask_bindings;

-- ============================================================
-- 9. audit_log
-- ============================================================
CREATE TABLE audit_log_new (
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
INSERT INTO audit_log_new
    SELECT printf('%d', id),
           principal_name,
           action,
           statement_type,
           original_sql,
           rewritten_sql,
           tables_accessed,
           status,
           error_message,
           duration_ms,
           created_at,
           rows_returned
    FROM audit_log;
DROP TABLE audit_log;
ALTER TABLE audit_log_new RENAME TO audit_log;
CREATE INDEX idx_audit_principal ON audit_log(principal_name);
CREATE INDEX idx_audit_created ON audit_log(created_at);
CREATE INDEX idx_audit_status ON audit_log(status);

-- ============================================================
-- 10. api_keys
-- ============================================================
CREATE TABLE api_keys_new (
    id           TEXT PRIMARY KEY,
    key_hash     TEXT NOT NULL UNIQUE,
    principal_id TEXT NOT NULL REFERENCES principals(id) ON DELETE CASCADE,
    name         TEXT NOT NULL,
    expires_at   TEXT,
    created_at   TEXT NOT NULL DEFAULT (datetime('now'))
);
INSERT INTO api_keys_new
    SELECT printf('%d', id),
           key_hash,
           printf('%d', principal_id),
           name,
           expires_at,
           created_at
    FROM api_keys;
DROP TABLE api_keys;
ALTER TABLE api_keys_new RENAME TO api_keys;
CREATE INDEX idx_api_keys_hash ON api_keys(key_hash);

-- ============================================================
-- 11. lineage_edges
-- ============================================================
CREATE TABLE lineage_edges_new (
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
INSERT INTO lineage_edges_new
    SELECT printf('%d', id),
           source_table,
           target_table,
           edge_type,
           principal_name,
           query_hash,
           created_at,
           source_schema,
           target_schema
    FROM lineage_edges;
DROP TABLE lineage_edges;
ALTER TABLE lineage_edges_new RENAME TO lineage_edges;
CREATE INDEX idx_lineage_source ON lineage_edges(source_table);
CREATE INDEX idx_lineage_target ON lineage_edges(target_table);
CREATE INDEX idx_lineage_created ON lineage_edges(created_at);

-- ============================================================
-- 12. tags
-- ============================================================
CREATE TABLE tags_new (
    id         TEXT PRIMARY KEY,
    key        TEXT NOT NULL,
    value      TEXT,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(key, value)
);
INSERT INTO tags_new
    SELECT printf('%d', id), key, value, created_by, created_at
    FROM tags;
DROP TABLE tags;
ALTER TABLE tags_new RENAME TO tags;

-- ============================================================
-- 13. tag_assignments
-- ============================================================
CREATE TABLE tag_assignments_new (
    id             TEXT PRIMARY KEY,
    tag_id         TEXT NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
    securable_type TEXT NOT NULL,
    securable_id   TEXT NOT NULL,
    column_name    TEXT,
    assigned_by    TEXT NOT NULL,
    assigned_at    TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(tag_id, securable_type, securable_id, column_name)
);
INSERT INTO tag_assignments_new
    SELECT printf('%d', id),
           printf('%d', tag_id),
           securable_type,
           printf('%d', securable_id),
           column_name,
           assigned_by,
           assigned_at
    FROM tag_assignments;
DROP TABLE tag_assignments;
ALTER TABLE tag_assignments_new RENAME TO tag_assignments;
CREATE INDEX idx_tag_assignments_securable ON tag_assignments(securable_type, securable_id);

-- ============================================================
-- 14. views
-- ============================================================
CREATE TABLE views_new (
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
INSERT INTO views_new
    SELECT printf('%d', id),
           printf('%d', schema_id),
           name,
           view_definition,
           comment,
           properties,
           owner,
           source_tables,
           created_at,
           updated_at,
           deleted_at
    FROM views;
DROP TABLE views;
ALTER TABLE views_new RENAME TO views;

-- ============================================================
-- 15. storage_credentials
-- ============================================================
CREATE TABLE storage_credentials_new (
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
INSERT INTO storage_credentials_new
    SELECT printf('%d', id),
           name,
           credential_type,
           key_id_encrypted,
           secret_encrypted,
           endpoint,
           region,
           url_style,
           comment,
           owner,
           created_at,
           updated_at,
           azure_account_name,
           azure_account_key_encrypted,
           azure_client_id,
           azure_tenant_id,
           azure_client_secret_encrypted,
           gcs_key_file_path
    FROM storage_credentials;
DROP TABLE storage_credentials;
ALTER TABLE storage_credentials_new RENAME TO storage_credentials;

-- ============================================================
-- 16. external_locations
-- ============================================================
CREATE TABLE external_locations_new (
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
INSERT INTO external_locations_new
    SELECT printf('%d', id),
           name,
           url,
           credential_name,
           storage_type,
           comment,
           owner,
           read_only,
           created_at,
           updated_at
    FROM external_locations;
DROP TABLE external_locations;
ALTER TABLE external_locations_new RENAME TO external_locations;

-- ============================================================
-- 17. external_tables
-- ============================================================
CREATE TABLE external_tables_new (
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
    deleted_at    TEXT,
    UNIQUE(schema_name, table_name)
);
INSERT INTO external_tables_new
    SELECT printf('%d', id),
           schema_name,
           table_name,
           file_format,
           source_path,
           location_name,
           comment,
           owner,
           created_at,
           updated_at,
           deleted_at
    FROM external_tables;
DROP TABLE external_tables;
ALTER TABLE external_tables_new RENAME TO external_tables;

-- ============================================================
-- 18. external_table_columns
-- ============================================================
CREATE TABLE external_table_columns_new (
    id                TEXT PRIMARY KEY,
    external_table_id TEXT NOT NULL REFERENCES external_tables(id) ON DELETE CASCADE,
    column_name       TEXT NOT NULL,
    column_type       TEXT NOT NULL,
    position          INTEGER NOT NULL,
    UNIQUE(external_table_id, column_name)
);
INSERT INTO external_table_columns_new
    SELECT printf('%d', id),
           printf('%d', external_table_id),
           column_name,
           column_type,
           position
    FROM external_table_columns;
DROP TABLE external_table_columns;
ALTER TABLE external_table_columns_new RENAME TO external_table_columns;

-- ============================================================
-- 19. volumes
-- ============================================================
CREATE TABLE volumes_new (
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
INSERT INTO volumes_new
    SELECT printf('%d', id),
           name,
           schema_name,
           catalog_name,
           volume_type,
           storage_location,
           comment,
           owner,
           created_at,
           updated_at
    FROM volumes;
DROP TABLE volumes;
ALTER TABLE volumes_new RENAME TO volumes;

-- ============================================================
-- 20. compute_endpoints
-- ============================================================
CREATE TABLE compute_endpoints_new (
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
INSERT INTO compute_endpoints_new
    SELECT printf('%d', id),
           external_id,
           name,
           url,
           type,
           status,
           size,
           max_memory_gb,
           auth_token,
           owner,
           created_at,
           updated_at
    FROM compute_endpoints;
DROP TABLE compute_endpoints;
ALTER TABLE compute_endpoints_new RENAME TO compute_endpoints;

-- ============================================================
-- 21. compute_assignments
-- ============================================================
CREATE TABLE compute_assignments_new (
    id             TEXT PRIMARY KEY,
    principal_id   TEXT NOT NULL,
    principal_type TEXT NOT NULL CHECK (principal_type IN ('user','group')),
    endpoint_id    TEXT NOT NULL REFERENCES compute_endpoints(id) ON DELETE CASCADE,
    is_default     INTEGER NOT NULL DEFAULT 1,
    fallback_local INTEGER NOT NULL DEFAULT 0,
    created_at     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(principal_id, principal_type, endpoint_id)
);
INSERT INTO compute_assignments_new
    SELECT printf('%d', id),
           printf('%d', principal_id),
           principal_type,
           printf('%d', endpoint_id),
           is_default,
           fallback_local,
           created_at
    FROM compute_assignments;
DROP TABLE compute_assignments;
ALTER TABLE compute_assignments_new RENAME TO compute_assignments;

-- ============================================================
-- 22. catalogs
-- ============================================================
CREATE TABLE catalogs_new (
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
INSERT INTO catalogs_new
    SELECT printf('%d', id),
           name,
           metastore_type,
           dsn,
           data_path,
           status,
           status_message,
           is_default,
           comment,
           created_at,
           updated_at
    FROM catalogs;
DROP TABLE catalogs;
ALTER TABLE catalogs_new RENAME TO catalogs;
CREATE UNIQUE INDEX idx_catalogs_default ON catalogs(is_default) WHERE is_default = 1;

PRAGMA foreign_keys = ON;

-- +goose Down
-- This migration is not reversible because it changes all primary key and
-- foreign key columns from INTEGER to TEXT across every table.  A rollback
-- would require the exact inverse rename-copy-drop-rename for each table
-- and is intentionally omitted.
