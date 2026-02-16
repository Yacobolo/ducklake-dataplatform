package domain

import (
	"context"
	"database/sql"
)

// QueryEngine executes SQL queries with RBAC enforcement.
// Implemented by engine.SecureEngine.
type QueryEngine interface {
	Query(ctx context.Context, principalName, sqlQuery string) (*sql.Rows, error)
}

// SessionEngine extends QueryEngine to support pinned-connection execution.
// Implemented by engine.SecureEngine.
type SessionEngine interface {
	QueryEngine
	// QueryOnConn executes SQL through the full RBAC/RLS/masking pipeline
	// but on a specific *sql.Conn instead of the connection pool.
	QueryOnConn(ctx context.Context, conn *sql.Conn, principalName, sqlQuery string) (*sql.Rows, error)
}

// SecretManager handles DuckDB secret lifecycle.
// Implemented by engine.DuckDBSecretManager.
type SecretManager interface {
	CreateS3Secret(ctx context.Context, name, keyID, secret, endpoint, region, urlStyle string) error
	CreateAzureSecret(ctx context.Context, name, accountName, accountKey, connectionString string) error
	CreateGCSSecret(ctx context.Context, name, keyFilePath string) error
	DropSecret(ctx context.Context, name string) error
}

// CatalogAttacher manages DuckLake catalog attachment and detachment.
// The implementation switches on reg.MetastoreType internally.
type CatalogAttacher interface {
	Attach(ctx context.Context, reg CatalogRegistration) error
	Detach(ctx context.Context, catalogName string) error
	SetDefaultCatalog(ctx context.Context, catalogName string) error
}

// AuthorizationService defines the interface for permission checking.
// The engine depends on this interface rather than a concrete service type.
type AuthorizationService interface {
	LookupTableID(ctx context.Context, tableName string) (tableID, schemaID string, isExternal bool, err error)
	CheckPrivilege(ctx context.Context, principalName, securableType string, securableID string, privilege string) (bool, error)
	GetEffectiveRowFilters(ctx context.Context, principalName string, tableID string) ([]string, error)
	GetEffectiveColumnMasks(ctx context.Context, principalName string, tableID string) (map[string]string, error)
	GetTableColumnNames(ctx context.Context, tableID string) ([]string, error)
}

// DuckDBExecutor executes raw SQL statements against DuckDB.
// Used for CALL statements that bypass the SQL parser (e.g. ducklake_add_data_files).
type DuckDBExecutor interface {
	ExecContext(ctx context.Context, query string) error
}

// MetastoreQuerierFactory creates per-catalog MetastoreQuerier instances.
type MetastoreQuerierFactory interface {
	ForCatalog(ctx context.Context, catalogName string) (MetastoreQuerier, error)
	Close(catalogName string) error // release pooled connections on catalog deletion
}

// MetastoreQuerier provides read-only access to the DuckLake metastore.
// Replaces raw *sql.DB usage in manifest and ingestion services.
type MetastoreQuerier interface {
	// ReadDataPath returns the global data_path from ducklake_metadata.
	ReadDataPath(ctx context.Context) (string, error)
	// ReadSchemaPath returns the storage path for a specific schema, or empty if not set.
	ReadSchemaPath(ctx context.Context, schemaName string) (string, error)
	// ListDataFiles returns the active Parquet file paths for a table.
	// Returns paths and whether each path is relative to the data_path.
	ListDataFiles(ctx context.Context, tableID string) (paths []string, pathIsRelative []bool, err error)
}

// NotebookProvider resolves a notebook ID to executable SQL blocks.
// Used by the pipeline executor to extract SQL cells from notebooks.
type NotebookProvider interface {
	GetSQLBlocks(ctx context.Context, notebookID string) ([]string, error)
}

// ModelRunner executes a model run synchronously. Used by the pipeline executor.
type ModelRunner interface {
	TriggerRunSync(ctx context.Context, principal string, req TriggerModelRunRequest) error
}
