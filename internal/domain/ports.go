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

// SecretManager handles DuckDB secret lifecycle.
// Implemented by engine.DuckDBSecretManager.
type SecretManager interface {
	CreateS3Secret(ctx context.Context, name, keyID, secret, endpoint, region, urlStyle string) error
	CreateAzureSecret(ctx context.Context, name, accountName, accountKey, connectionString string) error
	CreateGCSSecret(ctx context.Context, name, keyFilePath string) error
	DropSecret(ctx context.Context, name string) error
}

// CatalogAttacher manages DuckLake catalog attachment.
// Implemented by engine.DuckDBSecretManager.
type CatalogAttacher interface {
	AttachDuckLake(ctx context.Context, metaDBPath, dataPath string) error
}

// AuthorizationService defines the interface for permission checking.
// The engine depends on this interface rather than a concrete service type.
type AuthorizationService interface {
	LookupTableID(ctx context.Context, tableName string) (tableID, schemaID int64, isExternal bool, err error)
	CheckPrivilege(ctx context.Context, principalName, securableType string, securableID int64, privilege string) (bool, error)
	GetEffectiveRowFilters(ctx context.Context, principalName string, tableID int64) ([]string, error)
	GetEffectiveColumnMasks(ctx context.Context, principalName string, tableID int64) (map[string]string, error)
	GetTableColumnNames(ctx context.Context, tableID int64) ([]string, error)
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
	ListDataFiles(ctx context.Context, tableID int64) (paths []string, pathIsRelative []bool, err error)
}
