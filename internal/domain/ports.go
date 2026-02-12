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
