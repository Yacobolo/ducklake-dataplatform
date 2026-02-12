package engine

import (
	"context"
	"database/sql"

	"duck-demo/internal/domain"
)

// DuckDBSecretManager wraps a DuckDB connection to manage secrets and catalog attachment.
type DuckDBSecretManager struct {
	db *sql.DB
}

// NewDuckDBSecretManager creates a new DuckDBSecretManager.
func NewDuckDBSecretManager(db *sql.DB) *DuckDBSecretManager {
	return &DuckDBSecretManager{db: db}
}

// Compile-time interface checks.
var _ domain.SecretManager = (*DuckDBSecretManager)(nil)
var _ domain.CatalogAttacher = (*DuckDBSecretManager)(nil)

// CreateS3Secret creates an S3-type secret in DuckDB with the given credentials.
func (m *DuckDBSecretManager) CreateS3Secret(ctx context.Context, name, keyID, secret, endpoint, region, urlStyle string) error {
	return CreateS3Secret(ctx, m.db, name, keyID, secret, endpoint, region, urlStyle)
}

// CreateAzureSecret creates an Azure-type secret in DuckDB with the given credentials.
func (m *DuckDBSecretManager) CreateAzureSecret(ctx context.Context, name, accountName, accountKey, connectionString string) error {
	return CreateAzureSecret(ctx, m.db, name, accountName, accountKey, connectionString)
}

// CreateGCSSecret creates a GCS-type secret in DuckDB with the given key file path.
func (m *DuckDBSecretManager) CreateGCSSecret(ctx context.Context, name, keyFilePath string) error {
	return CreateGCSSecret(ctx, m.db, name, keyFilePath)
}

// DropSecret removes a named secret from DuckDB.
func (m *DuckDBSecretManager) DropSecret(ctx context.Context, name string) error {
	return DropSecret(ctx, m.db, name)
}

// AttachDuckLake attaches a DuckLake catalog to DuckDB using the given metastore and data paths.
func (m *DuckDBSecretManager) AttachDuckLake(ctx context.Context, metaDBPath, dataPath string) error {
	return AttachDuckLake(ctx, m.db, metaDBPath, dataPath)
}
