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

func (m *DuckDBSecretManager) CreateS3Secret(ctx context.Context, name, keyID, secret, endpoint, region, urlStyle string) error {
	return CreateS3Secret(ctx, m.db, name, keyID, secret, endpoint, region, urlStyle)
}

func (m *DuckDBSecretManager) CreateAzureSecret(ctx context.Context, name, accountName, accountKey, connectionString string) error {
	return CreateAzureSecret(ctx, m.db, name, accountName, accountKey, connectionString)
}

func (m *DuckDBSecretManager) CreateGCSSecret(ctx context.Context, name, keyFilePath string) error {
	return CreateGCSSecret(ctx, m.db, name, keyFilePath)
}

func (m *DuckDBSecretManager) DropSecret(ctx context.Context, name string) error {
	return DropSecret(ctx, m.db, name)
}

func (m *DuckDBSecretManager) AttachDuckLake(ctx context.Context, metaDBPath, dataPath string) error {
	return AttachDuckLake(ctx, m.db, metaDBPath, dataPath)
}
