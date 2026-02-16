package engine

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"duck-demo/internal/domain"
)

// DuckDBSecretManager wraps a DuckDB connection to manage secrets and catalog attachment.
type DuckDBSecretManager struct {
	db             *sql.DB
	postgresMu     sync.Mutex
	postgresLoaded bool
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

// Attach inspects reg.MetastoreType and dispatches to the right DDL.
func (m *DuckDBSecretManager) Attach(ctx context.Context, reg domain.CatalogRegistration) error {
	switch reg.MetastoreType {
	case domain.MetastoreTypeSQLite:
		return AttachDuckLake(ctx, m.db, reg.Name, reg.DSN, reg.DataPath)
	case domain.MetastoreTypePostgres:
		// Install postgres extension if not yet loaded. Uses a mutex + bool
		// instead of sync.Once so that transient failures can be retried.
		if err := m.ensurePostgresExtension(ctx); err != nil {
			return fmt.Errorf("install postgres extension: %w", err)
		}
		return AttachDuckLakePostgres(ctx, m.db, reg.Name, reg.DSN, reg.DataPath)
	default:
		return fmt.Errorf("unsupported metastore type: %q", reg.MetastoreType)
	}
}

// ensurePostgresExtension installs the postgres extension if it hasn't been
// loaded yet. Unlike sync.Once, a transient failure leaves the flag unset so
// the next call retries the installation.
func (m *DuckDBSecretManager) ensurePostgresExtension(ctx context.Context) error {
	m.postgresMu.Lock()
	defer m.postgresMu.Unlock()

	if m.postgresLoaded {
		return nil
	}
	if err := InstallPostgresExtension(ctx, m.db); err != nil {
		return err
	}
	m.postgresLoaded = true
	return nil
}

// Detach detaches a named catalog from DuckDB.
func (m *DuckDBSecretManager) Detach(ctx context.Context, catalogName string) error {
	return DetachCatalog(ctx, m.db, catalogName)
}

// SetDefaultCatalog runs USE <catalog> on DuckDB.
func (m *DuckDBSecretManager) SetDefaultCatalog(ctx context.Context, catalogName string) error {
	return SetDefaultCatalog(ctx, m.db, catalogName)
}
