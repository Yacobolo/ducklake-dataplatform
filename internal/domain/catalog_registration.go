package domain

import "time"

// CatalogStatus represents the lifecycle state of a registered catalog.
type CatalogStatus string

// Possible values for CatalogStatus.
const (
	CatalogStatusActive   CatalogStatus = "ACTIVE"
	CatalogStatusError    CatalogStatus = "ERROR"
	CatalogStatusDetached CatalogStatus = "DETACHED"
)

// MetastoreType represents the backend type for a DuckLake metastore.
type MetastoreType string

// Possible values for MetastoreType.
const (
	MetastoreTypeSQLite   MetastoreType = "sqlite"
	MetastoreTypePostgres MetastoreType = "postgres"
)

// CatalogRegistration represents a registered DuckLake catalog with its own metastore.
type CatalogRegistration struct {
	ID            int64
	Name          string // DuckDB catalog alias, used in SQL: catalog.schema.table
	MetastoreType MetastoreType
	DSN           string // file path (sqlite) or connection string (postgres)
	DataPath      string // s3://bucket/path/ or local path
	Status        CatalogStatus
	StatusMessage string
	IsDefault     bool
	Comment       string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// CreateCatalogRequest holds parameters for registering a new catalog.
type CreateCatalogRequest struct {
	Name          string
	MetastoreType string
	DSN           string
	DataPath      string
	Comment       string
}

// UpdateCatalogRegistrationRequest holds optional parameters for updating a catalog registration.
type UpdateCatalogRegistrationRequest struct {
	Comment  *string
	DataPath *string
	DSN      *string
}
