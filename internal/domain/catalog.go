package domain

import "time"

// CatalogInfo represents the single DuckLake catalog.
type CatalogInfo struct {
	Name      string
	Comment   string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// SchemaDetail is an enriched schema representation for the catalog API.
type SchemaDetail struct {
	SchemaID    int64
	Name        string
	CatalogName string
	Comment     string
	Properties  map[string]string
	Owner       string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// TableDetail is an enriched table representation for the catalog API.
type TableDetail struct {
	TableID     int64
	Name        string
	SchemaName  string
	CatalogName string
	TableType   string // "MANAGED"
	Columns     []ColumnDetail
	Comment     string
	Properties  map[string]string
	Owner       string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// ColumnDetail represents a column with full metadata.
type ColumnDetail struct {
	Name     string
	Type     string
	Position int
	Comment  string
}

// CreateSchemaRequest holds parameters for creating a new schema.
type CreateSchemaRequest struct {
	Name       string
	Comment    string
	Properties map[string]string
}

// UpdateSchemaRequest holds parameters for updating schema metadata.
type UpdateSchemaRequest struct {
	Comment    *string
	Properties map[string]string
}

// CreateTableRequest holds parameters for creating a new table.
type CreateTableRequest struct {
	Name    string
	Columns []CreateColumnDef
	Comment string
}

// CreateColumnDef defines a column for table creation.
type CreateColumnDef struct {
	Name string
	Type string
}

// MetastoreSummary provides high-level info about the DuckLake metastore.
type MetastoreSummary struct {
	CatalogName    string
	MetastoreType  string
	StorageBackend string
	DataPath       string
	SchemaCount    int64
	TableCount     int64
}
