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
	Tags        []Tag
	DeletedAt   *time.Time
}

// TableDetail is an enriched table representation for the catalog API.
type TableDetail struct {
	TableID      int64
	Name         string
	SchemaName   string
	CatalogName  string
	TableType    string // "MANAGED" or "EXTERNAL"
	Columns      []ColumnDetail
	Comment      string
	Properties   map[string]string
	Owner        string
	CreatedAt    time.Time
	UpdatedAt    time.Time
	Tags         []Tag
	Statistics   *TableStatistics
	DeletedAt    *time.Time
	SourcePath   string // populated for EXTERNAL tables
	FileFormat   string // populated for EXTERNAL tables
	LocationName string // populated for EXTERNAL tables
}

// ColumnDetail represents a column with full metadata.
type ColumnDetail struct {
	Name       string
	Type       string
	Position   int
	Comment    string
	Properties map[string]string
}

// CreateSchemaRequest holds parameters for creating a new schema.
type CreateSchemaRequest struct {
	Name         string
	Comment      string
	Properties   map[string]string
	LocationName string // optional: external location to use for schema storage path
}

// UpdateSchemaRequest holds parameters for updating schema metadata.
type UpdateSchemaRequest struct {
	Comment    *string
	Properties map[string]string
}

// UpdateTableRequest holds parameters for updating table metadata.
type UpdateTableRequest struct {
	Comment    *string
	Properties map[string]string
	Owner      *string
}

// UpdateColumnRequest holds parameters for updating column metadata.
type UpdateColumnRequest struct {
	Comment    *string
	Properties map[string]string
}

// UpdateCatalogRequest holds parameters for updating catalog metadata.
type UpdateCatalogRequest struct {
	Comment *string
}

// TableStatistics holds profiling statistics for a table.
type TableStatistics struct {
	RowCount       *int64
	SizeBytes      *int64
	ColumnCount    *int64
	LastProfiledAt *time.Time
	ProfiledBy     string
}

// CreateTableRequest holds parameters for creating a new table.
type CreateTableRequest struct {
	Name         string
	Columns      []CreateColumnDef
	Comment      string
	TableType    string // "MANAGED" (default) or "EXTERNAL"
	SourcePath   string // required for EXTERNAL
	FileFormat   string // "parquet" (default) or "csv"; only for EXTERNAL
	LocationName string // required for EXTERNAL
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
