package domain

import "time"

// Table type constants.
const (
	TableTypeManaged  = "MANAGED"
	TableTypeExternal = "EXTERNAL"
)

// ExternalTableRecord represents an external table stored in the application-owned SQLite table.
type ExternalTableRecord struct {
	ID           string
	CatalogName  string
	SchemaName   string
	TableName    string
	FileFormat   string
	SourcePath   string
	LocationName string
	Comment      string
	Owner        string
	CreatedAt    time.Time
	UpdatedAt    time.Time
	DeletedAt    *time.Time
	Columns      []ExternalTableColumn
}

// ExternalTableColumn describes a column in an external table.
type ExternalTableColumn struct {
	ID              string
	ExternalTableID string
	ColumnName      string
	ColumnType      string
	Position        int
}
