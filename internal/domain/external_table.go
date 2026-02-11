package domain

import "time"

// Table type constants.
const (
	TableTypeManaged  = "MANAGED"
	TableTypeExternal = "EXTERNAL"
)

// ExternalTableIDOffset separates external table IDs from DuckLake auto-increment IDs.
const ExternalTableIDOffset int64 = 10_000_000

// ExternalTableRecord represents an external table stored in the application-owned SQLite table.
type ExternalTableRecord struct {
	ID           int64
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
	ID              int64
	ExternalTableID int64
	ColumnName      string
	ColumnType      string
	Position        int
}

// EffectiveTableID returns the external table ID offset by ExternalTableIDOffset
// to avoid collisions with DuckLake's auto-increment IDs.
func (et *ExternalTableRecord) EffectiveTableID() int64 {
	return et.ID + ExternalTableIDOffset
}

// IsExternalTableID returns true if the given tableID falls in the external table ID range.
func IsExternalTableID(tableID int64) bool {
	return tableID >= ExternalTableIDOffset
}

// ExternalTableRawID converts an effective external table ID back to the raw SQLite row ID.
func ExternalTableRawID(tableID int64) int64 {
	return tableID - ExternalTableIDOffset
}
