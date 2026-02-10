package domain

// Schema represents a DuckLake schema.
type Schema struct {
	ID   int64
	Name string
}

// Table represents a DuckLake table.
type Table struct {
	ID       int64
	SchemaID int64
	Name     string
}

// Column represents a column in a DuckLake table.
type Column struct {
	ID      int64
	TableID int64
	Name    string
	Type    string
}
