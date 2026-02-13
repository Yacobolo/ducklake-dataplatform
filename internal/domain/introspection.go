package domain

// Schema represents a DuckLake schema.
type Schema struct {
	ID   string
	Name string
}

// Table represents a DuckLake table.
type Table struct {
	ID       string
	SchemaID string
	Name     string
}

// Column represents a column in a DuckLake table.
type Column struct {
	ID      string
	TableID string
	Name    string
	Type    string
}
