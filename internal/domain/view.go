package domain

import "time"

// ViewDetail represents a view in the catalog.
type ViewDetail struct {
	ID             int64
	SchemaID       int64
	SchemaName     string
	CatalogName    string
	Name           string
	ViewDefinition string
	Comment        *string
	Properties     map[string]string
	Owner          string
	SourceTables   []string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// CreateViewRequest holds parameters for creating a view.
type CreateViewRequest struct {
	Name           string
	ViewDefinition string
	Comment        string
}
