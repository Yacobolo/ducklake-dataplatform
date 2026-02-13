package domain

import "time"

// ViewDetail represents a view in the catalog.
type ViewDetail struct {
	ID             string
	SchemaID       string
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
	DeletedAt      *time.Time
}

// CreateViewRequest holds parameters for creating a view.
type CreateViewRequest struct {
	Name           string
	ViewDefinition string
	Comment        string
}

// UpdateViewRequest holds parameters for updating view metadata.
type UpdateViewRequest struct {
	Comment        *string
	Properties     map[string]string
	ViewDefinition *string
}
