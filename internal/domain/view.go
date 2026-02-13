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

// Validate checks that the request is well-formed.
func (r *CreateViewRequest) Validate() error {
	if r.Name == "" {
		return ErrValidation("view name is required")
	}
	if r.ViewDefinition == "" {
		return ErrValidation("view_definition is required")
	}
	return nil
}

// UpdateViewRequest holds parameters for updating view metadata.
type UpdateViewRequest struct {
	Comment        *string
	Properties     map[string]string
	ViewDefinition *string
}
