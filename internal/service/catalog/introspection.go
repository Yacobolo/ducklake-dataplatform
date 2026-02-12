package catalog

import (
	"context"

	"duck-demo/internal/domain"
)

// IntrospectionService provides read-only access to DuckLake metadata.
type IntrospectionService struct {
	repo domain.IntrospectionRepository
}

// NewIntrospectionService creates a new IntrospectionService.
func NewIntrospectionService(repo domain.IntrospectionRepository) *IntrospectionService {
	return &IntrospectionService{repo: repo}
}

// ListSchemas returns a paginated list of schemas.
func (s *IntrospectionService) ListSchemas(ctx context.Context, page domain.PageRequest) ([]domain.Schema, int64, error) {
	return s.repo.ListSchemas(ctx, page)
}

// ListTables returns a paginated list of tables in a schema.
func (s *IntrospectionService) ListTables(ctx context.Context, schemaID int64, page domain.PageRequest) ([]domain.Table, int64, error) {
	return s.repo.ListTables(ctx, schemaID, page)
}

// ListColumns returns a paginated list of columns for a table.
func (s *IntrospectionService) ListColumns(ctx context.Context, tableID int64, page domain.PageRequest) ([]domain.Column, int64, error) {
	return s.repo.ListColumns(ctx, tableID, page)
}
