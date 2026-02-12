package service

import (
	"context"

	"duck-demo/internal/domain"
)

// RowFilterService provides row-level security filter operations.
type RowFilterService struct {
	repo  domain.RowFilterRepository
	audit domain.AuditRepository
}

// NewRowFilterService creates a new RowFilterService.
func NewRowFilterService(repo domain.RowFilterRepository, audit domain.AuditRepository) *RowFilterService {
	return &RowFilterService{repo: repo, audit: audit}
}

// Create validates and persists a new row filter. Requires admin privileges.
func (s *RowFilterService) Create(ctx context.Context, principal string, f *domain.RowFilter) (*domain.RowFilter, error) {
	if err := requireAdmin(ctx); err != nil {
		return nil, err
	}
	if f.FilterSQL == "" {
		return nil, domain.ErrValidation("filter_sql is required")
	}
	result, err := s.repo.Create(ctx, f)
	if err != nil {
		return nil, err
	}
	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: callerName(ctx),
		Action:        "CREATE_ROW_FILTER",
		Status:        "ALLOWED",
	})
	return result, nil
}

// GetForTable returns a paginated list of row filters for a table.
func (s *RowFilterService) GetForTable(ctx context.Context, tableID int64, page domain.PageRequest) ([]domain.RowFilter, int64, error) {
	return s.repo.GetForTable(ctx, tableID, page)
}

// Delete removes a row filter by ID. Requires admin privileges.
func (s *RowFilterService) Delete(ctx context.Context, id int64) error {
	if err := requireAdmin(ctx); err != nil {
		return err
	}
	return s.repo.Delete(ctx, id)
}

// Bind associates a row filter with a principal or group. Requires admin privileges.
func (s *RowFilterService) Bind(ctx context.Context, b *domain.RowFilterBinding) error {
	if err := requireAdmin(ctx); err != nil {
		return err
	}
	return s.repo.Bind(ctx, b)
}

// Unbind removes a row filter binding from a principal or group. Requires admin privileges.
func (s *RowFilterService) Unbind(ctx context.Context, b *domain.RowFilterBinding) error {
	if err := requireAdmin(ctx); err != nil {
		return err
	}
	return s.repo.Unbind(ctx, b)
}

// ListBindings returns all bindings for a row filter.
func (s *RowFilterService) ListBindings(ctx context.Context, filterID int64) ([]domain.RowFilterBinding, error) {
	return s.repo.ListBindings(ctx, filterID)
}
